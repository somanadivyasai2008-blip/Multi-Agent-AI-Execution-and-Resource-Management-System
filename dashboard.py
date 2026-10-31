#!/usr/bin/env python3
"""Pipeline Operations Dashboard.

One command:   python3 dashboard.py

Serves a single-page dashboard on http://127.0.0.1:8765 (next free port if busy)
for the multi-agent C pipeline in this folder. It only ever reads the program's
real files (logs/, pipeline.conf, workspace/) and runs the program itself.
Python 3 standard library only; works offline.

Environment overrides (all optional):
  DASH_SOURCE=<file.c>   force which C file is the pipeline
  DASH_PORT=<n>          first port to try (default 8765)
  DASH_NO_OPEN=1         do not try to open a browser
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import socketserver
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent
DASH_DIR = ROOT / ".dashboard"
BIN = DASH_DIR / "pipeline"
BIN_REL = "./.dashboard/pipeline"
BUILD_INFO = DASH_DIR / "build.json"
META_FILE = DASH_DIR / "meta.json"
CUSTOM_DIR = DASH_DIR / "custom"

HISTORY_FILE = ROOT / "logs" / "run_history.log"
ACTIVITY_FILE = ROOT / "logs" / "agent_activity.log"
LAST_RESULT_FILE = ROOT / "logs" / "last_result.txt"
CONF_FILE = ROOT / "pipeline.conf"
CUSTOM_C = ROOT / "workspace" / "input" / "custom.c"

CANDIDATES = ["project.c", "program6.c"]
GCC_FLAGS = ["-Wall", "-Wextra", "-std=gnu11", "-O2"]
FIRST_PORT = int(os.environ.get("DASH_PORT", "8765") or 8765)
PORT_TRIES = 25
RUN_CAP_SEC = 60            # hard cap for any one subprocess
CODE_MAX_BYTES = 20 * 1024  # custom code ceiling (the program's own buffer may be smaller)
OUTPUT_CAP = 2 * 1024 * 1024
MAX_BODY = 256 * 1024
MAX_LOG_READ = 8 * 1024 * 1024

HISTORY_RE = re.compile(r"\[(.*?)\]\s+(.*?)\s+(PASS|FAIL|SKIPPED|INTERRUPTED)\s+(.*)")
ACTIVITY_RE = re.compile(r"^\[(.*?)\] PID: (\d+) \| (.*)$")
TEST_RE = re.compile(r"TEST RESULT: (PASS|FAIL|SKIPPED)(.*)")

AGENT_ORDER = ["Code Generator", "Code Review Agent", "Execution Agent",
               "Testing Agent", "Main Controller", "Resource Monitor"]

# --------------------------------------------------------------------------
# Source selection and build
# --------------------------------------------------------------------------


def read_text(path: Path, cap: int = MAX_LOG_READ) -> str | None:
    try:
        size = path.stat().st_size
        with open(path, "rb") as fh:
            if size > cap:
                fh.seek(size - cap)
            return fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None


def score_source(path: Path) -> dict:
    text = read_text(path, 4 * 1024 * 1024) or ""
    feats = {
        "fork": "fork(" in text,
        "mkfifo": "mkfifo" in text,
        "banner": bool(re.search(r"multi-agent", text, re.I)),
        "flock": "flock(" in text,
        "run_flag": '"--run"' in text,
        "endcode": "ENDCODE" in text,
        "signals": "sigaction" in text or "signal(" in text,
    }
    qualifies = feats["fork"] and feats["mkfifo"] and feats["banner"]
    score = sum(feats.values()) * 10 + text.count("\n") // 100
    return {"path": path, "qualifies": qualifies, "score": score, "feats": feats,
            "lines": text.count("\n") + 1 if text else 0}


def choose_source() -> dict:
    override = os.environ.get("DASH_SOURCE")
    if override:
        p = (ROOT / override).resolve() if not os.path.isabs(override) else Path(override)
        if not p.is_file():
            sys.exit(f"DASH_SOURCE={override} does not exist.")
        info = score_source(p)
        info["why"] = "chosen by DASH_SOURCE"
        return info
    infos = [score_source(ROOT / c) for c in CANDIDATES if (ROOT / c).is_file()]
    good = sorted((i for i in infos if i["qualifies"]), key=lambda i: -i["score"])
    if not good:
        sys.exit("No pipeline source found: none of %s contains fork(), mkfifo and 'Multi-Agent'.\n"
                 "Set DASH_SOURCE=<file.c> to point at it." % ", ".join(CANDIDATES))
    best = good[0]
    if len(good) == 1:
        others = [i["path"].name for i in infos if i is not best]
        best["why"] = "the only candidate with fork() + mkfifo + 'Multi-Agent'" + (
            f" (skipped {', '.join(others)}: not a pipeline)" if others else "")
    else:
        rest = good[1]
        best["why"] = (f"more complete than {rest['path'].name} "
                       f"(score {best['score']} vs {rest['score']}); override with DASH_SOURCE=<file>")
    return best


def source_stat_key(path: Path) -> dict:
    st = path.stat()
    return {"source": path.name, "mtime_ns": st.st_mtime_ns, "size": st.st_size, "flags": GCC_FLAGS}


def ensure_binary(src: Path) -> dict:
    """Compile the pipeline to .dashboard/pipeline if it is missing or stale."""
    DASH_DIR.mkdir(exist_ok=True)
    key = source_stat_key(src)
    prior = None
    try:
        prior = json.loads(BUILD_INFO.read_text())
    except (OSError, ValueError):
        pass
    fresh = (BIN.is_file() and os.access(BIN, os.X_OK) and prior
             and all(prior.get(k) == v for k, v in key.items()))
    if fresh:
        return {"status": "up to date", "warnings": prior.get("warnings", ""),
                "built_at": prior.get("built_at")}
    gcc = shutil.which("gcc")
    if not gcc:
        sys.exit("gcc was not found on PATH.\n"
                 "Install a compiler with:  sudo apt install build-essential\n"
                 "then run  python3 dashboard.py  again.")
    tmp = DASH_DIR / "pipeline.new"
    cmd = [gcc, *GCC_FLAGS, str(src), "-o", str(tmp)]
    print("  building:", " ".join([os.path.basename(gcc), *GCC_FLAGS, src.name, "-o", ".dashboard/pipeline"]))
    try:
        res = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        sys.exit("Compilation timed out after 180 s.")
    diag = (res.stderr or "") + (res.stdout or "")
    if res.returncode != 0:
        print(diag)
        sys.exit("Compilation failed - dashboard not started. (Your .c files were not modified.)")
    os.replace(tmp, BIN)
    BIN.chmod(0o755)
    info = dict(key, warnings=diag.strip(), built_at=time.time())
    BUILD_INFO.write_text(json.dumps(info))
    if diag.strip():
        n = len(re.findall(r"warning:", diag))
        print(f"  note: gcc reported {n} warning(s) in {src.name} (shown in the dashboard under Build notes).")
    return {"status": "rebuilt", "warnings": diag.strip(), "built_at": info["built_at"]}


# --------------------------------------------------------------------------
# Parsing the program's real files
# --------------------------------------------------------------------------

_ESC = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "'": "'", "\\": "\\", "a": "\a",
        "b": "\b", "f": "\f", "v": "\v", "0": "\0", "?": "?"}


def _unescape(body: str) -> str:
    out, i = [], 0
    while i < len(body):
        c = body[i]
        if c != "\\" or i + 1 >= len(body):
            out.append(c)
            i += 1
            continue
        n = body[i + 1]
        if n == "x":
            m = re.match(r"[0-9a-fA-F]{1,2}", body[i + 2:])
            if m:
                out.append(chr(int(m.group(0), 16)))
                i += 2 + len(m.group(0))
                continue
        m = re.match(r"[0-7]{1,3}", body[i + 1:])
        if m:
            out.append(chr(int(m.group(0), 8)))
            i += 1 + len(m.group(0))
            continue
        out.append(_ESC.get(n, n))
        i += 2
    return "".join(out)


def parse_catalog(text: str) -> list[dict]:
    """Read PROGRAM_CATALOG[] out of the C source (string literals only)."""
    m = re.search(r"PROGRAM_CATALOG\s*\[\s*\d*\s*\]\s*=\s*\{", text)
    if not m:
        return []
    i, n = m.end(), len(text)

    def skip(i: int) -> int:
        while i < n:
            if text[i].isspace():
                i += 1
            elif text.startswith("/*", i):
                j = text.find("*/", i + 2)
                i = n if j < 0 else j + 2
            elif text.startswith("//", i):
                j = text.find("\n", i)
                i = n if j < 0 else j + 1
            else:
                break
        return i

    entries = []
    while i < n:
        i = skip(i)
        if i >= n or text[i] == "}":
            break
        if text[i] != "{":
            i += 1
            continue
        i += 1
        fields, parts, is_null = [], [], False
        while i < n:
            i = skip(i)
            c = text[i]
            if c == '"':
                j = i + 1
                while j < n and text[j] != '"':
                    j += 2 if text[j] == "\\" else 1
                parts.append(_unescape(text[i + 1:j]))
                i = j + 1
            elif c in ",}":
                fields.append(None if (is_null or not parts) else "".join(parts))
                parts, is_null = [], False
                i += 1
                if c == "}":
                    break
            else:
                mm = re.match(r"[A-Za-z_0-9]+", text[i:])
                if mm:
                    is_null = is_null or mm.group(0) == "NULL"
                    i += len(mm.group(0))
                else:
                    i += 1
        entries.append(fields)
    out = []
    for idx, f in enumerate(entries):
        if idx == 0 or len(f) < 4 or not f[1]:
            continue
        out.append({"n": idx, "filename": f[0], "name": f[1], "source": f[2] or "", "expected": f[3] or ""})
    return out


def parse_defaults(text: str) -> dict:
    def num(name: str):
        m = re.search(r"#define\s+%s\s+\(?(\d+)" % name, text)
        return int(m.group(1)) if m else None
    return {"MEMORY_LIMIT_KB": num("MEMORY_LIMIT_DEFAULT"),
            "CPU_LIMIT_TICKS": num("CPU_LIMIT_DEFAULT"),
            "PROGRAM_TIMEOUT_SEC": num("PROGRAM_TIMEOUT_DEFAULT"),
            "CUSTOM_SOURCE_MAX": num("CUSTOM_SOURCE_MAX")}


def parse_history(text: str | None) -> tuple[list[dict], int]:
    rows, bad, seen = [], 0, {}
    for raw in (text or "").splitlines():
        if not raw.strip():
            continue
        m = HISTORY_RE.match(raw)
        if not m:
            bad += 1
            continue
        ts, name, verdict, detail = m.group(1), m.group(2).strip(), m.group(3), m.group(4).strip()
        epoch = None
        try:
            epoch = time.mktime(time.strptime(ts, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            pass
        k = (ts, name)
        seen[k] = seen.get(k, 0) + 1
        key = hashlib.sha1(f"{ts}|{name}|{seen[k]}".encode()).hexdigest()[:12]
        rows.append({"key": key, "ts": ts, "epoch": epoch, "name": name,
                     "verdict": verdict, "detail": detail})
    return rows, bad


def parse_activity(text: str | None) -> list[dict]:
    out = []
    for raw in (text or "").splitlines():
        if not raw.strip():
            continue
        m = ACTIVITY_RE.match(raw)
        if m:
            out.append({"agent": m.group(1), "pid": int(m.group(2)), "msg": m.group(3)})
        else:
            out.append({"agent": "(unparsed)", "pid": None, "msg": raw})
    return out


def _trim_trace(seg: list[dict]) -> list[dict]:
    """Drop session noise that precedes the run inside its segment."""
    start = None
    for j in range(len(seg) - 1, -1, -1):
        if seg[j]["msg"] == "Multi-Agent System execution started":
            start = j - 1 if j > 0 and seg[j - 1]["msg"] == "Main Controller started" else j
            break
    if start is None:
        start = 0
        while start < len(seg) and seg[start]["msg"] in ("Main Controller started", "Run All Demos started"):
            start += 1
    return seg[start:]


def split_runs(lines: list[dict]) -> tuple[list[list[dict]], list[dict]]:
    """Split the activity log on the 'All AI Agents completed' marker."""
    segs, cur = [], []
    for ln in lines:
        cur.append(ln)
        if ln["agent"] == "Main Controller" and ln["msg"].startswith("All AI Agents completed"):
            segs.append(cur)
            cur = []
    runs: list[list[dict]] = []

    def take_trailing(seg: list[dict]) -> list[dict]:
        # lines that close the previous run ("System exited", "Run All Demos completed")
        k = 0
        while k < len(seg) and (seg[k]["msg"].startswith("System exited")
                                or seg[k]["msg"].startswith("Run All Demos completed")):
            if runs:
                runs[-1].append(seg[k])
            k += 1
        return seg[k:]
    for seg in segs:
        runs.append(_trim_trace(take_trailing(seg)))
    return runs, _trim_trace(take_trailing(cur))


EXEC_FAIL_MARKS = ("Compilation failed", "timed out", "Program execution failed", "Execution denied",
                   "Workspace operation failed", "Compiler process creation failed",
                   "Program process creation failed", "terminated due to resource limit",
                   "Memory limit exceeded", "CPU limit exceeded")


def derive_stages(msgs: list[str] | None, verdict: str, detail: str) -> dict:
    def S(state, note=""):
        return {"state": state, "note": note}
    have = msgs is not None
    idle = "No agent trace matched this run" if not have else "Not reached"
    st = {k: S("none", idle) for k in ("generate", "review", "execute", "test")}
    if not have:
        return st
    joined = "\n".join(msgs)

    def has(s):
        return any(m.startswith(s) for m in msgs)
    if has("Code Generator finished"):
        st["generate"] = S("pass", "Source written and sent to the reviewer")
    elif has("Could not write generated source") or has("Generator FIFO open failed"):
        st["generate"] = S("fail", "Generator could not hand over the source")
    if has("Static review passed"):
        st["review"] = S("pass", "Static review passed")
    elif has("Static review failed"):
        note = detail[len("Review failed:"):].strip() if detail.startswith("Review failed:") else "Static review failed"
        st["review"] = S("fail", note)
    if has("Skipped: code review failed upstream"):
        st["execute"] = S("skip", "Blocked: review failed upstream")
    elif any(mark in joined for mark in EXEC_FAIL_MARKS):
        if "Compilation failed" in joined:
            note = "Compilation failed"
        elif "timed out" in joined:
            note = detail if "did not finish" in detail else "Timed out and was killed"
        else:
            note = detail or "Execution failed"
        st["execute"] = S("fail", note)
    elif has("Program execution successful"):
        st["execute"] = S("pass", "Compiled and ran to completion")
    for m in msgs:
        t = TEST_RE.search(m)
        if t:
            res, rest = t.group(1), t.group(2).strip()
            if res == "PASS":
                st["test"] = S("pass", ("Exit code only " + rest if rest else "Output matched") +
                               (f" - got: {detail}" if detail else ""))
            elif res == "FAIL":
                st["test"] = S("fail", "Output did not match" + (f" - got: {detail}" if detail else ""))
            else:
                st["test"] = S("skip", "Skipped" + (f": {detail}" if detail else ""))
    return st


def classify(verdict: str, detail: str, st: dict) -> str | None:
    if verdict == "PASS":
        return None
    if verdict == "INTERRUPTED":
        return "interrupted"
    d = detail.lower()
    if st["review"]["state"] == "fail" or d.startswith("review failed"):
        return "review"
    if "compilation failed" in d or st["execute"]["note"].startswith("Compilation failed"):
        return "compile"
    if "did not finish within" in d or "timed out" in st["execute"]["note"].lower():
        return "timeout"
    if "exited with an error" in d or (st["execute"]["state"] == "fail"):
        return "runtime"
    if st["test"]["state"] == "fail" or verdict == "FAIL":
        return "mismatch"
    return "other"


def parse_conf(text: str | None) -> dict:
    vals = {}
    for raw in (text or "").splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        k, _, v = raw.partition("=")
        try:
            n = int(v.strip())
        except ValueError:
            continue
        if k.strip() in ("MEMORY_LIMIT_KB", "CPU_LIMIT_TICKS", "PROGRAM_TIMEOUT_SEC") and n > 0:
            vals[k.strip()] = n
    return vals


# --------------------------------------------------------------------------
# Meta written by the dashboard itself (elapsed time, custom-code snapshots)
# --------------------------------------------------------------------------

_meta_lock = threading.Lock()


def load_meta() -> dict:
    try:
        return json.loads(META_FILE.read_text())
    except (OSError, ValueError):
        return {}


def save_meta_entry(key: str, entry: dict) -> None:
    with _meta_lock:
        meta = load_meta()
        meta[key] = entry
        DASH_DIR.mkdir(exist_ok=True)
        tmp = META_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(meta))
        os.replace(tmp, META_FILE)


# --------------------------------------------------------------------------
# Timing: the program's activity log has no timestamps, so while the dashboard
# is running a watcher notes the wall-clock moment each new line appears.
# Those arrival times (accurate to ~50 ms) are the only source of per-agent timing.
# --------------------------------------------------------------------------

EVENTS_FILE = DASH_DIR / "events.jsonl"
SAMPLE_RE = re.compile(r"^\s+(\d+)\s+(\d+)\s+(\d+)\s+(\S.*)$", re.M)
ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


class Watcher(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.lock = threading.Lock()
        self.times: dict[tuple, list[float]] = {}
        self.count = 0
        self.halt = threading.Event()
        txt = read_text(EVENTS_FILE, 4 * 1024 * 1024) or ""
        for raw in txt.splitlines()[-30000:]:
            try:
                t, a, p, m, mono = json.loads(raw)
            except (ValueError, TypeError):
                continue          # older format without a monotonic stamp: not usable for durations
            self._add(a, p, m, mono)

    def _add(self, agent, pid, msg, mono) -> None:
        # monotonic arrival time: unlike wall-clock time it does not jump when the machine sleeps
        self.times.setdefault((agent, pid, msg), []).append(mono)
        self.count += 1

    def run(self) -> None:
        try:
            off = ACTIVITY_FILE.stat().st_size
        except OSError:
            off = 0
        buf = b""
        while not self.halt.wait(0.04):
            try:
                size = ACTIVITY_FILE.stat().st_size
            except OSError:
                off, buf = 0, b""
                continue
            if size < off:                 # log was reset
                off, buf = 0, b""
            if size == off:
                continue
            try:
                with open(ACTIVITY_FILE, "rb") as fh:
                    fh.seek(off)
                    chunk = fh.read(size - off)
            except OSError:
                continue
            off += len(chunk)
            now, mono = time.time(), time.monotonic()
            *lines, buf = (buf + chunk).split(b"\n")
            out = []
            for raw in lines:
                m = ACTIVITY_RE.match(raw.decode("utf-8", "replace"))
                if m:
                    out.append([now, m.group(1), int(m.group(2)), m.group(3), mono])
            if not out:
                continue
            with self.lock:
                for e in out:
                    self._add(e[1], e[2], e[3], e[4])
            try:
                DASH_DIR.mkdir(exist_ok=True)
                with open(EVENTS_FILE, "a") as fh:
                    fh.writelines(json.dumps(e) + "\n" for e in out)
            except OSError:
                pass

    def lookup(self, trace: list[dict]) -> list[float | None]:
        with self.lock:
            used: dict[tuple, int] = {}
            res: list[float | None] = []
            for ln in trace:
                k = (ln["agent"], ln["pid"], ln["msg"])
                lst, i = self.times.get(k), used.get(k, 0)
                if lst and i < len(lst):
                    res.append(lst[i])
                    used[k] = i + 1
                else:
                    res.append(None)
            return res


def compute_timing(trace: list[dict], times: list, entry: dict) -> dict | None:
    got = [t for t in times if t is not None]
    if len(got) < 2 or len(got) < 0.8 * len(times):
        return None
    t0 = min(got)
    spans = []
    for agent in AGENT_ORDER:
        ts = [t - t0 for ln, t in zip(trace, times) if ln["agent"] == agent and t is not None]
        if ts:
            spans.append({"agent": agent, "start": round(min(ts), 3), "end": round(max(ts), 3), "lines": len(ts)})
    out = {"lines": [None if t is None else round(t - t0, 3) for t in times],
           "span": round(max(got) - t0, 3), "spans": spans}
    if entry.get("start_mono") is not None and entry.get("end_mono") is not None:
        startup, shutdown = t0 - entry["start_mono"], entry["end_mono"] - max(got)
        if 0 <= startup < 120 and 0 <= shutdown < 120:
            out["startup"], out["shutdown"] = round(startup, 3), round(shutdown, 3)
    return out


def parse_samples(text: str) -> list[dict]:
    """Rows of the program's resource-monitor table (only printed where /proc exists, i.e. Linux)."""
    text = ANSI_RE.sub("", text)
    i = text.find("SAMPLE")
    if i < 0:
        return []
    return [{"n": int(a), "mem_kb": int(b), "cpu": int(c), "status": d.strip()[:40]}
            for a, b, c, d in SAMPLE_RE.findall(text[i:i + 8000])][:30]


# --------------------------------------------------------------------------
# State model
# --------------------------------------------------------------------------


def sig(path: Path):
    try:
        st = path.stat()
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


class Model:
    def __init__(self, src: Path, build: dict, why: str):
        self.src = src
        self.build = build
        self.why = why
        self.src_text = read_text(src, 8 * 1024 * 1024) or ""
        self.catalog = parse_catalog(self.src_text)
        self.defaults = parse_defaults(self.src_text)
        self.sleeps = []          # deliberate sleep(N) calls in the C source: they set the floor on run time
        for no, ln in enumerate(self.src_text.splitlines(), 1):
            code = ln.split("//")[0]
            sm = re.search(r"(?<![A-Za-z_])sleep\s*\(\s*(\d+)\s*\)", code)
            if sm and not code.lstrip().startswith(("*", "/*")):
                self.sleeps.append({"line": no, "sec": int(sm.group(1))})
        self.has_run = '"--run"' in self.src_text
        self.has_custom = "ENDCODE" in self.src_text
        cap = self.defaults.get("CUSTOM_SOURCE_MAX")
        # the program stops reading at (buffer - 2) bytes and would then misread the rest as menu input
        self.code_max = min(CODE_MAX_BYTES, cap - 2) if cap else CODE_MAX_BYTES
        self.lock = threading.Lock()
        self._sig = None
        self._state = None
        self._detail: dict[str, dict] = {}
        self.runner: "Runner | None" = None
        self.watcher: "Watcher | None" = None
        self.port = 0

    def history_rows(self) -> list[dict]:
        return parse_history(read_text(HISTORY_FILE))[0]

    def _signature(self):
        job = self.runner.public() if self.runner else None
        return (sig(HISTORY_FILE), sig(ACTIVITY_FILE), sig(LAST_RESULT_FILE), sig(CONF_FILE),
                sig(CUSTOM_C), sig(META_FILE),
                (job["id"], job["done"]) if job else None,
                self.watcher.count if self.watcher else 0)

    def state(self) -> dict:
        s = self._signature()
        with self.lock:
            if s != self._sig or self._state is None:
                self._rebuild()
                self._sig = s
                self._state["rev"] = hashlib.sha1(repr(s).encode()).hexdigest()[:12]
            return self._state

    def _rebuild(self) -> None:
        hist_text = read_text(HISTORY_FILE)
        rows, bad = parse_history(hist_text)
        lines = parse_activity(read_text(ACTIVITY_FILE))
        traces, tail = split_runs(lines)
        meta = load_meta()
        by_name = {c["name"]: c for c in self.catalog}

        # align from the newest: k-th newest history row <-> k-th newest trace
        trace_for: dict[int, list[dict]] = {}
        for k in range(1, min(len(rows), len(traces)) + 1):
            trace_for[len(rows) - k] = traces[len(traces) - k]

        runs, detail = [], {}
        newest_custom = max((i for i, r in enumerate(rows) if r["name"] == "Custom Program"), default=None)
        for i, r in enumerate(rows):
            tr = trace_for.get(i)
            msgs = [t["msg"] for t in tr] if tr is not None else None
            st = derive_stages(msgs, r["verdict"], r["detail"])
            reason = classify(r["verdict"], r["detail"], st)
            m = meta.get(r["key"], {})
            cat = by_name.get(r["name"])
            pids: dict[str, list[int]] = {}
            for t in tr or []:
                if t["pid"] is not None and t["pid"] not in pids.setdefault(t["agent"], []):
                    pids[t["agent"]].append(t["pid"])
            src = None
            if cat and cat["source"]:
                src = {"text": cat["source"], "filename": cat["filename"],
                       "label": f"Catalog program {cat['n']}, as defined in {self.src.name} (PROGRAM_CATALOG)"}
            elif r["name"] == "Custom Program":
                snap = CUSTOM_DIR / f"{r['key']}.c"
                txt = read_text(snap, 64 * 1024)
                if txt is not None:
                    src = {"text": txt, "filename": "custom.c",
                           "label": "Snapshot of the code this dashboard submitted for this run"}
                elif i == newest_custom:
                    txt = read_text(CUSTOM_C, 64 * 1024)
                    if txt is not None:
                        src = {"text": txt, "filename": "custom.c",
                               "label": "workspace/input/custom.c - the latest custom program on disk"}
            timing = None
            if tr is not None and self.watcher is not None:
                timing = compute_timing(tr, self.watcher.lookup(tr), m)
            elapsed = m.get("elapsed")
            dur = elapsed if elapsed is not None else (timing["span"] if timing else None)
            dur_src = "wall" if elapsed is not None else ("log" if timing else None)
            run = {"key": r["key"], "idx": i, "ts": r["ts"], "epoch": r["epoch"], "name": r["name"],
                   "verdict": r["verdict"], "detail": r["detail"], "program": cat["n"] if cat else None,
                   "has_trace": tr is not None, "log_lines": len(tr) if tr is not None else None,
                   "stages": st, "reason": reason, "elapsed": elapsed, "dur": dur, "dur_src": dur_src,
                   "spans": timing["spans"] if timing else None, "samples": m.get("samples") or [],
                   "pid_count": sum(len(v) for v in pids.values())}
            runs.append(run)
            detail[r["key"]] = dict(run, trace=tr or [], pids=pids, source=src, timing=timing)
        self._detail = detail

        counts: dict[str, int] = {}
        for ln in lines:
            counts[ln["agent"]] = counts.get(ln["agent"], 0) + 1
        agents = [{"name": a, "count": counts.pop(a, 0)} for a in AGENT_ORDER]
        agents += [{"name": a, "count": c} for a, c in sorted(counts.items())]

        conf_text = read_text(CONF_FILE, 64 * 1024)
        conf = parse_conf(conf_text)
        last = None
        lr = read_text(LAST_RESULT_FILE, 4096)
        if lr and "|" in lr:
            v, _, d = lr.strip().partition("|")
            last = {"verdict": v, "detail": d}
        inflight = None
        if self.runner and self.runner.busy() and tail:
            inflight = {"lines": len(tail), "last_agent": tail[-1]["agent"], "last_msg": tail[-1]["msg"],
                        "trace": tail[-60:]}
        job = self.runner.public() if self.runner else None
        self._state = {
            "now": time.time(),
            "meta": {"source": self.src.name, "why": self.why, "binary": BIN_REL, "port": self.port,
                     "platform": sys.platform, "watching": self.watcher is not None, "sleeps": self.sleeps,
                     "build": self.build, "flags": " ".join(GCC_FLAGS), "run_cap": RUN_CAP_SEC,
                     "caps": {"catalog": self.has_run, "custom": self.has_custom,
                              "code_max": self.code_max, "expected_max": 62},
                     "catalog": [{"n": c["n"], "name": c["name"], "filename": c["filename"]} for c in self.catalog]},
            "config": {"present": conf_text is not None, "values": conf, "defaults": self.defaults,
                       "mtime": (sig(CONF_FILE)[0] / 1e9) if sig(CONF_FILE) else None},
            "runs": runs, "agents": agents, "agent_lines": len(lines),
            "unparsed_history": bad, "last_result": last, "inflight": inflight, "job": job,
            "files": {"history": HISTORY_FILE.exists(), "activity": ACTIVITY_FILE.exists()},
        }

    def run_detail(self, key: str) -> dict | None:
        self.state()
        with self.lock:
            return self._detail.get(key)


# --------------------------------------------------------------------------
# Job runner: one subprocess at a time, own process group, hard cap
# --------------------------------------------------------------------------


class Busy(Exception):
    pass


class Job:
    def __init__(self, jid: str, kind: str, label: str):
        self.id, self.kind, self.label = jid, kind, label
        self.events: list[tuple[str, dict]] = []
        self.cond = threading.Condition()
        self.done = False
        self.cancel = False
        self.proc: subprocess.Popen | None = None
        self.out_bytes = 0
        self.capped = False
        self.started = time.time()

    def emit(self, typ: str, **data) -> None:
        with self.cond:
            self.events.append((typ, data))
            self.cond.notify_all()

    def finish(self) -> None:
        with self.cond:
            self.done = True
            self.events.append(("done", {}))
            self.cond.notify_all()


def killpg(proc: subprocess.Popen, sig_: int) -> None:
    try:
        os.killpg(proc.pid, sig_)
    except (ProcessLookupError, PermissionError):
        pass


class Runner:
    def __init__(self, model: Model):
        self.model = model
        self.lock = threading.Lock()
        self.current: Job | None = None
        self.jobs: dict[str, Job] = {}
        self.thread: threading.Thread | None = None
        model.runner = self

    def busy(self) -> bool:
        with self.lock:
            return bool(self.current and not self.current.done)

    def public(self) -> dict | None:
        with self.lock:
            j = self.current
            return None if j is None else {"id": j.id, "kind": j.kind, "label": j.label,
                                           "done": j.done, "started": j.started}

    def get(self, jid: str) -> Job | None:
        with self.lock:
            return self.jobs.get(jid)

    def start(self, kind: str, label: str, steps: list[dict]) -> Job:
        with self.lock:
            if self.current and not self.current.done:
                raise Busy()
            job = Job(secrets.token_hex(8), kind, label)
            self.current = job
            self.jobs = {job.id: job}      # keep only the latest job's buffer
            self.thread = threading.Thread(target=self._execute, args=(job, steps), daemon=True)
            self.thread.start()
            return job

    def stop(self) -> bool:
        with self.lock:
            j = self.current
        if not j or j.done:
            return False
        j.cancel = True
        if j.proc:
            killpg(j.proc, signal.SIGTERM)
        return True

    def shutdown(self) -> None:
        j = self.current
        if j and not j.done:
            j.cancel = True
            if j.proc:
                killpg(j.proc, signal.SIGTERM)
                time.sleep(0.4)
                killpg(j.proc, signal.SIGKILL)
        if self.thread:
            self.thread.join(timeout=3)

    # -- internals --------------------------------------------------------
    def _execute(self, job: Job, steps: list[dict]) -> None:
        try:
            for i, step in enumerate(steps):
                if job.cancel:
                    job.emit("note", text="Cancelled before the next step started.")
                    break
                self._step(job, i, len(steps), step)
        except Exception as exc:  # never let the worker die silently
            job.emit("note", text=f"internal error: {exc!r}")
        finally:
            job.finish()

    def _step(self, job: Job, idx: int, total: int, step: dict) -> None:
        before = len(self.model.history_rows())
        job.emit("cmd", index=idx, total=total, command=step["display"], label=step["label"])
        env = dict(os.environ, TERM="dumb", NO_COLOR="1")
        stdin_bytes = step.get("stdin")
        out_chunks: list[str] = []
        t0 = time.monotonic()
        try:
            proc = subprocess.Popen(step["argv"], cwd=ROOT, env=env, shell=False, close_fds=True,
                                    stdin=subprocess.PIPE if stdin_bytes is not None else subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    start_new_session=True)
        except OSError as exc:
            job.emit("note", text=f"could not start: {exc}")
            job.emit("exit", index=idx, code=None, signal=None, timed_out=False, elapsed=0.0, results=[])
            return
        job.proc = proc

        def pump(fh, name):
            import codecs
            dec = codecs.getincrementaldecoder("utf-8")("replace")
            fd = fh.fileno()
            while True:
                try:
                    chunk = os.read(fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                if job.out_bytes > OUTPUT_CAP:
                    if not job.capped:
                        job.capped = True
                        job.emit("note", text=f"Output exceeded {OUTPUT_CAP // 1024} KB - further output dropped.")
                    continue
                job.out_bytes += len(chunk)
                txt = dec.decode(chunk)
                if txt:
                    if name == "stdout" and len(out_chunks) < 400:
                        out_chunks.append(txt)
                    job.emit("out", stream=name, text=txt)

        readers = [threading.Thread(target=pump, args=(proc.stdout, "stdout"), daemon=True),
                   threading.Thread(target=pump, args=(proc.stderr, "stderr"), daemon=True)]
        for t in readers:
            t.start()

        def feed():
            try:
                proc.stdin.write(stdin_bytes)
                proc.stdin.close()
            except (BrokenPipeError, OSError, ValueError):
                pass
        if stdin_bytes is not None:
            threading.Thread(target=feed, daemon=True).start()

        done = threading.Event()
        state = {"timed_out": False}

        def watchdog():
            term_at = None
            while not done.wait(0.2):
                now = time.monotonic()
                if term_at is None and (job.cancel or now - t0 > RUN_CAP_SEC):
                    state["timed_out"] = not job.cancel
                    if state["timed_out"]:
                        job.emit("note", text=f"Hard {RUN_CAP_SEC} s cap reached - killing the process group.")
                    killpg(proc, signal.SIGTERM)
                    term_at = now
                elif term_at is not None and now - term_at > 2:
                    killpg(proc, signal.SIGKILL)
                    term_at = now
        threading.Thread(target=watchdog, daemon=True).start()

        rc = proc.wait()
        done.set()
        killpg(proc, signal.SIGKILL)          # sweep any stragglers in the group
        for t in readers:
            t.join(timeout=3)
        elapsed = time.monotonic() - t0
        for fh in (proc.stdout, proc.stderr):
            try:
                fh.close()
            except OSError:
                pass

        rows = self.model.history_rows()
        new = rows[before:] if len(rows) >= before else rows
        results = [{"name": r["name"], "verdict": r["verdict"], "detail": r["detail"], "ts": r["ts"],
                    "key": r["key"]} for r in new]
        if new:
            entry = {"elapsed": round(elapsed, 2), "via": "dashboard",
                     "start_mono": t0, "end_mono": t0 + elapsed,
                     "samples": parse_samples("".join(out_chunks))}
            save_meta_entry(new[-1]["key"], entry)
            if step.get("snapshot"):
                for r in reversed(new):
                    if r["name"] == "Custom Program":
                        CUSTOM_DIR.mkdir(parents=True, exist_ok=True)
                        (CUSTOM_DIR / f"{r['key']}.c").write_text(step["snapshot"])
                        break
        job.emit("exit", index=idx, code=rc if rc >= 0 else None, signal=-rc if rc < 0 else None,
                 timed_out=state["timed_out"], cancelled=job.cancel, elapsed=round(elapsed, 2), results=results)


def build_steps(model: Model, body: dict) -> tuple[str, str, list[dict]]:
    """Validate a POST body against the fixed allow-list. Raises ValueError with a message."""
    action = body.get("action")
    if action == "catalog":
        n = body.get("n")
        if not model.has_run:
            raise ValueError("this pipeline source has no --run mode")
        if type(n) is not int or not 1 <= n <= 5:
            raise ValueError("n must be an integer from 1 to 5")
        name = next((c["name"] for c in model.catalog if c["n"] == n), f"program {n}")
        return "catalog", name, [{"argv": [str(BIN), "--run", str(n)], "display": f"{BIN_REL} --run {n}",
                                  "label": name}]
    if action == "all":
        if not model.has_run:
            raise ValueError("this pipeline source has no --run mode")
        steps = []
        for n in range(1, 6):
            name = next((c["name"] for c in model.catalog if c["n"] == n), f"program {n}")
            steps.append({"argv": [str(BIN), "--run", str(n)], "display": f"{BIN_REL} --run {n}", "label": name})
        return "all", "All 5 catalog programs", steps
    if action == "custom":
        if not model.has_custom:
            raise ValueError("this pipeline source has no custom-code mode")
        code, expected = body.get("code"), body.get("expected", "")
        if not isinstance(code, str) or not isinstance(expected, str):
            raise ValueError("code and expected must be strings")
        code = code.replace("\r\n", "\n").replace("\r", "\n")
        if not code.strip():
            raise ValueError("code is empty")
        if "\0" in code or "\0" in expected:
            raise ValueError("NUL bytes are not allowed")
        raw = code.encode("utf-8")
        if len(raw) > CODE_MAX_BYTES:
            raise ValueError(f"code is {len(raw)} bytes; the limit is {CODE_MAX_BYTES} bytes")
        if len(raw) > model.code_max:
            raise ValueError(f"code is {len(raw)} bytes; this program's own buffer holds only "
                             f"{model.code_max} bytes and would desync its input")
        for ln in code.split("\n"):
            if ln.startswith("ENDCODE"):
                raise ValueError("a line starting with ENDCODE would end the paste early")
            if len(ln.encode("utf-8")) > 500:
                raise ValueError("a line is longer than 500 bytes (the program reads input 511 bytes at a time)")
        if "\n" in expected or "\r" in expected:
            raise ValueError("expected output must be a single line")
        if len(expected.encode("utf-8")) > 62:
            raise ValueError("expected output is longer than 62 bytes (the program's buffer is 64)")
        if not code.endswith("\n"):
            code += "\n"
        payload = f"1\n6\n{code}ENDCODE\n{expected}\n7\n".encode("utf-8")
        display = f"{BIN_REL}   # stdin: 1, 6, <{len(raw)} bytes of C>, ENDCODE, {expected!r}, 7"
        return "custom", "Custom program", [{"argv": [str(BIN)], "stdin": payload, "display": display,
                                              "label": "Custom Program", "snapshot": code}]
    raise ValueError("unknown action")


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

MODEL: Model
RUNNER: Runner
STOPPING = threading.Event()
HOST_RE = re.compile(r"^(127\.0\.0\.1|localhost|\[::1\])(?::(\d+))?$", re.I)


def origin_ok(origin: str, port: int) -> bool:
    try:
        u = urlsplit(origin)
        return (u.scheme == "http" and u.hostname in ("127.0.0.1", "localhost", "::1")
                and (u.port or 80) == port)
    except ValueError:
        return False


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "PipelineDashboard"
    sys_version = ""
    timeout = 60

    def log_message(self, fmt, *args):  # quiet; actions are logged explicitly
        pass

    # -- plumbing ---------------------------------------------------------
    def _base_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")

    def _send(self, code: int, body: bytes, ctype: str, extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self._base_headers()
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, code: int, obj) -> None:
        self._send(code, json.dumps(obj).encode(), "application/json; charset=utf-8")

    def _guard(self) -> bool:
        """Reject anything that is not a same-origin request to 127.0.0.1/localhost."""
        port = self.server.server_address[1]
        m = HOST_RE.match(self.headers.get("Host", ""))
        if not m or int(m.group(2) or 80) != port:
            self._json(403, {"error": "bad Host header"})
            return False
        origin = self.headers.get("Origin")
        if origin is not None and not origin_ok(origin, port):
            self._json(403, {"error": "bad Origin"})
            return False
        if self.headers.get("Sec-Fetch-Site") not in (None, "same-origin", "none", "same-site"):
            self._json(403, {"error": "cross-site request refused"})
            return False
        if self.command == "POST" and origin is None:
            self._json(403, {"error": "POST requires a same-origin Origin header"})
            return False
        return True

    # -- verbs ------------------------------------------------------------
    def do_GET(self):
        if not self._guard():
            return
        url = urlsplit(self.path)
        path, q = url.path, parse_qs(url.query)
        if path == "/":
            nonce = secrets.token_urlsafe(16)
            csp = ("default-src 'none'; script-src 'nonce-%s'; style-src 'nonce-%s'; connect-src 'self'; "
                   "img-src data:; base-uri 'none'; form-action 'none'" % (nonce, nonce))
            html = UI_HTML.replace("__NONCE__", nonce).encode()
            return self._send(200, html, "text/html; charset=utf-8",
                              {"Content-Security-Policy": csp})
        if path == "/favicon.ico":
            return self._send(204, b"", "image/x-icon")
        if path == "/api/state":
            st = MODEL.state()
            if q.get("rev", [""])[0] == st["rev"]:
                return self._json(200, {"unchanged": True, "rev": st["rev"], "job": st["job"]})
            return self._json(200, st)
        m = re.fullmatch(r"/api/run/([0-9a-f]{12})", path)
        if m:
            d = MODEL.run_detail(m.group(1))
            return self._json(200, d) if d else self._json(404, {"error": "no such run"})
        if path == "/api/stream":
            return self._stream(q.get("job", [""])[0])
        return self._json(404, {"error": "not found"})

    do_HEAD = do_GET

    def do_POST(self):
        if not self._guard():
            return
        path = urlsplit(self.path).path
        if path not in ("/api/run", "/api/stop"):
            return self._json(404, {"error": "not found"})
        ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if ctype != "application/json":
            return self._json(415, {"error": "Content-Type must be application/json"})
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            return self._json(411, {"error": "Content-Length required"})
        if length < 0 or length > MAX_BODY:
            self.close_connection = True
            return self._json(413, {"error": "body too large"})
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, UnicodeDecodeError):
            return self._json(400, {"error": "invalid JSON"})
        if not isinstance(body, dict):
            return self._json(400, {"error": "body must be a JSON object"})
        if path == "/api/stop":
            return self._json(200, {"stopped": RUNNER.stop()})
        try:
            kind, label, steps = build_steps(MODEL, body)
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})
        try:
            job = RUNNER.start(kind, label, steps)
        except Busy:
            return self._json(409, {"error": "a run is already in progress", "job": RUNNER.public()})
        print(f"  run: {kind} - {label}")
        return self._json(202, {"job": job.id, "kind": kind, "label": label})

    def _stream(self, jid: str):
        job = RUNNER.get(jid) if re.fullmatch(r"[0-9a-f]{16}", jid) else None
        if not job:
            return self._json(404, {"error": "no such job"})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self._base_headers()
        self.end_headers()
        self.close_connection = True
        try:
            nxt = 0
            try:
                nxt = int(self.headers.get("Last-Event-ID", "-1")) + 1
            except ValueError:
                pass
            last_ping = time.monotonic()
            while not STOPPING.is_set():
                with job.cond:
                    while nxt >= len(job.events) and not STOPPING.is_set():
                        if time.monotonic() - last_ping > 10:
                            break
                        job.cond.wait(timeout=1.0)
                    batch = job.events[nxt:nxt + 200]
                if not batch:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    last_ping = time.monotonic()
                    continue
                out = []
                for typ, data in batch:
                    out.append(f"id: {nxt}\nevent: {typ}\ndata: {json.dumps(data)}\n\n")
                    nxt += 1
                    if typ == "done":
                        self.wfile.write("".join(out).encode())
                        self.wfile.flush()
                        return
                self.wfile.write("".join(out).encode())
                self.wfile.flush()
                last_ping = time.monotonic()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self):
        # skip HTTPServer's socket.getfqdn() reverse-DNS lookup: it can stall for many seconds offline
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


def make_server() -> ThreadingHTTPServer:
    last = None
    for p in range(FIRST_PORT, FIRST_PORT + PORT_TRIES):
        try:
            return Server(("127.0.0.1", p), Handler)
        except OSError as exc:
            last = exc
    sys.exit(f"No free port in {FIRST_PORT}-{FIRST_PORT + PORT_TRIES - 1}: {last}")


def main() -> None:
    global MODEL, RUNNER
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, ValueError):
        pass
    print("Pipeline Operations Dashboard")
    info = choose_source()
    src: Path = info["path"]
    print(f"  source : {src.name} - {info['why']}")
    build = ensure_binary(src)
    print(f"  binary : {BIN_REL} ({build['status']})")
    MODEL = Model(src, build, info["why"])
    RUNNER = Runner(MODEL)
    MODEL.watcher = Watcher()
    MODEL.watcher.start()
    if not MODEL.has_run:
        print("  note   : this source has no --run mode, so catalog buttons will be disabled.")
    server = make_server()
    MODEL.port = server.server_address[1]
    url = f"http://127.0.0.1:{MODEL.port}"
    print(f"\n  Dashboard: {url}\n  (Ctrl+C to stop)\n")

    def on_term(signum, frame):
        raise KeyboardInterrupt
    # explicit, so Ctrl+C works even if this process was started with SIGINT ignored
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, on_term)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, on_term)

    if not os.environ.get("DASH_NO_OPEN"):
        threading.Timer(0.6, lambda: _try_open(url)).start()
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        print("\nStopping...")
        STOPPING.set()
        MODEL.watcher.halt.set()
        RUNNER.shutdown()
        server.server_close()
        print("Stopped.")


def _try_open(url: str) -> None:
    try:
        webbrowser.open(url)
    except Exception:
        pass


UI_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>Pipeline Operations</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='7' fill='%234338ca'/%3E%3Cpath d='M7 16h5m4 0h9M20 11l5 5-5 5' stroke='white' stroke-width='2.4' fill='none' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<script nonce="__NONCE__">
/* apply the remembered theme before first paint */
(function () {
  try { var t = localStorage.getItem('pd-theme'); if (t === 'light' || t === 'dark') document.documentElement.setAttribute('data-theme', t); } catch (e) {}
})();
</script>
<style nonce="__NONCE__">
:root {
  --bg: #f6f4ee; --surface: #fffdf8; --surface-2: #f0ede4; --ink: #1c1a16; --ink-2: #4f4b43; --ink-3: #6b665c;
  --line: #e3ded1; --line-strong: #c9c2b0; --focus: #4338ca; --neutral: #64748b; --none: #b9b3a3;
  --pass: #0ca30c; --skip: #fab219; --fail: #d03b3b; --int: #64748b;
  --a-gen: #0e7490; --a-rev: #a21caf; --a-exe: #1d4ed8; --a-tst: #15803d; --a-ctl: #4338ca; --a-mon: #c2410c;
  --term-bg: #15171c; --term-ink: #e6e3da; --term-dim: #8d8a82; --term-err: #f0a08a;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  --sans: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  color-scheme: light;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #131419; --surface: #1a1c23; --surface-2: #22252d; --ink: #ece9e1; --ink-2: #b9b5ab; --ink-3: #928e84;
    --line: #2b2e37; --line-strong: #3d414d; --focus: #9a93ff; --neutral: #94a3b8; --none: #4a4d57; --int: #94a3b8;
    --a-gen: #3cc4de; --a-rev: #de7fe8; --a-exe: #7ea6ff; --a-tst: #4fd37f; --a-ctl: #9a93ff; --a-mon: #fb9a5c;
    --term-bg: #0d0e12;
    color-scheme: dark;
  }
}
:root[data-theme="dark"] {
  --bg: #131419; --surface: #1a1c23; --surface-2: #22252d; --ink: #ece9e1; --ink-2: #b9b5ab; --ink-3: #928e84;
  --line: #2b2e37; --line-strong: #3d414d; --focus: #9a93ff; --neutral: #94a3b8; --none: #4a4d57; --int: #94a3b8;
  --a-gen: #3cc4de; --a-rev: #de7fe8; --a-exe: #7ea6ff; --a-tst: #4fd37f; --a-ctl: #9a93ff; --a-mon: #fb9a5c;
  --term-bg: #0d0e12;
  color-scheme: dark;
}
* { box-sizing: border-box; }
[hidden] { display: none !important; }
html { -webkit-text-size-adjust: 100%; scroll-padding-top: 64px; }
body { margin: 0; background: var(--bg); color: var(--ink); font: 15px/1.5 var(--sans); }
h1, h2, h3, p { margin: 0; }
button, input, textarea, select { font: inherit; color: inherit; }
button { cursor: pointer; }
button:disabled { cursor: not-allowed; opacity: .5; }
:focus-visible { outline: 2px solid var(--focus); outline-offset: 2px; border-radius: 4px; }
.sprite { position: absolute; }
#customBox { margin-top: 14px; }
.runrow { margin-top: 12px; }
#customBox summary { cursor: pointer; font-weight: 600; }
#dPos { font-size: 13px; }
.tip span { display: block; }
.sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }
.skiplink { position: absolute; left: 8px; top: -48px; background: var(--surface); padding: 8px 12px; border: 1px solid var(--line-strong); border-radius: 6px; z-index: 100; }
.skiplink:focus { top: 8px; }
.wrap { max-width: 1160px; margin: 0 auto; padding: 0 16px; }
svg.i { width: 1em; height: 1em; flex: none; vertical-align: -0.125em; fill: none; stroke: currentColor; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }

/* header + nav */
.top { padding: 28px 0 18px; }
.top .wrap { display: flex; gap: 16px; align-items: flex-end; justify-content: space-between; flex-wrap: wrap; }
.kicker { font-size: 11px; letter-spacing: .09em; text-transform: uppercase; color: var(--ink-3); font-weight: 600; }
h1 { font-size: clamp(26px, 5vw, 36px); line-height: 1.1; letter-spacing: -.02em; font-weight: 650; margin-top: 4px; }
.sub { color: var(--ink-2); margin-top: 8px; max-width: 70ch; overflow-wrap: anywhere; }
.sub code, code { font-family: var(--mono); font-size: .92em; background: var(--surface-2); padding: 1px 5px; border-radius: 4px; }
.tools { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
.live { display: inline-flex; align-items: center; gap: 8px; font-size: 13px; color: var(--ink-2); padding: 6px 10px; border: 1px solid var(--line); border-radius: 999px; background: var(--surface); }
.live .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--none); }
.live.ok .dot { background: var(--pass); animation: pulse 2s ease-out infinite; }
.live.bad .dot { background: var(--fail); }
@keyframes pulse { 0% { box-shadow: 0 0 0 0 color-mix(in srgb, var(--pass) 55%, transparent); } 100% { box-shadow: 0 0 0 8px transparent; } }
.btn { display: inline-flex; align-items: center; gap: 8px; border: 1px solid var(--line-strong); background: var(--surface); padding: 7px 12px; border-radius: 8px; font-size: 14px; min-height: 36px; }
.btn:hover:not(:disabled) { background: var(--surface-2); }
.btn.primary { background: var(--ink); color: var(--bg); border-color: var(--ink); font-weight: 600; }
.btn.primary:hover:not(:disabled) { background: var(--ink-2); }
.btn.danger { border-color: var(--fail); }
.btn.sm { padding: 4px 9px; min-height: 30px; font-size: 13px; }
.secnav { position: sticky; top: 0; z-index: 30; background: color-mix(in srgb, var(--bg) 92%, transparent); backdrop-filter: blur(8px); border-bottom: 1px solid var(--line); }
.secnav .wrap { display: flex; gap: 4px; overflow-x: auto; scrollbar-width: none; }
.secnav a { color: var(--ink-2); text-decoration: none; padding: 12px 12px; font-size: 14px; white-space: nowrap; border-bottom: 2px solid transparent; }
.secnav a:hover { color: var(--ink); }
.secnav a[aria-current="true"] { color: var(--ink); border-bottom-color: var(--ink); font-weight: 600; }
main { padding: 24px 0 64px; }
section.panel, .card { background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 18px; min-width: 0; }
section.panel { margin-top: 20px; }
.ph { margin-bottom: 14px; }
.ph h2 { font-size: 18px; letter-spacing: -.01em; font-weight: 650; }
.ph .desc { color: var(--ink-3); font-size: 13px; margin-top: 2px; }
.insight { margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--line); color: var(--ink-2); font-size: 14px; overflow-wrap: anywhere; }
.insight::before { content: "Insight  "; font-size: 11px; letter-spacing: .08em; text-transform: uppercase; color: var(--ink-3); font-weight: 600; margin-right: 6px; }
.muted { color: var(--ink-3); }
.grid2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; }
.grid2 > section.panel { margin-top: 20px; }
details.alt { margin-top: 12px; }
details.alt summary { cursor: pointer; font-size: 13px; color: var(--ink-3); width: fit-content; }
details.alt summary:hover { color: var(--ink); }

/* pipeline strip */
.strip { display: flex; align-items: stretch; gap: 0; }
.stage { flex: 1 1 0; min-width: 0; border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; background: var(--surface); border-top: 3px solid var(--c, var(--line)); }
.stage .nm { font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
.stage .nm .sw { width: 9px; height: 9px; border-radius: 3px; background: var(--c); flex: none; }
.stage .st { margin-top: 8px; display: flex; gap: 8px; align-items: flex-start; font-size: 14px; }
.stage .note { color: var(--ink-3); font-size: 12.5px; margin-top: 4px; overflow-wrap: anywhere; }
.arrow { flex: none; width: 28px; display: grid; place-items: center; color: var(--ink-3); }
.stage.running .st .ic { animation: blink 1.2s ease-in-out infinite; }
@keyframes blink { 50% { opacity: .35; } }
.ic { display: inline-grid; place-items: center; flex: none; }
.ic.pass { color: var(--pass); } .ic.fail { color: var(--fail); } .ic.skip { color: #c98a00; } .ic.none, .ic.waiting { color: var(--none); } .ic.interrupted, .ic.int { color: var(--int); } .ic.running { color: var(--a-ctl); }
:root[data-theme="dark"] .ic.skip { color: var(--skip); }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) .ic.skip { color: var(--skip); } }

/* KPI */
.kpis { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 16px; }
.kpi { border: 1px solid var(--line); border-radius: 10px; padding: 14px; background: var(--surface); min-width: 0; }
.kpi .lb { font-size: 12px; color: var(--ink-3); display: flex; align-items: center; gap: 6px; font-weight: 600; letter-spacing: .02em; }
.kpi .num { font-size: 34px; font-weight: 650; letter-spacing: -.02em; line-height: 1.15; margin-top: 4px; }
.kpi .sm { color: var(--ink-3); font-size: 12.5px; }
.kpi .spark { margin-top: 6px; display: block; width: 100%; height: 34px; overflow: visible; }
.spark:focus-visible { outline-offset: 4px; }

/* bars */
.legend { display: flex; flex-wrap: wrap; gap: 6px 16px; font-size: 13px; color: var(--ink-2); margin: 0 0 10px; padding: 0; list-style: none; }
.legend li { display: inline-flex; align-items: center; gap: 6px; }
.sw { display: inline-block; width: 10px; height: 10px; border-radius: 3px; flex: none; }
.barrow { display: grid; grid-template-columns: minmax(80px, 190px) minmax(0, 1fr) auto; gap: 12px; align-items: center; margin: 7px 0; }
.barlab { font-size: 13px; overflow-wrap: anywhere; line-height: 1.3; }
.barval { font-size: 13px; color: var(--ink-2); min-width: 2.5ch; text-align: right; }
.track { height: 22px; position: relative; background:
  linear-gradient(var(--line), var(--line)) 0 0 / 1px 100% no-repeat,
  linear-gradient(var(--line), var(--line)) 25% 0 / 1px 100% no-repeat,
  linear-gradient(var(--line), var(--line)) 50% 0 / 1px 100% no-repeat,
  linear-gradient(var(--line), var(--line)) 75% 0 / 1px 100% no-repeat,
  linear-gradient(var(--line), var(--line)) 100% 0 / 1px 100% no-repeat; }
.track.hero { height: 24px; }
.fill { display: flex; gap: 2px; height: 100%; }
.seg { flex: 1 1 0; min-width: 3px; height: 100%; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 600; overflow: hidden; background: var(--c); color: var(--t, #fff); }
.seg:last-child { border-radius: 0 4px 4px 0; }
.seg .sl { white-space: nowrap; padding: 0 4px; }
.seg .sl.off { visibility: hidden; }
.seg:hover { filter: brightness(1.08); }

/* timeline */
.scrollx { overflow-x: auto; overflow-y: hidden; }
.tl text { font-size: 11px; fill: var(--ink-3); }
.tl .grid { stroke: var(--line); stroke-width: 1; }
.tl .axis { stroke: var(--line-strong); stroke-width: 1; }
.mark { cursor: pointer; outline: none; }
.mark .hit { fill: transparent; }
.mark .shape { stroke: var(--surface); stroke-width: 1.5; }
.mark:hover .shape, .mark:focus-visible .shape { stroke: var(--ink); stroke-width: 2; }
.mark.sel .ring { fill: none; stroke: var(--ink); stroke-width: 1.5; }
.mark:focus-visible .ring { fill: none; stroke: var(--focus); stroke-width: 2; }
.mark .ring { fill: none; stroke: none; }
.tip { position: fixed; z-index: 90; pointer-events: none; max-width: min(320px, calc(100vw - 16px)); background: var(--ink); color: var(--bg); padding: 7px 10px; border-radius: 8px; font-size: 12.5px; line-height: 1.4; overflow-wrap: anywhere; box-shadow: 0 6px 20px rgba(0,0,0,.25); }
.tip b { display: block; }

/* performance */
.pfmode { display: inline-flex; gap: 8px; align-items: center; font-size: 13px; color: var(--ink-2); margin-bottom: 10px; }
.pfmode select { border: 1px solid var(--line-strong); background: var(--surface); border-radius: 8px; padding: 5px 8px; }
.range { position: absolute; top: 3px; bottom: 3px; background: var(--c); border-radius: 0 4px 4px 0; min-width: 3px; }
.range:hover { filter: brightness(1.1); }
.axis { position: relative; height: 16px; font-size: 11px; color: var(--ink-3); }
.axis span { position: absolute; top: 0; transform: translateX(-50%); white-space: nowrap; }
.axis span:first-child { transform: none; } .axis span:last-child { transform: translateX(-100%); }
.pfnote { color: var(--ink-2); font-size: 14px; border: 1px dashed var(--line-strong); border-radius: 10px; padding: 12px 14px; }
.seg[role="button"] { cursor: pointer; }
/* config strip */
.limits { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
.limit { border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; }
.limit .v { font-size: 26px; font-weight: 650; letter-spacing: -.02em; }
.limit .u { font-size: 13px; color: var(--ink-3); font-weight: 400; margin-left: 4px; }
.limit .lb { font-size: 12px; color: var(--ink-3); font-weight: 600; }
pre.raw { background: var(--surface-2); padding: 10px; border-radius: 8px; overflow: auto; max-height: 260px; font: 12px/1.45 var(--mono); white-space: pre-wrap; overflow-wrap: anywhere; margin: 8px 0 0; }

/* table */
.controls { display: flex; flex-wrap: wrap; gap: 10px 14px; align-items: center; margin-bottom: 12px; }
.chips { display: flex; gap: 6px; flex-wrap: wrap; }
.chip { display: inline-flex; align-items: center; gap: 6px; padding: 5px 11px; border-radius: 999px; border: 1px solid var(--line-strong); background: var(--surface); font-size: 13px; min-height: 32px; }
.chip[aria-pressed="true"] { background: var(--ink); color: var(--bg); border-color: var(--ink); }
.chip[aria-pressed="true"] .ic { color: inherit; }
.chip .n { color: var(--ink-3); font-size: 12px; } .chip[aria-pressed="true"] .n { color: inherit; opacity: .8; }
.search { flex: 1 1 180px; max-width: 320px; display: flex; align-items: center; gap: 8px; border: 1px solid var(--line-strong); border-radius: 8px; padding: 0 10px; background: var(--surface); min-height: 36px; }
.search input { border: 0; background: transparent; outline: none; width: 100%; padding: 6px 0; min-width: 0; }
.search:focus-within { outline: 2px solid var(--focus); outline-offset: 2px; }
.count { color: var(--ink-2); font-size: 13px; margin-left: auto; }
.tablewrap { overflow: auto; max-height: 560px; border: 1px solid var(--line); border-radius: 10px; }
table { border-collapse: collapse; width: 100%; min-width: 760px; font-size: 14px; }
th, td { text-align: left; padding: 9px 12px; border-bottom: 1px solid var(--line); vertical-align: top; font-variant-numeric: tabular-nums; }
thead th { position: sticky; top: 0; background: var(--surface-2); font-size: 12px; letter-spacing: .04em; text-transform: uppercase; color: var(--ink-2); white-space: nowrap; z-index: 1; }
thead th button { all: unset; cursor: pointer; display: inline-flex; gap: 6px; align-items: center; }
thead th button:focus-visible { outline: 2px solid var(--focus); outline-offset: 2px; }
tbody tr:last-child td { border-bottom: 0; }
tbody tr[data-key] { cursor: pointer; }
tbody tr[data-key]:hover td { background: var(--surface-2); }
tbody td:nth-child(2) { white-space: nowrap; }
td.det { max-width: 420px; overflow-wrap: anywhere; color: var(--ink-2); }
td .lnk { all: unset; cursor: pointer; font-weight: 600; text-decoration: underline; text-decoration-color: var(--line-strong); text-underline-offset: 3px; overflow-wrap: anywhere; }
td .lnk:focus-visible { outline: 2px solid var(--focus); outline-offset: 2px; }
.badge { display: inline-flex; align-items: center; gap: 6px; padding: 2px 9px 2px 7px; border-radius: 999px; font-size: 13px; font-weight: 600; background: color-mix(in srgb, var(--c) 16%, transparent); white-space: nowrap; }
.badge .ic { color: var(--c); }
.mini { display: inline-flex; gap: 3px; }
.empty-row td { text-align: center; color: var(--ink-3); padding: 28px; }

/* empty */
.emptycard { margin-top: 20px; text-align: left; padding: 28px; border: 1px dashed var(--line-strong); border-radius: 12px; background: var(--surface); }
.emptycard h2 { font-size: 22px; }
.emptycard p { color: var(--ink-2); margin: 8px 0 16px; max-width: 62ch; }
body.is-empty [data-needs-data] { display: none !important; }

/* run panel + terminal */
.runbtns { display: flex; flex-wrap: wrap; gap: 8px; }
.runbtns .btn { text-align: left; }
.runbtns .n { font-weight: 700; color: var(--ink-3); }
.field { display: block; margin-top: 14px; }
.field > span { display: block; font-size: 13px; font-weight: 600; margin-bottom: 4px; }
textarea.code { width: 100%; min-height: 150px; resize: vertical; font: 13px/1.45 var(--mono); padding: 10px; border: 1px solid var(--line-strong); border-radius: 8px; background: var(--surface); tab-size: 4; }
input.text { width: 100%; padding: 8px 10px; border: 1px solid var(--line-strong); border-radius: 8px; background: var(--surface); font-family: var(--mono); font-size: 13px; }
.msg { font-size: 13px; margin-top: 8px; min-height: 1.4em; color: var(--ink-2); overflow-wrap: anywhere; }
.msg.err { color: var(--fail); }
.term { margin-top: 16px; border-radius: 10px; overflow: hidden; background: var(--term-bg); color: var(--term-ink); border: 1px solid var(--line); }
.termhead { display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: center; padding: 9px 12px; border-bottom: 1px solid rgba(255,255,255,.1); font: 12.5px/1.4 var(--mono); }
.termhead .cmd { flex: 1 1 260px; min-width: 0; overflow-wrap: anywhere; color: var(--term-ink); }
.termhead .cmd::before { content: "$ "; color: var(--term-dim); }
.termhead .meta { color: var(--term-dim); display: inline-flex; gap: 12px; align-items: center; flex-wrap: wrap; }
.tbadge { display: inline-flex; gap: 6px; align-items: center; padding: 2px 8px; border-radius: 999px; font: 600 12px var(--sans); background: rgba(255,255,255,.1); color: var(--term-ink); }
.tbadge.pass { background: color-mix(in srgb, var(--pass) 30%, transparent); }
.tbadge.fail { background: color-mix(in srgb, var(--fail) 40%, transparent); }
.tbadge.skip { background: color-mix(in srgb, var(--skip) 30%, transparent); }
.tbadge.run { background: color-mix(in srgb, #7ea6ff 30%, transparent); }
.termbody { margin: 0; padding: 12px; height: 320px; overflow: auto; font: 12.5px/1.5 var(--mono); white-space: pre-wrap; overflow-wrap: anywhere; }
.termbody .e { color: var(--term-err); }
.termbody .sep { color: var(--term-dim); display: block; margin: 6px 0; }
.termbody .sys { color: #9ec1ff; display: block; }
.termbody .idle { color: var(--term-dim); }

/* drawer */
.scrim { position: fixed; inset: 0; background: rgba(10, 10, 14, .45); z-index: 60; opacity: 0; transition: opacity .18s; }
.scrim.open { opacity: 1; }
.drawer { position: fixed; top: 0; right: 0; bottom: 0; width: min(620px, 100vw); background: var(--surface); border-left: 1px solid var(--line-strong); z-index: 70; display: flex; flex-direction: column; transform: translateX(100%); transition: transform .2s ease; }
.drawer.open { transform: none; }
.dhead { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 10px 14px; border-bottom: 1px solid var(--line); }
.dnav { display: flex; align-items: center; gap: 8px; }
.dbody { overflow: auto; padding: 16px 18px 40px; flex: 1; }
.dbody h2 { font-size: 20px; letter-spacing: -.01em; overflow-wrap: anywhere; }
.dbody h3 { font-size: 13px; letter-spacing: .06em; text-transform: uppercase; color: var(--ink-3); font-weight: 650; margin: 22px 0 8px; }
.dmeta { display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: center; margin-top: 8px; color: var(--ink-2); font-size: 14px; }
.stepper { list-style: none; margin: 0; padding: 0; display: grid; gap: 0; }
.stepper li { display: grid; grid-template-columns: 28px 1fr; gap: 10px; padding-bottom: 14px; position: relative; }
.stepper li::before { content: ""; position: absolute; left: 13px; top: 26px; bottom: 0; width: 1px; background: var(--line-strong); }
.stepper li:last-child::before { display: none; }
.stepper .dot { width: 26px; height: 26px; border-radius: 50%; display: grid; place-items: center; border: 1.5px solid var(--c, var(--line-strong)); background: var(--surface); font-size: 14px; }
.stepper .t { font-weight: 600; font-size: 14px; display: flex; gap: 8px; flex-wrap: wrap; align-items: baseline; }
.stepper .t small { font-weight: 500; color: var(--ink-2); font-size: 12.5px; }
.stepper .nt { color: var(--ink-2); font-size: 13px; overflow-wrap: anywhere; }
.stepper li.running .dot { animation: blink 1s ease-in-out infinite; border-color: var(--a-ctl); }
dl.facts { display: grid; grid-template-columns: max-content 1fr; gap: 6px 14px; margin: 0; font-size: 14px; }
dl.facts dt { color: var(--ink-3); } dl.facts dd { margin: 0; overflow-wrap: anywhere; }
.pidchip { display: inline-flex; align-items: center; gap: 5px; margin: 0 6px 4px 0; padding: 1px 8px; border-radius: 999px; border: 1px solid var(--line-strong); font-size: 12.5px; }
.pidchip .sw { width: 8px; height: 8px; }
.trhead { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
.trhead h3 { margin: 22px 0 8px; }
.rctl { display: flex; gap: 6px; flex-wrap: wrap; }
ol.trace { list-style: none; margin: 0; padding: 0; border: 1px solid var(--line); border-radius: 10px; max-height: 340px; overflow: auto; font: 12.5px/1.45 var(--mono); }
ol.trace li { display: grid; grid-template-columns: 112px 1fr; gap: 8px; padding: 5px 10px; border-bottom: 1px solid var(--line); border-left: 3px solid var(--c, transparent); }
ol.trace li:last-child { border-bottom: 0; }
ol.trace .ag { color: var(--c); font-weight: 700; font-family: var(--sans); font-size: 12px; overflow-wrap: anywhere; }
:root[data-theme="light"] ol.trace .ag { color: var(--c); }
ol.trace .ms { overflow-wrap: anywhere; }
ol.trace .pid { color: var(--ink-3); }
ol.trace.replaying li.pending { opacity: .3; }
ol.trace li.cur { background: color-mix(in srgb, var(--c, var(--a-ctl)) 16%, transparent); }
.code-view { border: 1px solid var(--line); border-radius: 10px; overflow: auto; max-height: 360px; font: 12.5px/1.5 var(--mono); background: var(--surface-2); }
.code-view .ln { display: grid; grid-template-columns: 38px 1fr; }
.code-view .no { text-align: right; padding-right: 10px; color: var(--ink-3); user-select: none; }
.code-view .tx { white-space: pre; padding-right: 12px; }
.code-view .flag { background: color-mix(in srgb, var(--fail) 16%, transparent); box-shadow: inset 3px 0 0 var(--fail); }
.warnline { color: var(--fail); font-size: 13px; margin: 6px 0; display: flex; gap: 6px; align-items: flex-start; }

/* confirm dialog */
dialog { border: 1px solid var(--line-strong); border-radius: 12px; background: var(--surface); color: var(--ink); max-width: min(520px, calc(100vw - 24px)); padding: 20px; }
dialog::backdrop { background: rgba(10,10,14,.5); }
dialog h2 { font-size: 18px; }
dialog p { margin: 10px 0; color: var(--ink-2); font-size: 14px; }
dialog pre { background: var(--surface-2); padding: 10px; border-radius: 8px; max-height: 160px; overflow: auto; font: 12px/1.45 var(--mono); white-space: pre-wrap; overflow-wrap: anywhere; }
dialog .row { display: flex; gap: 8px; justify-content: flex-end; margin-top: 14px; flex-wrap: wrap; }
footer { margin-top: 32px; color: var(--ink-3); font-size: 12.5px; }

@media (max-width: 860px) {
  .grid2 { grid-template-columns: minmax(0, 1fr); }
  .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .limits { grid-template-columns: minmax(0, 1fr); }
  .strip { flex-direction: column; }
  .arrow { width: auto; height: 24px; transform: rotate(90deg); }
}
@media (max-width: 520px) {
  .barrow { grid-template-columns: minmax(0, 1fr) auto; }
  .barrow .track { grid-column: 1 / -1; grid-row: 2; }
  .kpi .num { font-size: 28px; }
  ol.trace li { grid-template-columns: 1fr; gap: 0; }
  .count { margin-left: 0; width: 100%; }
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation: none !important; transition: none !important; scroll-behavior: auto !important; }
}
</style>
</head>
<body>
<svg class="sprite" width="0" height="0" aria-hidden="true" focusable="false">
  <defs>
    <symbol id="i-pass" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="m8 12.5 3 3 5-6"/></symbol>
    <symbol id="i-fail" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="m9 9 6 6M15 9l-6 6"/></symbol>
    <symbol id="i-skip" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M8 12h8"/><path d="m13.5 9.5 2.5 2.5-2.5 2.5"/></symbol>
    <symbol id="i-int" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><rect x="9" y="9" width="6" height="6" rx="1"/></symbol>
    <symbol id="i-none" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9" stroke-dasharray="3 3"/></symbol>
    <symbol id="i-waiting" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9" stroke-dasharray="3 3"/></symbol>
    <symbol id="i-running" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M10 8.5v7l5.5-3.5z"/></symbol>
    <symbol id="i-warn" viewBox="0 0 24 24"><path d="M12 4 3 20h18z"/><path d="M12 10v4M12 17v.01"/></symbol>
    <symbol id="i-play" viewBox="0 0 24 24"><path d="M7 5v14l12-7z"/></symbol>
    <symbol id="i-pause" viewBox="0 0 24 24"><path d="M8 5v14M16 5v14"/></symbol>
    <symbol id="i-step" viewBox="0 0 24 24"><path d="M6 5v14l9-7zM18 5v14"/></symbol>
    <symbol id="i-reset" viewBox="0 0 24 24"><path d="M4 12a8 8 0 1 0 3-6.2M4 4v5h5"/></symbol>
    <symbol id="i-close" viewBox="0 0 24 24"><path d="M6 6l12 12M18 6 6 18"/></symbol>
    <symbol id="i-prev" viewBox="0 0 24 24"><path d="m15 5-7 7 7 7"/></symbol>
    <symbol id="i-next" viewBox="0 0 24 24"><path d="m9 5 7 7-7 7"/></symbol>
    <symbol id="i-down" viewBox="0 0 24 24"><path d="M12 4v12m-5-5 5 5 5-5M5 20h14"/></symbol>
    <symbol id="i-search" viewBox="0 0 24 24"><circle cx="11" cy="11" r="6"/><path d="m20 20-4.5-4.5"/></symbol>
    <symbol id="i-stop" viewBox="0 0 24 24"><rect x="6" y="6" width="12" height="12" rx="2"/></symbol>
    <symbol id="i-theme" viewBox="0 0 24 24"><circle cx="12" cy="12" r="8"/><path d="M12 4v16"/><path d="M12 4a8 8 0 0 1 0 16z" fill="currentColor"/></symbol>
    <symbol id="i-arrow" viewBox="0 0 24 24"><path d="M5 12h14m-5-5 5 5-5 5"/></symbol>
    <symbol id="i-sortn" viewBox="0 0 24 24"><path d="m8 10 4-4 4 4M8 14l4 4 4-4" opacity=".4"/></symbol>
    <symbol id="i-sorta" viewBox="0 0 24 24"><path d="m7 14 5-5 5 5"/></symbol>
    <symbol id="i-sortd" viewBox="0 0 24 24"><path d="m7 10 5 5 5-5"/></symbol>
  </defs>
</svg>
<a class="skiplink" href="#main">Skip to content</a>
<noscript><p class="wrap">This dashboard needs JavaScript.</p></noscript>

<header class="top" id="topHeader">
  <div class="wrap">
    <div>
      <div class="kicker">Multi-agent execution pipeline</div>
      <h1>Pipeline Operations</h1>
      <p class="sub" id="subtitle">Reading the program's logs…</p>
    </div>
    <div class="tools">
      <span class="live" id="live" role="status"><i class="dot"></i><span id="liveText">Connecting…</span></span>
      <button class="btn" id="themeBtn" type="button"><svg class="i"><use href="#i-theme"/></svg><span id="themeLabel">Theme: Auto</span></button>
    </div>
  </div>
</header>
<nav class="secnav" id="secnav" aria-label="Sections">
  <div class="wrap">
    <a href="#overview">Overview</a>
    <a href="#timeline" data-needs-data>Timeline</a>
    <a href="#performance" data-needs-data>Performance</a>
    <a href="#breakdown" data-needs-data>Breakdown</a>
    <a href="#agents" data-needs-data>Agents</a>
    <a href="#history" data-needs-data>History</a>
    <a href="#run">Run</a>
  </div>
</nav>

<main id="main" class="wrap" tabindex="-1">
  <section id="empty" class="emptycard" hidden aria-labelledby="emptyT">
    <h2 id="emptyT">No runs recorded yet</h2>
    <p>This dashboard only shows what the pipeline itself writes to <code>logs/</code>, so there is nothing to display until it has run. Nothing on this page is sample data.</p>
    <button class="btn primary" id="emptyRun" type="button"><svg class="i"><use href="#i-play"/></svg>Run all 5 demos</button>
    <p class="msg" id="emptyMsg" role="status"></p>
  </section>

  <section id="overview" class="panel" aria-labelledby="ovT">
    <div class="ph"><h2 id="ovT">Overview</h2><p class="desc">The four agents, as seen in the most recent run, and totals across everything in <code>logs/run_history.log</code>.</p></div>
    <div class="strip" id="strip" role="list" aria-label="Pipeline stages of the latest run"></div>
    <p class="insight" id="insStrip"></p>
    <div class="kpis" id="kpis"></div>
    <p class="insight" id="insKpi"></p>
  </section>

  <section id="timeline" class="panel" data-needs-data aria-labelledby="tlT">
    <div class="ph"><h2 id="tlT">Runs over time</h2><p class="desc">One mark per run at its recorded time. Overlapping runs are stacked vertically so each stays selectable; the vertical position carries no other meaning.</p></div>
    <ul class="legend" id="tlLegend"></ul>
    <div class="scrollx" id="tlScroll"><svg id="tlSvg" class="tl" role="group" aria-label="Runs over time"></svg></div>
    <p class="insight" id="insTl"></p>
    <details class="alt"><summary>View as table</summary><div class="tablewrap" id="tlTable"></div></details>
  </section>

  <div id="performance" data-needs-data>
    <section class="panel" aria-labelledby="pfT">
      <div class="ph"><h2 id="pfT">Performance</h2><p class="desc">How long runs take and where the time goes. The pipeline's logs carry no timestamps, so durations are measured by this dashboard: wall-clock time for runs it starts, and the moment each log line appears for any run it sees while open.</p></div>
      <div class="kpis" id="pfKpis"></div>
      <p class="insight" id="insPfK"></p>
      <p class="pfnote" id="pfSleep" hidden></p>
    </section>
    <div class="grid2">
      <section class="panel" aria-labelledby="pdT">
        <div class="ph"><h2 id="pdT">How long each run took</h2><p class="desc">Latest 20 timed runs, newest first. Select a bar to open the run.</p></div>
        <ul class="legend" id="pdLegend"></ul>
        <div id="pfRuns"></div>
        <p class="insight" id="insPfR"></p>
        <details class="alt"><summary>View as table</summary><div class="tablewrap" id="pfRunsTable"></div></details>
      </section>
      <section class="panel" aria-labelledby="gaT">
        <div class="ph"><h2 id="gaT">Where the time goes</h2><p class="desc">From each agent's first to its last log line inside a run. Agents run at the same time, so bars overlap: this is a timeline, not a sum.</p></div>
        <label class="pfmode">View <select id="pfMode"><option value="pass">Average of passing runs</option><option value="avg">Average of all timed runs</option><option value="latest">Latest timed run</option></select></label>
        <ul class="legend" id="gaLegend"></ul>
        <div id="pfGantt"></div>
        <p class="insight" id="insPfG"></p>
        <details class="alt"><summary>View as table</summary><div class="tablewrap" id="pfGanttTable"></div></details>
      </section>
      <section class="panel" aria-labelledby="ppT">
        <div class="ph"><h2 id="ppT">Duration by program</h2><p class="desc">Median time per program, timed runs only.</p></div>
        <div id="pfProg"></div>
        <p class="insight" id="insPfP"></p>
        <details class="alt"><summary>View as table</summary><div class="tablewrap" id="pfProgTable"></div></details>
      </section>
      <section class="panel" aria-labelledby="prT">
        <div class="ph"><h2 id="prT">Resource monitor</h2><p class="desc">Peak memory and CPU the program's monitor saw, against the limits in pipeline.conf.</p></div>
        <div id="pfRes"></div>
        <p class="insight" id="insPfS"></p>
        <details class="alt"><summary>View as table</summary><div class="tablewrap" id="pfResTable"></div></details>
      </section>
    </div>
  </div>

  <div id="breakdown" class="grid2" data-needs-data>
    <section class="panel" aria-labelledby="vdT">
      <div class="ph"><h2 id="vdT">Verdict distribution</h2><p class="desc">Every run's final verdict.</p></div>
      <ul class="legend" id="vdLegend"></ul>
      <div id="vdBars"></div>
      <p class="insight" id="insVd"></p>
      <details class="alt"><summary>View as table</summary><div class="tablewrap" id="vdTable"></div></details>
    </section>
    <section class="panel" aria-labelledby="whyT">
      <div class="ph"><h2 id="whyT">Why runs didn't pass</h2><p class="desc">Derived from each run's agent trace and result detail.</p></div>
      <div id="whyBars"></div>
      <p class="insight" id="insWhy"></p>
      <details class="alt"><summary>View as table</summary><div class="tablewrap" id="whyTable"></div></details>
    </section>
    <section class="panel" aria-labelledby="pgT">
      <div class="ph"><h2 id="pgT">Runs by program</h2><p class="desc">Verdicts per program, same scale across rows.</p></div>
      <ul class="legend" id="pgLegend"></ul>
      <div id="pgBars"></div>
      <p class="insight" id="insPg"></p>
      <details class="alt"><summary>View as table</summary><div class="tablewrap" id="pgTable"></div></details>
    </section>
    <section class="panel" aria-labelledby="fnT">
      <div class="ph"><h2 id="fnT">Stage funnel</h2><p class="desc">What happened to every run at each of the four stages.</p></div>
      <ul class="legend" id="fnLegend"></ul>
      <div id="fnBars"></div>
      <p class="insight" id="insFn"></p>
      <details class="alt"><summary>View as table</summary><div class="tablewrap" id="fnTable"></div></details>
    </section>
  </div>

  <div id="agents" class="grid2" data-needs-data>
    <section class="panel" aria-labelledby="agT">
      <div class="ph"><h2 id="agT">Agent activity</h2><p class="desc">Lines each agent wrote to <code>logs/agent_activity.log</code>.</p></div>
      <ul class="legend" id="agLegend"></ul>
      <div id="agBars"></div>
      <p class="insight" id="insAg"></p>
      <details class="alt"><summary>View as table</summary><div class="tablewrap" id="agTable"></div></details>
    </section>
    <section class="panel" aria-labelledby="rlT">
      <div class="ph"><h2 id="rlT">Resource limits</h2><p class="desc" id="rlDesc">Read from <code>pipeline.conf</code>.</p></div>
      <div class="limits" id="limits"></div>
      <p class="insight" id="insRl"></p>
      <details class="alt" id="buildNotes"><summary>Build notes</summary><div id="buildBody"></div></details>
    </section>
  </div>

  <section id="history" class="panel" data-needs-data aria-labelledby="hsT">
    <div class="ph"><h2 id="hsT">Run history</h2><p class="desc">Select a row to open the full drill-down.</p></div>
    <div class="controls">
      <div class="chips" id="chips" role="group" aria-label="Filter by verdict"></div>
      <label class="search"><svg class="i"><use href="#i-search"/></svg><span class="sr">Search runs</span><input id="q" type="search" placeholder="Search program, detail, time" autocomplete="off"></label>
      <button class="btn sm" id="csvBtn" type="button"><svg class="i"><use href="#i-down"/></svg>Export CSV</button>
      <span class="count" id="rowCount" role="status" aria-live="polite"></span>
    </div>
    <div class="tablewrap" id="tableWrap">
      <table id="runTable">
        <caption class="sr">Pipeline runs</caption>
        <thead><tr id="thead"></tr></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
    <p class="insight" id="insHs"></p>
  </section>

  <section id="run" class="panel" aria-labelledby="rnT">
    <div class="ph"><h2 id="rnT">Run</h2><p class="desc">Starts the real program from this folder and streams its output below. One run at a time, each capped at 60 s.</p></div>
    <div class="runbtns" id="runBtns"></div>
    <details id="customBox"><summary>Run your own C code</summary>
      <label class="field"><span>C program <small class="muted" id="codeCount"></small></span>
        <textarea class="code" id="code" spellcheck="false" autocomplete="off" placeholder="#include &lt;stdio.h&gt;&#10;int main(void) { puts(&quot;hello&quot;); return 0; }"></textarea></label>
      <label class="field"><span>Expected output <small class="muted">(optional, one line; blank = only check the exit code)</small></span>
        <input class="text" id="expected" maxlength="62" autocomplete="off" spellcheck="false"></label>
      <div class="runrow"><button class="btn" id="customRun" type="button"><svg class="i"><use href="#i-play"/></svg>Run custom code…</button></div>
    </details>
    <p class="msg" id="runMsg" role="status"></p>
    <div class="term" id="term">
      <div class="termhead">
        <span class="cmd" id="tCmd">no command run yet</span>
        <span class="meta"><span id="tStep"></span><span id="tElapsed"></span><span id="tBadge" class="tbadge">idle</span>
          <button class="btn sm" id="stopBtn" type="button" disabled><svg class="i"><use href="#i-stop"/></svg>Stop</button></span>
      </div>
      <pre class="termbody" id="tOut" role="log" aria-label="Program output" tabindex="0"><span class="idle">Output from the program appears here as it runs.</span></pre>
    </div>
    <p class="insight" id="insRun"></p>
  </section>
  <footer id="foot"></footer>
</main>

<div class="scrim" id="scrim" hidden></div>
<aside class="drawer" id="drawer" role="dialog" aria-modal="true" aria-labelledby="dTitle" hidden tabindex="-1">
  <div class="dhead">
    <div class="dnav">
      <button class="btn sm" id="dPrev" type="button" aria-label="Previous run"><svg class="i"><use href="#i-prev"/></svg></button>
      <button class="btn sm" id="dNext" type="button" aria-label="Next run"><svg class="i"><use href="#i-next"/></svg></button>
      <span class="muted" id="dPos"></span>
    </div>
    <button class="btn sm" id="dClose" type="button" aria-label="Close run details"><svg class="i"><use href="#i-close"/></svg><span>Esc</span></button>
  </div>
  <div class="dbody" id="dBody"></div>
</aside>

<dialog id="confirm" aria-labelledby="cfT">
  <form method="dialog">
    <h2 id="cfT">Compile and run this code on your machine?</h2>
    <p>The pipeline will compile this program with <code>gcc</code> and execute it with your user's permissions. The pipeline's static review only rejects <code>gets(</code>, <code>system(</code> and unbalanced braces; it is not a sandbox. The run is killed after the program's own timeout and, in any case, after 60 s.</p>
    <pre id="cfPre"></pre>
    <div class="row">
      <button class="btn" value="cancel" id="cfCancel">Cancel</button>
      <button class="btn primary" value="ok" id="cfOk">Compile &amp; run</button>
    </div>
  </form>
</dialog>
<div class="tip" id="tip" role="tooltip" hidden></div>

<script nonce="__NONCE__">
'use strict';
/* ---------- tiny DOM helpers (no innerHTML anywhere: all log/program text goes through textContent) ---------- */
const SVGNS = 'http://www.w3.org/2000/svg';
function h(tag, props, ...kids) {
  const e = document.createElement(tag);
  if (props) for (const [k, v] of Object.entries(props)) {
    if (v == null || v === false) continue;
    if (k === 'class') e.className = v;
    else if (k === 'text') e.textContent = v;
    else if (k === 'style') { for (const [sk, sv] of Object.entries(v)) { if (sk.startsWith('--')) e.style.setProperty(sk, sv); else e.style[sk] = sv; } }
    else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v === true ? '' : String(v));
  }
  for (const c of kids.flat(2)) if (c != null && c !== false) e.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return e;
}
function s(tag, props, ...kids) {
  const e = document.createElementNS(SVGNS, tag);
  if (props) for (const [k, v] of Object.entries(props)) { if (v != null && v !== false) e.setAttribute(k, String(v)); }
  for (const c of kids.flat(2)) if (c != null && c !== false) e.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return e;
}
const $ = (id) => document.getElementById(id);
function icon(id, cls) { const e = s('svg', { class: 'i' + (cls ? ' ' + cls : ''), 'aria-hidden': 'true', focusable: 'false' }); e.append(s('use', { href: '#' + id })); return e; }
function ic(state) { const sp = h('span', { class: 'ic ' + state }); sp.append(icon('i-' + (state === 'PASS' ? 'pass' : state))); return sp; }
const reduced = () => matchMedia('(prefers-reduced-motion: reduce)').matches;
const pad2 = (n) => String(n).padStart(2, '0');
function fmtDur(sec) {
  if (sec == null) return '—'; if (sec < 0.95) return '<1 s'; if (sec < 60) return sec.toFixed(1) + ' s';
  const r = Math.round(sec), d = Math.floor(r / 86400), hr = Math.floor((r % 86400) / 3600), m = Math.floor((r % 3600) / 60), s2 = r % 60;
  if (d) return d + ' d ' + hr + ' h'; if (hr) return hr + ' h ' + m + ' min'; return m + ' min ' + s2 + ' s';
}
function fmtClock(d) { return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds()); }
const plural = (n, one, many) => n + ' ' + (n === 1 ? one : (many || one + 's'));
const pct = (a, b) => b ? Math.round(100 * a / b) : 0;
function clear(node) { node.replaceChildren(); return node; }
function stripAnsi(t) { return t.replace(/\x1b\[[0-9;?]*[ -\/]*[@-~]/g, '').replace(/\x1b[()][A-Za-z0-9]/g, ''); }

/* ---------- constants ---------- */
const V = {
  PASS:        { label: 'Pass',        color: 'var(--pass)', text: '#06240a', shape: 'circle',   chip: 'Pass' },
  SKIPPED:     { label: 'Skipped',     color: 'var(--skip)', text: '#2b1f00', shape: 'diamond',  chip: 'Skipped' },
  FAIL:        { label: 'Fail',        color: 'var(--fail)', text: '#fff',    shape: 'square',   chip: 'Failed' },
  INTERRUPTED: { label: 'Interrupted', color: 'var(--int)',  text: '#fff',    shape: 'triangle', chip: 'Interrupted' },
};
const VORDER = ['PASS', 'FAIL', 'SKIPPED', 'INTERRUPTED'];
const STAGES = [
  { id: 'generate', name: 'Code Generator',  agent: 'Code Generator',    c: 'var(--a-gen)' },
  { id: 'review',   name: 'Code Reviewer',   agent: 'Code Review Agent', c: 'var(--a-rev)' },
  { id: 'execute',  name: 'Execution Agent', agent: 'Execution Agent',   c: 'var(--a-exe)' },
  { id: 'test',     name: 'Testing Agent',   agent: 'Testing Agent',     c: 'var(--a-tst)' },
];
const AGENT_COLOR = { 'Code Generator': 'var(--a-gen)', 'Code Review Agent': 'var(--a-rev)', 'Execution Agent': 'var(--a-exe)',
  'Testing Agent': 'var(--a-tst)', 'Main Controller': 'var(--a-ctl)', 'Resource Monitor': 'var(--a-mon)' };
const AGENT_STAGE = { 'Code Generator': 'generate', 'Code Review Agent': 'review', 'Execution Agent': 'execute', 'Testing Agent': 'test' };
const STATE_LABEL = { pass: 'Passed', fail: 'Failed', skip: 'Skipped', none: 'No data', waiting: 'Waiting', running: 'Running' };
const REASONS = [
  ['review', 'Blocked by review'], ['compile', 'Compile error'], ['timeout', 'Timeout'], ['mismatch', 'Output mismatch'],
  ['runtime', 'Runtime error'], ['interrupted', 'Interrupted'], ['other', 'Other'],
];
const ALWAYS_REASONS = new Set(['review', 'compile', 'timeout', 'mismatch']);
const UNSAFE = [/\b(gets|strcpy|strcat|sprintf|vsprintf|system|popen|mktemp|tmpnam|getwd|alloca)\s*\(/, /\bscanf\s*\(\s*"[^"]*%s/];

/* ---------- app state ---------- */
const S = { st: null, rev: '', filter: 'all', q: '', sort: { key: 'idx', dir: -1 }, openKey: null, detail: null, detailSeq: 0,
  visibleKeys: [], es: null, jobId: null, running: false, termStart: 0, termTimer: 0, lastOk: 0, polling: false, jobSeen: null };
const store = { get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }, set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} } };

/* ---------- theme ---------- */
const themes = ['auto', 'light', 'dark'];
function applyTheme(mode) {
  const root = document.documentElement;
  if (mode === 'light' || mode === 'dark') root.setAttribute('data-theme', mode); else root.removeAttribute('data-theme');
  $('themeLabel').textContent = 'Theme: ' + mode[0].toUpperCase() + mode.slice(1);
}
let themeMode = store.get('pd-theme'); if (!themes.includes(themeMode)) themeMode = 'auto';
applyTheme(themeMode);
$('themeBtn').addEventListener('click', () => { themeMode = themes[(themes.indexOf(themeMode) + 1) % 3]; store.set('pd-theme', themeMode); applyTheme(themeMode); });

/* ---------- tooltip ---------- */
const tip = $('tip');
function showTip(el, lines) {
  clear(tip);
  lines.forEach((l, i) => tip.append(i === 0 ? h('b', { text: l }) : h('span', { text: l })));
  tip.hidden = false;
  const r = el.getBoundingClientRect(), tr = tip.getBoundingClientRect();
  let x = r.left + r.width / 2 - tr.width / 2, y = r.top - tr.height - 8;
  if (y < 6) y = r.bottom + 8;
  x = Math.max(6, Math.min(x, innerWidth - tr.width - 6));
  tip.style.left = x + 'px'; tip.style.top = y + 'px';
}
function hideTip() { tip.hidden = true; }
function tipify(el, lines, label) {
  el.addEventListener('pointerenter', () => showTip(el, lines));
  el.addEventListener('pointerleave', hideTip);
  el.addEventListener('focus', () => showTip(el, lines));
  el.addEventListener('blur', hideTip);
  if (label) el.setAttribute('aria-label', label);
  return el;
}
addEventListener('scroll', hideTip, { passive: true, capture: true });

/* ---------- derived data ---------- */
const runs = () => (S.st ? S.st.runs : []);
function countBy(list, fn) { const m = {}; for (const r of list) { const k = fn(r); m[k] = (m[k] || 0) + 1; } return m; }
function verdictCounts(list) { const c = countBy(list, (r) => r.verdict); return { PASS: c.PASS || 0, FAIL: c.FAIL || 0, SKIPPED: c.SKIPPED || 0, INTERRUPTED: c.INTERRUPTED || 0 }; }
function stageCounts(list) {
  return STAGES.map((st) => { const c = { pass: 0, fail: 0, skip: 0, none: 0 }; for (const r of list) c[r.stages[st.id].state]++; return { stage: st, c }; });
}
function reasonLabel(k) { const f = REASONS.find((x) => x[0] === k); return f ? f[1] : k; }
function nameOrder(names, catalog) {
  const idx = new Map(catalog.map((c) => [c.name, c.n]));
  return [...names].sort((a, b) => (idx.has(a) ? idx.get(a) : 99) - (idx.has(b) ? idx.get(b) : 99) || a.localeCompare(b));
}
function median(a) { if (!a.length) return null; const b = [...a].sort((x, y) => x - y), m = b.length >> 1; return b.length % 2 ? b[m] : (b[m - 1] + b[m]) / 2; }

/* ---------- generic components ---------- */
function badge(verdict) {
  const v = V[verdict] || { label: verdict, color: 'var(--neutral)' };
  const b = h('span', { class: 'badge', style: { '--c': v.color } });
  b.append(ic(verdict === 'PASS' ? 'pass' : verdict === 'FAIL' ? 'fail' : verdict === 'SKIPPED' ? 'skip' : 'int'), v.label);
  return b;
}
function legendItem(label, color, count, shapeIcon) {
  const li = h('li');
  const sw = h('span', { class: 'sw', style: { background: color } }); li.append(sw, label + (count != null ? ' (' + count + ')' : ''));
  return li;
}
function renderLegend(ul, items) { clear(ul); items.forEach((i) => ul.append(legendItem(i.label, i.color, i.count))); }
/* a horizontal stacked bar row; segs: [{key,label,value,color,text}] */
function barRow(label, segs, scaleMax, valText, opts) {
  opts = opts || {};
  const total = segs.reduce((a, x) => a + x.value, 0);
  const row = h('div', { class: 'barrow' });
  const track = h('div', { class: 'track' + (opts.hero ? ' hero' : '') });
  const fill = h('div', { class: 'fill', style: { width: scaleMax ? (100 * total / scaleMax) + '%' : '0%' } });
  for (const sg of segs) {
    if (!sg.value) continue;
    const seg = h('div', { class: 'seg', tabindex: 0, role: opts.open ? 'button' : 'img', style: { flexGrow: String(sg.value), '--c': sg.color, '--t': sg.text || '#fff' } });
    seg.append(h('span', { class: 'sl', text: (opts.fmt || String)(sg.value) }));
    const p = pct(sg.value, opts.pctOf || total);
    tipify(seg, opts.tip ? opts.tip(sg) : [sg.label + ': ' + plural(sg.value, 'run'), (label ? label + ' · ' : '') + p + '%'],
      opts.aria ? opts.aria(sg) : (label ? label + ', ' : '') + sg.label + ': ' + plural(sg.value, 'run') + ' (' + p + '%)');
    if (opts.open) {
      seg.addEventListener('click', () => opts.open(seg));
      seg.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); opts.open(seg); } });
    }
    fill.append(seg);
  }
  track.append(fill);
  row.append(h('div', { class: 'barlab', text: label }), track, h('div', { class: 'barval', text: valText != null ? valText : String(total) }));
  return row;
}
function fitLabels(root) {
  root.querySelectorAll('.seg').forEach((seg) => {
    const lab = seg.firstElementChild; if (!lab) return;
    lab.classList.remove('off');
    if (lab.scrollWidth + 6 > seg.clientWidth) lab.classList.add('off');
  });
}
function fitAll() { document.querySelectorAll('#vdBars,#whyBars,#pgBars,#fnBars,#agBars,#pfRuns,#pfProg,#pfRes').forEach(fitLabels); }
function simpleTable(headers, rows) {
  return h('table', { style: { minWidth: '0' } }, h('thead', null, h('tr', null, headers.map((x) => h('th', { text: x })))),
    h('tbody', null, rows.map((r) => h('tr', null, r.map((c) => h('td', { text: String(c) }))))));
}

/* ---------- Performance ---------- */
function fmtSecs(x) { return x < 10 ? x.toFixed(1) + ' s' : Math.round(x) + ' s'; }
function gantt(box, rows, maxT) {
  rows.forEach((r) => {
    const left = Math.min(100, r.start / maxT * 100), w = Math.max(0.8, Math.min(100 - left, (r.end - r.start) / maxT * 100));
    const bar = h('div', { class: 'range', tabindex: 0, role: 'img', style: { left: left + '%', width: w + '%', '--c': r.color } });
    tipify(bar, r.tip, r.aria);
    box.append(h('div', { class: 'barrow' }, h('div', { class: 'barlab', text: r.label }), h('div', { class: 'track gt' }, bar), h('div', { class: 'barval', text: r.val })));
  });
  const ax = h('div', { class: 'axis', 'aria-hidden': 'true' });
  for (let i = 0; i <= 4; i++) ax.append(h('span', { style: { left: (i * 25) + '%' }, text: fmtSecs(maxT * i / 4) }));
  box.append(h('div', { class: 'barrow' }, h('div'), ax, h('div')));
}
function timingBlock(d) {
  const wrap = h('div');
  const tm = d.timing;
  if (!tm) { wrap.append(h('p', { class: 'muted', text: d.dur != null ? 'Only the total was captured for this run (' + fmtDur(d.dur) + '); no per-line timing.' : 'Not timed. The program’s logs have no timestamps, and this run happened while the dashboard was not watching.' })); return wrap; }
  const f = h('dl', { class: 'facts' });
  if (d.elapsed != null) f.append(h('dt', { text: 'Total' }), h('dd', { text: fmtDur(d.elapsed) + ' wall clock (process start to exit)' }));
  f.append(h('dt', { text: 'Log window' }), h('dd', { text: fmtDur(tm.span) + ' from first to last log line' }));
  if (tm.startup != null) f.append(h('dt', { text: 'Startup' }), h('dd', { text: fmtDur(tm.startup) + ' before the first log line' }));
  if (tm.shutdown != null) f.append(h('dt', { text: 'Wind-down' }), h('dd', { text: fmtDur(tm.shutdown) + ' after the last log line' }));
  wrap.append(f);
  const g = h('div', { style: { marginTop: '12px' } });
  const maxT = Math.max(0.5, tm.span);
  gantt(g, tm.spans.map((sp) => ({ label: sp.agent, color: AGENT_COLOR[sp.agent] || 'var(--neutral)', start: sp.start, end: sp.end,
    val: fmtSecs(sp.start) + '–' + fmtSecs(sp.end),
    tip: [sp.agent, 'first log line at +' + sp.start.toFixed(2) + ' s, last at +' + sp.end.toFixed(2) + ' s', plural(sp.lines, 'log line')],
    aria: sp.agent + ' logging from ' + sp.start.toFixed(2) + ' to ' + sp.end.toFixed(2) + ' seconds' })), maxT);
  wrap.append(g);
  return wrap;
}
const AGENTS_ORDER_UI = ['Code Generator', 'Code Review Agent', 'Execution Agent', 'Testing Agent', 'Main Controller', 'Resource Monitor'];
function renderPerformance() {
  const list = runs(), timed = list.filter((r) => r.dur != null), meta = S.st.meta;
  const wall = timed.filter((r) => r.dur_src === 'wall').length, byLog = timed.length - wall;
  const kp = clear($('pfKpis'));
  const tile = (label, value, sub) => h('div', { class: 'kpi' }, h('div', { class: 'lb', text: label }), h('div', { class: 'num', text: value }), h('div', { class: 'sm', text: sub }));
  if (!timed.length) {
    kp.append(tile('Median run time', '—', 'no timed runs yet'), tile('Fastest', '—', ''), tile('Slowest', '—', ''), tile('Runs timed', '0 of ' + list.length, 'start a run below to measure one'));
    $('insPfK').textContent = list.length ? 'None of the ' + plural(list.length, 'run') + ' on record has a duration, because they ran before this dashboard was watching. Run a program from the Run panel and it will be timed.' : 'No runs yet.';
  } else {
    const sorted = [...timed].sort((a, b) => a.dur - b.dur), fast = sorted[0], slow = sorted[sorted.length - 1], med = median(timed.map((r) => r.dur));
    kp.append(tile('Median run time', fmtDur(med), 'across ' + plural(timed.length, 'timed run')), tile('Fastest', fmtDur(fast.dur), fast.name), tile('Slowest', fmtDur(slow.dur), slow.name),
      tile('Runs timed', timed.length + ' of ' + list.length, wall + ' by wall clock' + (byLog ? ' · ' + byLog + ' from log arrival' : '')));
    const pass = timed.filter((r) => r.verdict === 'PASS').map((r) => r.dur), other = timed.filter((r) => r.verdict !== 'PASS').map((r) => r.dur);
    $('insPfK').textContent = 'A typical run takes ' + fmtDur(med) + (pass.length && other.length ? '; passing runs take a median ' + fmtDur(median(pass)) + ' while runs that did not pass take ' + fmtDur(median(other)) + '.' : '.') +
      (list.length > timed.length ? ' ' + plural(list.length - timed.length, 'older run') + (list.length - timed.length === 1 ? ' has' : ' have') + ' no timing.' : '');
  }
  const sl = meta.sleeps || [], note = $('pfSleep');
  note.hidden = !sl.length;
  if (sl.length) {
    const bySec = {}; sl.forEach((x) => { (bySec[x.sec] = bySec[x.sec] || []).push(x.line); });
    const parts = Object.keys(bySec).sort((a, b) => b - a).map((sec) => 'sleep(' + sec + ') at line' + (bySec[sec].length > 1 ? 's ' : ' ') + bySec[sec].join(', '));
    note.textContent = 'Why runs have a floor: ' + meta.source + ' pauses on purpose — ' + parts.join('; ') + '. Those waits, not computation, set the minimum time of a run; a run blocked at review still takes about the first pause.';
  }
  // 1. per-run duration bars
  const recent = [...timed].sort((a, b) => b.idx - a.idx).slice(0, 20), box = clear($('pfRuns'));
  const present = VORDER.filter((k) => recent.some((r) => r.verdict === k));
  renderLegend($('pdLegend'), present.map((k) => ({ label: V[k].label, color: V[k].color })));
  const mx = Math.max(1, ...recent.map((r) => r.dur));
  recent.forEach((r) => box.append(barRow('#' + (r.idx + 1) + ' ' + r.name, [{ key: r.verdict, label: V[r.verdict].label, value: r.dur, color: V[r.verdict].color, text: V[r.verdict].text }], mx, fmtDur(r.dur),
    { fmt: fmtDur, tip: () => ['#' + (r.idx + 1) + ' · ' + r.name, V[r.verdict].label + ' · ' + fmtDur(r.dur), r.ts], aria: () => 'Run ' + (r.idx + 1) + ', ' + r.name + ', ' + V[r.verdict].label + ', ' + fmtDur(r.dur) + '. Press Enter to open.', open: (el) => openRun(r.key, el) })));
  if (!recent.length) box.append(h('p', { class: 'pfnote', text: 'No timed runs yet.' }));
  $('insPfR').textContent = !recent.length ? 'Nothing to chart yet.' : 'The slowest of these ' + recent.length + ' was #' + (recent.reduce((a, b) => (b.dur > a.dur ? b : a)).idx + 1) + ' at ' + fmtDur(mx) + '; the fastest ' + fmtDur(Math.min(...recent.map((r) => r.dur))) + '.';
  clear($('pfRunsTable')).append(simpleTable(['#', 'Program', 'Verdict', 'Duration (s)', 'Measured as'], recent.map((r) => [r.idx + 1, r.name, V[r.verdict].label, r.dur, r.dur_src === 'wall' ? 'wall clock' : 'log arrival'])));

  // 2. where the time goes
  const withSp = timed.filter((r) => r.spans && r.spans.length), gbox = clear($('pfGantt'));
  const mode = $('pfMode').value, agentsSeen = AGENTS_ORDER_UI.filter((a) => withSp.some((r) => r.spans.some((s2) => s2.agent === a)));
  renderLegend($('gaLegend'), agentsSeen.map((a) => ({ label: a, color: AGENT_COLOR[a] })));
  let rows = [];
  if (withSp.length) {
    const passing = withSp.filter((r) => r.verdict === 'PASS');
    const src = mode === 'latest' ? [withSp.reduce((a, b) => (b.idx > a.idx ? b : a))] : (mode === 'pass' && passing.length ? passing : withSp);
    rows = agentsSeen.map((a) => {
      const sp = src.map((r) => r.spans.find((s2) => s2.agent === a)).filter(Boolean);
      const avg = (k) => sp.reduce((t2, x) => t2 + x[k], 0) / sp.length;
      return { agent: a, start: avg('start'), end: avg('end'), n: sp.length };
    });
    const maxT = Math.max(0.5, ...rows.map((r) => r.end));
    gantt(gbox, rows.map((r) => ({ label: r.agent, color: AGENT_COLOR[r.agent], start: r.start, end: r.end, val: fmtSecs(r.start) + '–' + fmtSecs(r.end),
      tip: [r.agent, (mode === 'latest' ? 'latest run' : (mode === 'pass' && passing.length ? 'average of ' + plural(r.n, 'passing run') : 'average of ' + plural(r.n, 'run'))), 'first log line at +' + r.start.toFixed(2) + ' s, last at +' + r.end.toFixed(2) + ' s'],
      aria: r.agent + ' logging from ' + r.start.toFixed(2) + ' to ' + r.end.toFixed(2) + ' seconds' })), maxT);
    const last = [...rows].sort((a, b) => b.end - a.end)[0], longest = [...rows].sort((a, b) => (b.end - b.start) - (a.end - a.start))[0];
    $('insPfG').textContent = last.agent + ' writes the last log line (about ' + fmtSecs(last.end) + ' in)' + (longest.agent !== last.agent ? ', and ' + longest.agent + ' has the longest window (' + fmtSecs(longest.end - longest.start) + ').' : ', and it also has the longest window.') + (mode !== 'latest' ? ' Based on ' + plural(src.length, mode === 'pass' && passing.length ? 'passing run' : 'timed run') + '.' : '');
  } else {
    gbox.append(h('p', { class: 'pfnote', text: 'No per-agent timing yet. It is captured for any run that happens while this dashboard is open.' }));
    $('insPfG').textContent = 'Nothing to chart yet.';
  }
  clear($('pfGanttTable')).append(simpleTable(['Agent', 'Starts (s)', 'Ends (s)'], rows.map((r) => [r.agent, r.start.toFixed(2), r.end.toFixed(2)])));

  // 3. by program
  const names = nameOrder(new Set(timed.map((r) => r.name)), meta.catalog), pbox = clear($('pfProg'));
  const per = names.map((nm) => { const d = timed.filter((r) => r.name === nm).map((r) => r.dur); return { nm, med: median(d), n: d.length }; });
  const pmx = Math.max(1, ...per.map((p) => p.med));
  per.forEach((p) => pbox.append(barRow(p.nm, [{ key: p.nm, label: p.nm, value: p.med, color: 'var(--a-ctl)', text: '#fff' }], pmx, fmtDur(p.med) + ' · n=' + p.n,
    { fmt: fmtDur, tip: () => [p.nm, 'median ' + fmtDur(p.med), plural(p.n, 'timed run')], aria: () => p.nm + ', median ' + fmtDur(p.med) + ' over ' + plural(p.n, 'timed run') })));
  if (!per.length) pbox.append(h('p', { class: 'pfnote', text: 'No timed runs yet.' }));
  if (per.length) { const s1 = [...per].sort((a, b) => a.med - b.med); $('insPfP').textContent = per.length === 1 ? per[0].nm + ' is the only timed program so far (median ' + fmtDur(per[0].med) + ').' : s1[s1.length - 1].nm + ' is the slowest (median ' + fmtDur(s1[s1.length - 1].med) + ') and ' + s1[0].nm + ' the fastest (' + fmtDur(s1[0].med) + ').'; } else $('insPfP').textContent = 'Nothing to chart yet.';
  clear($('pfProgTable')).append(simpleTable(['Program', 'Median (s)', 'Timed runs'], per.map((p) => [p.nm, p.med.toFixed(2), p.n])));

  // 4. resource monitor
  const withS = list.filter((r) => r.samples && r.samples.length), rbox = clear($('pfRes')), cfg = S.st.config;
  const memLim = cfg.values.MEMORY_LIMIT_KB != null ? cfg.values.MEMORY_LIMIT_KB : cfg.defaults.MEMORY_LIMIT_KB, cpuLim = cfg.values.CPU_LIMIT_TICKS != null ? cfg.values.CPU_LIMIT_TICKS : cfg.defaults.CPU_LIMIT_TICKS;
  const plat = { darwin: 'macOS', linux: 'Linux', win32: 'Windows' }[meta.platform] || meta.platform;
  const rrows = [];
  if (!withS.length) {
    rbox.append(h('p', { class: 'pfnote', text: 'No resource samples have been recorded. The program’s monitor reads /proc/<pid>, which only exists on Linux; this dashboard is running on ' + plat + (meta.platform === 'linux' ? ', so samples appear once a run lasts long enough for the monitor to catch it.' : ', so the monitor finds nothing to sample.') }));
    $('insPfS').textContent = 'Limits in force: ' + (memLim ? memLim.toLocaleString() + ' KB memory' : '') + (cpuLim ? ', ' + cpuLim.toLocaleString() + ' CPU ticks' : '') + '. No usage has been observed yet.';
  } else {
    [...withS].sort((a, b) => b.idx - a.idx).slice(0, 8).forEach((r) => {
      const pm = Math.max(...r.samples.map((x) => x.mem_kb)), pc = Math.max(...r.samples.map((x) => x.cpu));
      rrows.push([r.idx + 1, r.name, pm, pc]);
      const mk = (what, val, lim, unit) => barRow('#' + (r.idx + 1) + ' ' + r.name + ' · ' + what, [{ key: what, label: what, value: val, color: val > lim ? 'var(--fail)' : 'var(--a-mon)', text: '#fff' }], Math.max(lim, val), val.toLocaleString() + ' / ' + lim.toLocaleString() + ' ' + unit,
        { fmt: (v) => v.toLocaleString(), tip: () => [r.name + ' · peak ' + what, val.toLocaleString() + ' ' + unit + ' of ' + lim.toLocaleString() + ' allowed'], aria: () => what + ' peak ' + val + ' of ' + lim + ' ' + unit });
      rbox.append(mk('memory', pm, memLim, 'KB'), mk('CPU', pc, cpuLim, 'ticks'));
    });
    const over = rrows.filter((x) => x[2] > memLim || x[3] > cpuLim).length;
    $('insPfS').textContent = over ? plural(over, 'run') + ' crossed a limit.' : 'All ' + plural(rrows.length, 'sampled run') + ' stayed within the memory and CPU limits.';
  }
  clear($('pfResTable')).append(simpleTable(['#', 'Program', 'Peak memory (KB)', 'Peak CPU (ticks)'], rrows));
  requestAnimationFrame(fitAll);
}
$('pfMode').addEventListener('change', () => { if (S.st) renderPerformance(); });

/* ---------- Overview ---------- */
function stageFromInflight(inf) {
  const out = {}; STAGES.forEach((st) => { out[st.id] = { state: 'waiting', note: 'Waiting' }; });
  const seen = new Set();
  for (const t of inf.trace) {
    const sid = AGENT_STAGE[t.agent]; if (sid) seen.add(sid);
    if (t.msg.startsWith('Code Generator finished')) out.generate = { state: 'pass', note: 'Source written' };
    if (t.msg.startsWith('Static review passed')) out.review = { state: 'pass', note: 'Static review passed' };
    if (t.msg.startsWith('Static review failed')) out.review = { state: 'fail', note: 'Static review failed' };
    if (t.msg.startsWith('Program execution successful')) out.execute = { state: 'pass', note: 'Ran to completion' };
    if (t.msg.startsWith('Skipped: code review failed')) out.execute = { state: 'skip', note: 'Blocked by review' };
    if (/Compilation failed|timed out|Program execution failed/.test(t.msg)) out.execute = { state: 'fail', note: t.msg };
    const m = /TEST RESULT: (PASS|FAIL|SKIPPED)/.exec(t.msg);
    if (m) out.test = { state: m[1] === 'PASS' ? 'pass' : m[1] === 'FAIL' ? 'fail' : 'skip', note: 'Test ' + m[1].toLowerCase() };
  }
  STAGES.forEach((st) => { if (out[st.id].state === 'waiting' && seen.has(st.id)) out[st.id] = { state: 'running', note: 'Running…' }; });
  return out;
}
function renderStrip() {
  const st = S.st, list = runs(), last = list[list.length - 1];
  const strip = clear($('strip'));
  const live = st.inflight && st.job && !st.job.done;
  const stg = live ? stageFromInflight(st.inflight) : last ? last.stages : null;
  STAGES.forEach((def, i) => {
    const o = stg ? stg[def.id] : { state: 'none', note: 'No run yet' };
    const card = h('div', { class: 'stage' + (o.state === 'running' ? ' running' : ''), role: 'listitem', style: { '--c': def.c } });
    card.append(h('div', { class: 'nm' }, h('span', { class: 'sw' }), def.name));
    const stline = h('div', { class: 'st' }); stline.append(ic(o.state === 'waiting' ? 'waiting' : o.state), h('span', { text: STATE_LABEL[o.state] || o.state }));
    card.append(stline, h('div', { class: 'note', text: o.note || '' }));
    strip.append(card);
    if (i < 3) { const a = h('div', { class: 'arrow', 'aria-hidden': 'true' }); a.append(icon('i-arrow')); strip.append(a); }
  });
  let ins;
  if (live) ins = 'A run is in progress; the latest agent line is “' + st.inflight.last_msg + '” from ' + st.inflight.last_agent + '.';
  else if (!last) ins = 'No run has been recorded yet.';
  else {
    const firstBlock = STAGES.find((d) => last.stages[d.id].state === 'fail');
    if (last.verdict === 'PASS') ins = 'Latest run (' + last.name + ', ' + last.ts + ') cleared all four stages.';
    else if (firstBlock) ins = 'Latest run (' + last.name + ', ' + last.ts + ') stopped at ' + firstBlock.name + ': ' + last.stages[firstBlock.id].note + '.';
    else ins = 'Latest run (' + last.name + ', ' + last.ts + ') ended ' + V[last.verdict].label.toLowerCase() + ': ' + last.detail;
  }
  $('insStrip').textContent = ins;
}
function sparkline(list) {
  const W = 160, H = 34, pts = []; let pass = 0;
  list.forEach((r, i) => { if (r.verdict === 'PASS') pass++; pts.push({ n: i + 1, rate: pass / (i + 1), r }); });
  const svg = s('svg', { class: 'spark', viewBox: '0 0 ' + W + ' ' + H, preserveAspectRatio: 'none', role: 'img', tabindex: 0,
    'aria-label': 'Cumulative pass rate after each run. Use left and right arrow keys to step through runs.' });
  svg.append(s('line', { x1: 0, x2: W, y1: H - 1, y2: H - 1, stroke: 'var(--line)', 'stroke-width': 1 }));
  svg.append(s('line', { x1: 0, x2: W, y1: 1, y2: 1, stroke: 'var(--line)', 'stroke-width': 1 }));
  if (!pts.length) return svg;
  const X = (i) => pts.length === 1 ? W / 2 : 4 + (W - 8) * i / (pts.length - 1), Y = (p) => H - 3 - (H - 6) * p.rate;
  svg.append(s('polyline', { points: pts.map((p, i) => X(i).toFixed(1) + ',' + Y(p).toFixed(1)).join(' '), fill: 'none', stroke: 'var(--pass)', 'stroke-width': 2, 'stroke-linejoin': 'round', 'stroke-linecap': 'round', 'vector-effect': 'non-scaling-stroke' }));
  const dot = s('circle', { r: 4, cx: X(pts.length - 1), cy: Y(pts[pts.length - 1]), fill: 'var(--pass)', stroke: 'var(--surface)', 'stroke-width': 1.5 });
  svg.append(dot);
  let cur = pts.length - 1;
  const show = (i) => {
    cur = Math.max(0, Math.min(pts.length - 1, i)); const p = pts[cur];
    dot.setAttribute('cx', X(cur)); dot.setAttribute('cy', Y(p));
    showTip(svg, ['After run ' + p.n + ': ' + Math.round(p.rate * 100) + '% pass', p.r.name + ' · ' + V[p.r.verdict].label]);
  };
  svg.addEventListener('pointermove', (e) => { const b = svg.getBoundingClientRect(); show(Math.round((e.clientX - b.left) / b.width * (pts.length - 1))); });
  svg.addEventListener('pointerleave', () => { cur = pts.length - 1; dot.setAttribute('cx', X(cur)); dot.setAttribute('cy', Y(pts[cur])); hideTip(); });
  svg.addEventListener('focus', () => show(cur));
  svg.addEventListener('blur', () => { cur = pts.length - 1; dot.setAttribute('cx', X(cur)); dot.setAttribute('cy', Y(pts[cur])); hideTip(); });
  svg.addEventListener('keydown', (e) => { if (e.key === 'ArrowLeft') { e.preventDefault(); show(cur - 1); } else if (e.key === 'ArrowRight') { e.preventDefault(); show(cur + 1); } });
  return svg;
}
function renderKpis() {
  const list = runs(), c = verdictCounts(list), n = list.length;
  const box = clear($('kpis'));
  const tile = (label, ico, value, sub, extra) => {
    const t = h('div', { class: 'kpi' });
    const lb = h('div', { class: 'lb' }); if (ico) lb.append(ic(ico)); lb.append(label);
    t.append(lb, h('div', { class: 'num', text: value }), h('div', { class: 'sm', text: sub }));
    if (extra) t.append(extra);
    return t;
  };
  box.append(
    tile('Total runs', null, n ? String(n) : '—', n ? (c.INTERRUPTED ? plural(c.INTERRUPTED, 'interrupted run') + ' included' : 'since ' + list[0].ts) : 'nothing recorded'),
    tile('Pass rate', 'pass', n ? pct(c.PASS, n) + '%' : '—', n ? c.PASS + ' of ' + n + ' passed' : 'no runs', sparkline(list)),
    tile('Failed', 'fail', n ? String(c.FAIL) : '—', n ? pct(c.FAIL, n) + '% · ran, wrong or non-zero result' : 'no runs'),
    tile('Skipped', 'skip', n ? String(c.SKIPPED) : '—', n ? pct(c.SKIPPED, n) + '% · stopped before the test' : 'no runs'));
  let ins = 'No runs yet.';
  if (n) {
    const why = countBy(list.filter((r) => r.reason), (r) => r.reason);
    const top = Object.entries(why).sort((a, b) => b[1] - a[1])[0];
    ins = 'Pass rate is ' + pct(c.PASS, n) + '% over ' + plural(n, 'run') + '; ' + (top ? 'the most common reason for not passing is “' + reasonLabel(top[0]).toLowerCase() + '” (' + top[1] + ').' : 'every run passed.');
  }
  $('insKpi').textContent = ins;
}

/* ---------- Timeline ---------- */
function niceTicks(t0, t1, maxTicks) {
  const steps = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400, 172800, 604800];
  let step = steps.find((x) => (t1 - t0) / x <= maxTicks) || 604800;
  const off = -new Date(t0 * 1000).getTimezoneOffset() * 60, out = [];
  for (let t = Math.ceil((t0 + off) / step) * step - off; t <= t1; t += step) out.push(t);
  return { ticks: out, step };
}
function fmtTick(t, step) {
  const d = new Date(t * 1000);
  if (step >= 86400) return pad2(d.getMonth() + 1) + '-' + pad2(d.getDate());
  if (step >= 3600 * 6) return pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) + ' ' + pad2(d.getHours()) + ':00';
  if (step >= 60) return pad2(d.getHours()) + ':' + pad2(d.getMinutes());
  return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
}
function markShape(v, cls) {
  const col = (V[v] || V.INTERRUPTED).color, a = { class: 'shape ' + (cls || ''), fill: col };
  switch ((V[v] || V.INTERRUPTED).shape) {
    case 'circle': return s('circle', Object.assign({ r: 6 }, a));
    case 'square': return s('rect', Object.assign({ x: -5.5, y: -5.5, width: 11, height: 11, rx: 1.5 }, a));
    case 'diamond': return s('rect', Object.assign({ x: -5, y: -5, width: 10, height: 10, rx: 1, transform: 'rotate(45)' }, a));
    default: return s('path', Object.assign({ d: 'M0 -7 L7 5.5 L-7 5.5 Z', 'stroke-linejoin': 'round' }, a));
  }
}
function renderTimeline() {
  const list = runs().filter((r) => r.epoch != null).sort((a, b) => a.epoch - b.epoch || a.idx - b.idx);
  const c = verdictCounts(runs());
  renderLegend($('tlLegend'), VORDER.filter((k) => c[k] || k !== 'INTERRUPTED').map((k) => ({ label: V[k].label, color: V[k].color, count: c[k] })));
  const legend = $('tlLegend');
  [...legend.children].forEach((li, i) => { const k = VORDER.filter((q) => c[q] || q !== 'INTERRUPTED')[i]; const sw = li.firstChild; const sv = s('svg', { width: 14, height: 14, viewBox: '-8 -8 16 16', 'aria-hidden': 'true' }); sv.append(markShape(k)); li.replaceChild(sv, sw); });
  const svg = clear($('tlSvg')), scroll = $('tlScroll'), keepLeft = scroll.scrollLeft;
  if (!list.length) { svg.setAttribute('width', 300); svg.setAttribute('height', 40); svg.append(s('text', { x: 4, y: 24 }, 'No timestamped runs yet.')); $('insTl').textContent = 'No runs yet.'; clear($('tlTable')); return; }
  let t0 = list[0].epoch, t1 = list[list.length - 1].epoch;
  if (t1 - t0 < 60) { const mid = (t0 + t1) / 2; t0 = mid - 30; t1 = mid + 30; }
  const sp = t1 - t0; t0 -= sp * .03; t1 += sp * .03;
  const base = Math.max(280, scroll.clientWidth || 600), padL = 18, padR = 18, LANE = 26;
  let best = null;
  for (const mul of [1, 1.5, 2, 3, 4]) {
    const W = base * mul, inner = W - padL - padR, lastX = [], placed = [];
    for (const r of list) {
      const x = padL + (r.epoch - t0) / (t1 - t0) * inner; let lane = 0;
      while (lane < lastX.length && x - lastX[lane] < 24) lane++;
      lastX[lane] = x; placed.push({ r, x, lane });
    }
    best = { W, placed, lanes: lastX.length }; if (lastX.length <= 8) break;
  }
  const top = 14, axisH = 26, H = top + best.lanes * LANE + axisH;
  svg.setAttribute('width', best.W); svg.setAttribute('height', H); svg.setAttribute('viewBox', '0 0 ' + best.W + ' ' + H);
  const { ticks, step } = niceTicks(t0, t1, Math.max(3, Math.floor(best.W / 110)));
  const X = (t) => padL + (t - t0) / (t1 - t0) * (best.W - padL - padR);
  for (const t of ticks) {
    svg.append(s('line', { class: 'grid', x1: X(t), x2: X(t), y1: top - 6, y2: H - axisH }));
    svg.append(s('text', { x: X(t), y: H - 8, 'text-anchor': 'middle' }, fmtTick(t, step)));
  }
  svg.append(s('line', { class: 'axis', x1: padL - 6, x2: best.W - padR + 6, y1: H - axisH, y2: H - axisH }));
  for (const p of best.placed) {
    const y = top + (best.lanes - 1 - p.lane) * LANE + LANE / 2 - 2, r = p.r;
    const g = s('g', { class: 'mark' + (S.openKey === r.key ? ' sel' : ''), transform: 'translate(' + p.x.toFixed(1) + ' ' + y + ')', tabindex: 0, role: 'button', 'data-fid': 'm' + r.key });
    g.append(s('circle', { class: 'hit', r: 12 }), s('circle', { class: 'ring', r: 11 }), markShape(r.verdict));
    tipify(g, ['#' + (r.idx + 1) + ' · ' + r.name, V[r.verdict].label + ' · ' + r.ts, r.detail.length > 90 ? r.detail.slice(0, 88) + '…' : r.detail],
      'Run ' + (r.idx + 1) + ': ' + r.name + ', ' + V[r.verdict].label + ', ' + r.ts + '. Press Enter to open.');
    g.addEventListener('click', () => openRun(r.key, g));
    g.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openRun(r.key, g); } });
    svg.append(g);
  }
  scroll.scrollLeft = keepLeft;
  const spanSec = list[list.length - 1].epoch - list[0].epoch;
  const lastR = list[list.length - 1];
  $('insTl').textContent = list.length === 1 ? 'One run recorded so far (' + lastR.ts + ').' :
    plural(list.length, 'run') + ' over ' + (spanSec < 1 ? 'under a second' : fmtDur(spanSec)) + '; the latest run: ' + V[lastR.verdict].label.toLowerCase() + '.';
  clear($('tlTable')).append(simpleTable(['#', 'Time', 'Program', 'Verdict'], list.map((r) => [r.idx + 1, r.ts, r.name, V[r.verdict].label])));
}

/* ---------- Breakdown ---------- */
function renderBreakdown() {
  const list = runs(), n = list.length, cat = S.st.meta.catalog;
  const vcount = verdictCounts(list);
  const vsegs = VORDER.map((k) => ({ key: k, label: V[k].label, value: vcount[k], color: V[k].color, text: V[k].text }));
  renderLegend($('vdLegend'), vsegs.filter((x) => x.value || x.key !== 'INTERRUPTED').map((x) => ({ label: x.label, color: x.color, count: x.value })));
  clear($('vdBars')).append(barRow('All ' + plural(n, 'run'), vsegs, n, String(n), { hero: true }));
  const topV = vsegs.filter((x) => x.value).sort((a, b) => b.value - a.value)[0];
  $('insVd').textContent = topV ? V[topV.key].label + ' is the most common verdict: ' + topV.value + ' of ' + n + ' runs (' + pct(topV.value, n) + '%).' : 'No runs yet.';
  clear($('vdTable')).append(simpleTable(['Verdict', 'Runs', 'Share'], vsegs.map((x) => [x.label, x.value, pct(x.value, n) + '%'])));

  // why not pass
  const bad = list.filter((r) => r.reason), why = countBy(bad, (r) => r.reason);
  const rs = REASONS.filter(([k]) => ALWAYS_REASONS.has(k) || why[k]);
  const mx = Math.max(1, ...rs.map(([k]) => why[k] || 0));
  const whyBox = clear($('whyBars'));
  rs.forEach(([k, lab]) => whyBox.append(barRow(lab, [{ key: k, label: lab, value: why[k] || 0, color: 'var(--neutral)' }], mx, String(why[k] || 0), { pctOf: bad.length })));
  const topW = rs.map(([k, l]) => [k, l, why[k] || 0]).sort((a, b) => b[2] - a[2])[0];
  $('insWhy').textContent = !n ? 'No runs yet.' : !bad.length ? 'Every run passed, so there is nothing to explain.' :
    '“' + topW[1] + '” accounts for ' + topW[2] + ' of ' + plural(bad.length, 'run') + ' that did not pass (' + pct(topW[2], bad.length) + '%).';
  clear($('whyTable')).append(simpleTable(['Reason', 'Runs'], rs.map(([k, l]) => [l, why[k] || 0])));

  // by program
  const names = nameOrder(new Set(list.map((r) => r.name)), cat);
  renderLegend($('pgLegend'), vsegs.filter((x) => x.value || x.key !== 'INTERRUPTED').map((x) => ({ label: x.label, color: x.color })));
  const perProg = names.map((nm) => ({ nm, c: verdictCounts(list.filter((r) => r.name === nm)) }));
  const pmax = Math.max(1, ...perProg.map((p) => Object.values(p.c).reduce((a, b) => a + b, 0)));
  const pgBox = clear($('pgBars'));
  perProg.forEach((p) => pgBox.append(barRow(p.nm, VORDER.map((k) => ({ key: k, label: V[k].label, value: p.c[k], color: V[k].color, text: V[k].text })), pmax)));
  const rated = perProg.map((p) => ({ nm: p.nm, tot: p.c.PASS + p.c.FAIL + p.c.SKIPPED + p.c.INTERRUPTED, pass: p.c.PASS }));
  if (!rated.length) $('insPg').textContent = 'No runs yet.';
  else {
    const w = [...rated].sort((a, b) => a.pass / a.tot - b.pass / b.tot || b.tot - a.tot)[0];
    $('insPg').textContent = w.pass === w.tot ? 'Every program that has run passed each time.' : '“' + w.nm + '” is the weakest: ' + w.pass + ' of ' + plural(w.tot, 'run') + ' passed.';
  }
  clear($('pgTable')).append(simpleTable(['Program', ...VORDER.map((k) => V[k].label)], perProg.map((p) => [p.nm, ...VORDER.map((k) => p.c[k])])));

  // funnel
  const sc = stageCounts(list);
  const fsegs = (c) => [{ key: 'pass', label: 'Passed', value: c.pass, color: 'var(--pass)', text: V.PASS.text }, { key: 'fail', label: 'Failed', value: c.fail, color: 'var(--fail)', text: '#fff' },
    { key: 'skip', label: 'Skipped', value: c.skip, color: 'var(--skip)', text: V.SKIPPED.text }, { key: 'none', label: 'Not reached / no trace', value: c.none, color: 'var(--none)', text: '#fff' }];
  const tot = { pass: 0, fail: 0, skip: 0, none: 0 }; sc.forEach((x) => { for (const k in tot) tot[k] += x.c[k]; });
  renderLegend($('fnLegend'), fsegs(tot).map((x) => ({ label: x.label, color: x.color })));
  const fnBox = clear($('fnBars'));
  sc.forEach((x) => fnBox.append(barRow(x.stage.name, fsegs(x.c), n, String(n))));
  const drops = sc.map((x) => ({ name: x.stage.name, lost: x.c.fail + x.c.skip })).sort((a, b) => b.lost - a.lost)[0];
  $('insFn').textContent = !n ? 'No runs yet.' : drops.lost === 0 ? 'Every run cleared all four stages.' :
    'The biggest loss is at ' + drops.name + ': ' + plural(drops.lost, 'run') + ' (' + pct(drops.lost, n) + '%) did not clear it (failed or skipped).';
  clear($('fnTable')).append(simpleTable(['Stage', 'Passed', 'Failed', 'Skipped', 'Not reached / no trace'], sc.map((x) => [x.stage.name, x.c.pass, x.c.fail, x.c.skip, x.c.none])));
  requestAnimationFrame(fitAll);
}

/* ---------- Agents + limits ---------- */
function renderAgents() {
  const ag = S.st.agents.filter((a) => a.count > 0 || AGENT_COLOR[a.name]), total = S.st.agent_lines, mx = Math.max(1, ...ag.map((a) => a.count));
  renderLegend($('agLegend'), ag.map((a) => ({ label: a.name, color: AGENT_COLOR[a.name] || 'var(--neutral)', count: a.count })));
  const box = clear($('agBars'));
  ag.forEach((a) => box.append(barRow(a.name, [{ key: a.name, label: a.name, value: a.count, color: AGENT_COLOR[a.name] || 'var(--neutral)', text: '#fff' }], mx, String(a.count), { pctOf: total })));
  const top = [...ag].sort((a, b) => b.count - a.count)[0];
  $('insAg').textContent = !total ? 'The activity log is empty.' : top.name + ' writes the most: ' + plural(top.count, 'line') + ' of ' + total + ' (' + pct(top.count, total) + '%).';
  clear($('agTable')).append(simpleTable(['Agent', 'Log lines', 'Share'], ag.map((a) => [a.name, a.count, pct(a.count, total) + '%'])));
  requestAnimationFrame(fitAll);
}
function renderLimits() {
  const cfg = S.st.config, def = cfg.defaults, v = cfg.values;
  const items = [['MEMORY_LIMIT_KB', 'Memory limit', 'KB'], ['CPU_LIMIT_TICKS', 'CPU limit', 'ticks'], ['PROGRAM_TIMEOUT_SEC', 'Program timeout', 'sec']];
  const box = clear($('limits'));
  items.forEach(([k, label, unit]) => {
    const fromConf = v[k] != null, val = fromConf ? v[k] : def[k];
    const it = h('div', { class: 'limit' }, h('div', { class: 'lb', text: label }),
      h('div', { class: 'v' }, val != null ? val.toLocaleString() : '—', h('span', { class: 'u', text: unit })),
      h('div', { class: 'sm muted', style: { fontSize: '12.5px' }, text: fromConf ? 'from pipeline.conf' : 'program default' }));
    box.append(it);
  });
  $('rlDesc').textContent = cfg.present ? 'Read from pipeline.conf (changed ' + new Date(cfg.mtime * 1000).toLocaleString() + ').' : 'pipeline.conf does not exist yet; showing the defaults compiled into the program.';
  const to = v.PROGRAM_TIMEOUT_SEC != null ? v.PROGRAM_TIMEOUT_SEC : def.PROGRAM_TIMEOUT_SEC, mem = v.MEMORY_LIMIT_KB != null ? v.MEMORY_LIMIT_KB : def.MEMORY_LIMIT_KB;
  $('insRl').textContent = 'A program is killed after ' + to + ' s' + (mem ? '; the monitor flags memory above ' + Math.round(mem / 1024) + ' MB' : '') + '. Edit pipeline.conf and the next run picks it up.';
  const b = S.st.meta.build, bb = clear($('buildBody'));
  const nwarn = ((b.warnings || '').match(/warning:/g) || []).length;
  $('buildNotes').querySelector('summary').textContent = 'Build notes (' + (nwarn ? plural(nwarn, 'compiler warning') : 'no compiler warnings') + ')';
  bb.append(h('p', { class: 'muted', style: { fontSize: '13px', marginTop: '8px' }, text: 'gcc ' + S.st.meta.flags + ' ' + S.st.meta.source + ' → ' + S.st.meta.binary + ' (' + b.status + ')' }));
  if (b.warnings) bb.append(h('pre', { class: 'raw', text: b.warnings }));
}

/* ---------- History table ---------- */
const COLS = [
  { key: 'idx', label: '#', get: (r) => r.idx }, { key: 'epoch', label: 'Time', get: (r) => r.epoch || 0 },
  { key: 'name', label: 'Program', get: (r) => r.name.toLowerCase() }, { key: 'verdict', label: 'Verdict', get: (r) => VORDER.indexOf(r.verdict) },
  { key: 'detail', label: 'Detail', get: (r) => r.detail.toLowerCase() }, { key: 'stages', label: 'Stages', get: (r) => STAGES.filter((d) => r.stages[d.id].state === 'pass').length },
  { key: 'elapsed', label: 'Duration', get: (r) => r.dur == null ? -1 : r.dur },
];
function visibleRuns() {
  const q = S.q.trim().toLowerCase();
  let list = runs().filter((r) => (S.filter === 'all' || r.verdict === S.filter) &&
    (!q || (r.ts + ' ' + r.name + ' ' + r.verdict + ' ' + r.detail + ' ' + (r.reason ? reasonLabel(r.reason) : '')).toLowerCase().includes(q)));
  const col = COLS.find((c) => c.key === S.sort.key) || COLS[0], d = S.sort.dir;
  return list.sort((a, b) => { const x = col.get(a), y = col.get(b); return (x < y ? -1 : x > y ? 1 : 0) * d || (b.idx - a.idx); });
}
function stageDots(r) {
  const w = h('span', { class: 'mini' });
  STAGES.forEach((d) => { const o = r.stages[d.id]; const e = ic(o.state === 'none' ? 'none' : o.state); e.setAttribute('title', d.name + ': ' + (STATE_LABEL[o.state] || o.state)); e.append(h('span', { class: 'sr', text: d.name + ' ' + (STATE_LABEL[o.state] || o.state) + '. ' })); w.append(e); });
  return w;
}
function renderChips() {
  const list = runs(), c = verdictCounts(list), box = clear($('chips'));
  const defs = [['all', 'All', list.length, null], ['PASS', 'Pass', c.PASS, 'pass'], ['FAIL', 'Failed', c.FAIL, 'fail'], ['SKIPPED', 'Skipped', c.SKIPPED, 'skip']];
  if (c.INTERRUPTED) defs.push(['INTERRUPTED', 'Interrupted', c.INTERRUPTED, 'int']);
  if (!defs.some((d) => d[0] === S.filter)) S.filter = 'all';
  defs.forEach(([k, lab, n, i]) => {
    const b = h('button', { class: 'chip', type: 'button', 'aria-pressed': String(S.filter === k), 'data-fid': 'chip' + k });
    if (i) b.append(ic(i)); b.append(lab, h('span', { class: 'n', text: String(n) }));
    b.addEventListener('click', () => { S.filter = k; renderChips(); renderTable(); });
    box.append(b);
  });
}
function renderHead() {
  const tr = clear($('thead'));
  COLS.forEach((c) => {
    const active = S.sort.key === c.key;
    const th = h('th', { scope: 'col', 'aria-sort': active ? (S.sort.dir > 0 ? 'ascending' : 'descending') : 'none' });
    const b = h('button', { type: 'button', 'data-fid': 'th' + c.key }, c.label, icon(active ? (S.sort.dir > 0 ? 'i-sorta' : 'i-sortd') : 'i-sortn'));
    b.addEventListener('click', () => { S.sort = { key: c.key, dir: active ? -S.sort.dir : (c.key === 'name' || c.key === 'detail' ? 1 : -1) }; renderHead(); renderTable(); });
    th.append(b); tr.append(th);
  });
}
function renderTable() {
  const list = visibleRuns(), all = runs();
  S.visibleKeys = list.map((r) => r.key);
  const body = $('tbody'), fid = document.activeElement && document.activeElement.getAttribute && document.activeElement.getAttribute('data-fid');
  clear(body);
  if (!list.length) body.append(h('tr', { class: 'empty-row' }, h('td', { colspan: COLS.length, text: all.length ? 'No runs match this filter.' : 'No runs yet.' })));
  for (const r of list) {
    const tr = h('tr', { 'data-key': r.key });
    const open = (e) => { if (e.target.closest('button.lnk')) return; openRun(r.key, tr.querySelector('button.lnk')); };
    tr.addEventListener('click', open);
    const lnk = h('button', { class: 'lnk', type: 'button', 'data-fid': 'r' + r.key, text: r.name, 'aria-label': 'Open run ' + (r.idx + 1) + ': ' + r.name + ', ' + V[r.verdict].label });
    lnk.addEventListener('click', () => openRun(r.key, lnk));
    tr.append(h('td', { text: String(r.idx + 1) }), h('td', { text: r.ts }), h('td', null, lnk), h('td', null, badge(r.verdict)),
      h('td', { class: 'det', text: r.detail }), h('td', null, stageDots(r)),
      h('td', { text: r.dur == null ? '—' : fmtDur(r.dur), title: r.dur == null ? 'Not timed: the program\u2019s logs have no timestamps, and this run happened while the dashboard was not watching.' : (r.dur_src === 'wall' ? 'Wall-clock time, measured by this dashboard' : 'First to last log line, measured by this dashboard') }));
    body.append(tr);
  }
  $('rowCount').textContent = 'Showing ' + list.length + ' of ' + plural(all.length, 'run');
  if (fid) { const el = document.querySelector('[data-fid="' + CSS.escape(fid) + '"]'); if (el && el !== document.activeElement) el.focus({ preventScroll: true }); }
  const pass = list.filter((r) => r.verdict === 'PASS').length;
  $('insHs').textContent = !all.length ? 'No runs yet.' : list.length ? plural(list.length, 'run') + ' shown, ' + pass + ' of them passed' + (S.q ? ' for “' + S.q + '”' : '') + '; newest run is #' + all.length + ' at ' + all[all.length - 1].ts + '.' : 'Nothing matches the current filter and search.';
}
function csvCell(v) { let t = String(v == null ? '' : v); if (/^[=+\-@\t\r]/.test(t)) t = "'" + t; return /[",\n\r]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t; }
function exportCsv() {
  const rows = visibleRuns();
  const head = ['run', 'timestamp', 'program', 'verdict', 'detail', 'generate', 'review', 'execute', 'test', 'not_passed_reason', 'log_lines', 'duration_seconds', 'duration_source'];
  const lines = [head.join(',')];
  for (const r of rows) lines.push([r.idx + 1, r.ts, r.name, r.verdict, r.detail, ...STAGES.map((d) => r.stages[d.id].state), r.reason ? reasonLabel(r.reason) : '', r.log_lines == null ? '' : r.log_lines, r.dur == null ? '' : r.dur, r.dur_src || ''].map(csvCell).join(','));
  const blob = new Blob([lines.join('\r\n') + '\r\n'], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob), d = new Date();
  const a = h('a', { href: url, download: 'pipeline-runs-' + d.getFullYear() + pad2(d.getMonth() + 1) + pad2(d.getDate()) + '-' + pad2(d.getHours()) + pad2(d.getMinutes()) + pad2(d.getSeconds()) + '.csv', style: { display: 'none' } });
  document.body.append(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 4000);
}

/* ---------- Drawer ---------- */
const D = { lastFocus: null, replay: null, closing: 0 };
const OUT_PATTERNS = {
  generate: (m) => m.startsWith('Code Generator finished'),
  review: (m) => m.startsWith('Static review passed') || m.startsWith('Static review failed'),
  execute: (m) => m.startsWith('Program execution successful') || m.startsWith('Skipped: code review failed') || /Compilation failed|timed out|Program execution failed|Execution denied|Workspace operation failed/.test(m),
  test: (m) => /TEST RESULT:/.test(m),
};
function navKeys() { return S.visibleKeys.includes(S.openKey) ? S.visibleKeys : [...runs()].sort((a, b) => b.idx - a.idx).map((r) => r.key); }
function openRun(key, opener) {
  if (!document.getElementById('drawer').hidden) { S.openKey = key; loadDetail(); return; }
  D.lastFocus = opener || document.activeElement; S.openKey = key; hideTip();
  const dr = $('drawer'), sc = $('scrim');
  clearTimeout(D.closing); dr.hidden = false; sc.hidden = false; void dr.offsetWidth;
  dr.classList.add('open'); sc.classList.add('open');
  ['topHeader', 'secnav', 'main'].forEach((i) => { const e = $(i); if (e) e.inert = true; });
  clear($('dBody')).append(h('p', { class: 'muted', text: 'Loading run…' }));
  dr.focus({ preventScroll: true }); $('dClose').focus();
  loadDetail(); renderTimelineSel();
}
function closeDrawer() {
  const dr = $('drawer'), sc = $('scrim'); if (dr.hidden) return;
  stopReplay(); dr.classList.remove('open'); sc.classList.remove('open');
  ['topHeader', 'secnav', 'main'].forEach((i) => { const e = $(i); if (e) e.inert = false; });
  const fin = () => { dr.hidden = true; sc.hidden = true; };
  if (reduced()) fin(); else D.closing = setTimeout(fin, 220);
  S.openKey = null; S.detail = null; renderTimelineSel();
  const lf = D.lastFocus; D.lastFocus = null;
  if (lf && document.contains(lf)) lf.focus(); else { const t = document.querySelector('#tbody .lnk'); if (t) t.focus(); }
}
function renderTimelineSel() { document.querySelectorAll('.mark').forEach((g) => g.classList.toggle('sel', g.getAttribute('data-fid') === 'm' + S.openKey)); }
async function loadDetail() {
  const key = S.openKey, seq = ++S.detailSeq; if (!key) return;
  try {
    const r = await fetch('/api/run/' + encodeURIComponent(key), { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json(); if (seq !== S.detailSeq || key !== S.openKey) return;
    const sig = JSON.stringify(d);
    if (S.detail && S.detail.key === key && S.detail._sig === sig) return;
    const same = S.detail && S.detail.key === key; d._sig = sig; S.detail = d; renderDrawer(same);
  } catch (e) { if (seq === S.detailSeq) { clear($('dBody')).append(h('p', { class: 'muted', text: 'Could not load this run (it may have been removed from the logs). ' + e.message })); } }
}
function outcomeIdx(trace) {
  const o = {}; STAGES.forEach((d) => { const i = trace.findIndex((t) => OUT_PATTERNS[d.id](t.msg)); o[d.id] = i < 0 ? null : i; }); return o;
}
function stepperStates(d, rp) {
  const out = {};
  STAGES.forEach((def) => {
    const fin = d.stages[def.id];
    if (!rp) { out[def.id] = { state: fin.state, note: fin.note }; return; }
    const oi = rp.outcome[def.id];
    if (oi != null && oi <= rp.i) out[def.id] = { state: fin.state, note: fin.note };
    else if (rp.running === def.id) out[def.id] = { state: 'running', note: 'Running…' };
    else out[def.id] = { state: fin.state === 'none' && rp.i >= d.trace.length - 1 ? 'none' : 'waiting', note: fin.state === 'none' && rp.i >= d.trace.length - 1 ? fin.note : 'Waiting' };
  });
  return out;
}
function renderStepper(d) {
  const ol = $('stepper'); if (!ol) return; clear(ol);
  const st = stepperStates(d, D.replay);
  STAGES.forEach((def) => {
    const o = st[def.id];
    const dot = h('span', { class: 'dot', style: { '--c': def.c } }); dot.append(ic(o.state === 'waiting' ? 'waiting' : o.state));
    ol.append(h('li', { class: o.state === 'running' ? 'running' : '' }, dot,
      h('div', null, h('div', { class: 't' }, def.name, h('small', { text: STATE_LABEL[o.state] || o.state })), h('div', { class: 'nt', text: o.note || '' }))));
  });
}
function renderTrace(d) {
  const ol = $('trace'); if (!ol) return; clear(ol);
  const rp = D.replay, tm = d.timing; ol.classList.toggle('replaying', !!rp);
  d.trace.forEach((t, i) => {
    const li = h('li', { class: rp ? (i < rp.i ? 'done' : i === rp.i ? 'cur' : 'pending') : '', style: { '--c': AGENT_COLOR[t.agent] || 'var(--neutral)' } });
    li.append(h('span', { class: 'ag', text: t.agent }), h('span', { class: 'ms' }, h('span', { class: 'pid', text: (tm && tm.lines[i] != null ? '+' + tm.lines[i].toFixed(2) + ' s · ' : '') + (t.pid != null ? 'PID ' + t.pid + ' · ' : '') }), t.msg));
    ol.append(li);
  });
  if (rp && rp.i >= 0 && ol.children[rp.i]) ol.children[rp.i].scrollIntoView({ block: 'nearest', behavior: reduced() ? 'auto' : 'smooth' });
}
function renderReplayControls(d) {
  const box = $('rctl'); if (!box) return; clear(box);
  if (!d.trace.length) return;
  const rp = D.replay, rd = reduced();
  const atEnd = rp && rp.i >= d.trace.length - 1;
  const playLabel = rd ? (rp ? 'Restart' : 'Start step-through') : rp && rp.playing ? 'Pause' : atEnd ? 'Replay again' : rp ? 'Resume' : 'Replay';
  const play = h('button', { class: 'btn sm', type: 'button', id: 'rPlay' }); play.append(icon(rd ? 'i-reset' : (rp && rp.playing ? 'i-pause' : 'i-play')), playLabel);
  play.addEventListener('click', () => { if (rd) { startReplay(d, false); stepReplay(d); } else if (rp && rp.playing) pauseReplay(d); else if (atEnd || !rp) startReplay(d, true); else resumeReplay(d); });
  const step = h('button', { class: 'btn sm', type: 'button' }); step.append(icon('i-step'), 'Step');
  step.addEventListener('click', () => { if (!D.replay) startReplay(d, false); pauseReplay(d, true); stepReplay(d); });
  const rst = h('button', { class: 'btn sm', type: 'button', disabled: !rp }); rst.append(icon('i-reset'), 'Reset');
  rst.addEventListener('click', () => { stopReplay(); renderStepper(d); renderTrace(d); renderReplayControls(d); });
  box.append(play, step, rst);
  if (rd) box.append(h('span', { class: 'muted', style: { fontSize: '12.5px', alignSelf: 'center' }, text: 'Reduced motion is on: step through manually.' }));
}
function startReplay(d, auto) {
  stopReplay(true);
  D.replay = { i: -1, running: null, playing: false, outcome: outcomeIdx(d.trace), timer: 0 };
  if (auto && !reduced()) resumeReplay(d); else refreshReplay(d);
}
function resumeReplay(d) {
  const rp = D.replay; if (!rp) return; rp.playing = true;
  const delay = Math.max(110, Math.min(520, 14000 / Math.max(1, d.trace.length)));
  clearInterval(rp.timer);
  rp.timer = setInterval(() => { if (!stepReplay(d)) { pauseReplay(d); } }, delay);
  refreshReplay(d);
}
function pauseReplay(d, silent) { const rp = D.replay; if (!rp) return; rp.playing = false; clearInterval(rp.timer); if (!silent) refreshReplay(d); }
function stepReplay(d) {
  const rp = D.replay; if (!rp) return false;
  if (rp.i >= d.trace.length - 1) { return false; }
  rp.i++;
  const sid = AGENT_STAGE[d.trace[rp.i].agent]; if (sid) rp.running = sid;
  // a stage whose outcome line was just passed stops running
  if (rp.running && rp.outcome[rp.running] != null && rp.outcome[rp.running] <= rp.i && !sid) rp.running = null;
  refreshReplay(d); return rp.i < d.trace.length - 1;
}
function refreshReplay(d) { renderStepper(d); renderTrace(d); renderReplayControls(d); }
function stopReplay(keep) { if (D.replay) { clearInterval(D.replay.timer); D.replay = null; } }
function flagLines(text) {
  const out = [];
  text.split('\n').forEach((ln, i) => {
    const t = ln.trim(); if (t.startsWith('//') || t.startsWith('*') || t.startsWith('/*')) return;
    for (const re of UNSAFE) { const m = re.exec(ln); if (m) { out.push({ line: i + 1, what: (m[1] ? m[1] + '()' : 'scanf("%s")') }); break; } }
  });
  return out;
}
function renderDrawer(preserve) {
  const d = S.detail, body = $('dBody'), keepTop = preserve ? body.scrollTop : 0, hadReplay = preserve ? D.replay : null;
  if (!preserve) stopReplay();
  clear(body);
  const keys = navKeys(), pos = keys.indexOf(d.key);
  $('dPos').textContent = 'Run ' + (d.idx + 1) + ' of ' + runs().length + (pos >= 0 ? ' · ' + (pos + 1) + '/' + keys.length + ' in view' : '');
  $('dPrev').disabled = pos <= 0; $('dNext').disabled = pos < 0 || pos >= keys.length - 1;
  const title = h('h2', { id: 'dTitle', text: '#' + (d.idx + 1) + ' ' + d.name });
  body.append(title, h('div', { class: 'dmeta' }, badge(d.verdict), h('span', { text: d.ts })));
  if (d.reason) body.append(h('p', { style: { marginTop: '10px' } }, h('b', { text: 'Why it didn’t pass: ' }), reasonLabel(d.reason)));
  body.append(h('p', { class: 'muted', style: { marginTop: '6px', overflowWrap: 'anywhere' } }, 'Program result: ', d.detail || '(none)'));
  body.append(h('h3', { text: 'Stages' }), h('ol', { class: 'stepper', id: 'stepper', 'aria-label': 'Stage outcomes' }));
  const facts = h('dl', { class: 'facts' });
  facts.append(h('dt', { text: 'Log lines' }), h('dd', { text: d.has_trace ? String(d.trace.length) : 'no trace matched' }));
  const pidDd = h('dd');
  const ents = Object.entries(d.pids);
  if (!ents.length) pidDd.textContent = 'none recorded';
  ents.forEach(([a, ps]) => pidDd.append(h('span', { class: 'pidchip' }, h('span', { class: 'sw', style: { background: AGENT_COLOR[a] || 'var(--neutral)' } }), a + ': ' + ps.join(', '))));
  facts.append(h('dt', { text: 'PIDs' }), pidDd);
  facts.append(h('dt', { text: 'Duration' }), h('dd', { text: d.dur != null ? fmtDur(d.dur) + (d.dur_src === 'wall' ? ' (wall clock, measured by this dashboard)' : ' (first to last log line, measured by this dashboard)') : 'not timed' }));
  facts.append(h('dt', { text: 'Program' }), h('dd', { text: d.program != null ? 'Catalog program ' + d.program : 'Custom or unrecognised' }));
  body.append(h('h3', { text: 'Facts' }), facts);
  body.append(h('h3', { text: 'Timing' }), timingBlock(d));
  body.append(h('div', { class: 'trhead' }, h('h3', { text: 'Agent trace' }), h('div', { class: 'rctl', id: 'rctl' })));
  if (d.has_trace) body.append(h('ol', { class: 'trace', id: 'trace', 'aria-label': 'Agent trace' }));
  else body.append(h('p', { class: 'muted', text: 'No trace could be matched to this run. Run history rows and “All AI Agents completed” markers are paired from the newest run backwards; this row is older than the oldest trace left in the log.' }));
  // source
  body.append(h('h3', { text: 'C source that was executed' }));
  if (d.source) {
    const flags = flagLines(d.source.text), fl = new Set(flags.map((f) => f.line));
    body.append(h('p', { class: 'muted', style: { fontSize: '13px', marginBottom: '6px', overflowWrap: 'anywhere' }, text: d.source.filename + ' — ' + d.source.label }));
    if (flags.length) {
      const w = h('div', { class: 'warnline' }); w.append(icon('i-warn'), h('span', { text: plural(flags.length, 'unsafe call') + ' flagged: ' + flags.map((f) => f.what + ' on line ' + f.line).join(', ') + '. The pipeline’s own reviewer only checks gets( and system(.' }));
      body.append(w);
    } else body.append(h('p', { class: 'muted', style: { fontSize: '13px', marginBottom: '6px' }, text: 'No unsafe calls flagged by the dashboard’s scan (gets, strcpy, strcat, sprintf, system, popen, scanf %s…).' }));
    const cv = h('div', { class: 'code-view', tabindex: 0, role: 'region', 'aria-label': 'C source' });
    d.source.text.replace(/\n$/, '').split('\n').forEach((ln, i) => {
      const row = h('div', { class: 'ln' + (fl.has(i + 1) ? ' flag' : '') }, h('span', { class: 'no', text: String(i + 1) }), h('span', { class: 'tx', text: ln }));
      if (fl.has(i + 1)) row.setAttribute('title', 'Unsafe call');
      cv.append(row);
    });
    body.append(cv);
  } else body.append(h('p', { class: 'muted', text: 'The program does not keep a copy of the source for this run (only the latest custom program stays on disk).' }));
  if (hadReplay && d.has_trace) { D.replay = hadReplay; }
  renderStepper(d); renderTrace(d); renderReplayControls(d);
  body.scrollTop = keepTop;
}
$('dPrev').addEventListener('click', () => stepRun(-1)); $('dNext').addEventListener('click', () => stepRun(1));
$('dClose').addEventListener('click', closeDrawer); $('scrim').addEventListener('click', closeDrawer);
function stepRun(delta) { const keys = navKeys(), i = keys.indexOf(S.openKey), nk = keys[i + delta]; if (nk) { S.openKey = nk; stopReplay(); loadDetail(); renderTimelineSel(); } }
document.addEventListener('keydown', (e) => {
  const dr = $('drawer'); if (dr.hidden) return;
  if (e.key === 'Escape') { e.preventDefault(); closeDrawer(); return; }
  if (e.key === 'Tab') {
    const f = [...dr.querySelectorAll('button:not(:disabled), [href], input, textarea, select, [tabindex]:not([tabindex="-1"])')].filter((x) => x.offsetParent !== null || x === document.activeElement);
    if (!f.length) { e.preventDefault(); return; }
    const first = f[0], last = f[f.length - 1];
    if (!dr.contains(document.activeElement)) { e.preventDefault(); first.focus(); }
    else if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    return;
  }
  if ((e.key === 'ArrowLeft' || e.key === 'ArrowRight') && !e.altKey && !e.ctrlKey && !e.metaKey) {
    const t = e.target; if (t && (/^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName) || t.isContentEditable)) return;
    e.preventDefault(); stepRun(e.key === 'ArrowLeft' ? -1 : 1);
  }
});

/* ---------- Run panel + terminal ---------- */
function setMsg(t, err) { const m = $('runMsg'); m.textContent = t || ''; m.classList.toggle('err', !!err); }
function renderRunPanel() {
  const st = S.st, cat = st.meta.catalog, caps = st.meta.caps, busy = !!(st.job && !st.job.done);
  const box = clear($('runBtns'));
  const mk = (label, n, primary, handler) => {
    const b = h('button', { class: 'btn' + (primary ? ' primary' : ''), type: 'button', disabled: busy || !caps.catalog });
    if (n) b.append(h('span', { class: 'n', text: String(n) })); else b.append(icon('i-play'));
    b.append(label); b.addEventListener('click', handler); return b;
  };
  const names = cat.length ? cat : [1, 2, 3, 4, 5].map((n) => ({ n, name: 'Program ' + n }));
  names.forEach((c) => box.append(mk(c.name, c.n, false, () => startRun({ action: 'catalog', n: c.n }))));
  box.append(mk('Run all 5', null, true, () => startRun({ action: 'all' })));
  $('customRun').disabled = busy || !caps.custom;
  $('customBox').style.display = caps.custom ? '' : 'none';
  $('emptyRun').disabled = busy || !caps.catalog;
  $('code').maxLength = caps.code_max;
  updateCodeCount();
  $('stopBtn').disabled = !busy;
  if (!caps.catalog) setMsg('This pipeline source has no --run mode, so catalog runs are unavailable.', true);
  const timed = runs().filter((r) => r.elapsed != null).map((r) => r.elapsed), med = median(timed);
  $('insRun').textContent = med != null ? 'Runs started here take about ' + fmtDur(med) + ' (median of ' + plural(timed.length, 'timed run') + '); a program that never finishes is killed at its configured timeout.' :
    'Runs started from this panel are timed; none have been recorded yet.';
}
function updateCodeCount() { const max = S.st ? S.st.meta.caps.code_max : 4094, n = new TextEncoder().encode($('code').value).length; $('codeCount').textContent = '(' + n + ' / ' + max + ' bytes)'; }
$('code').addEventListener('input', updateCodeCount);
function tbadge(kind, text, ico) { const b = $('tBadge'); b.className = 'tbadge ' + kind; clear(b); if (ico) b.append(icon(ico)); b.append(text); }
function termAppend(node) {
  const o = $('tOut'), near = o.scrollHeight - o.scrollTop - o.clientHeight < 40;
  o.append(node); if (near) o.scrollTop = o.scrollHeight;
}
function attachStream(jobId) {
  if (S.es) { S.es.close(); S.es = null; }
  S.jobId = jobId; clear($('tOut'));
  const es = new EventSource('/api/stream?job=' + encodeURIComponent(jobId)); S.es = es;
  const on = (name, fn) => es.addEventListener(name, (ev) => { try { fn(JSON.parse(ev.data)); } catch (e) {} });
  on('cmd', (d) => {
    $('tCmd').textContent = d.command; $('tStep').textContent = d.total > 1 ? 'step ' + (d.index + 1) + '/' + d.total : '';
    if (d.total > 1 || d.index > 0 || $('tOut').childNodes.length) termAppend(h('span', { class: 'sep', text: '── ' + (d.total > 1 ? '[' + (d.index + 1) + '/' + d.total + '] ' : '') + d.label + ' · ' + d.command }));
    S.running = true; S.termStart = performance.now(); tbadge('run', 'running', 'i-running');
    clearInterval(S.termTimer); S.termTimer = setInterval(() => { $('tElapsed').textContent = fmtDur((performance.now() - S.termStart) / 1000); }, 200);
  });
  on('out', (d) => { const t = stripAnsi(d.text); if (!t) return; termAppend(h('span', { class: d.stream === 'stderr' ? 'e' : 'o', text: t })); });
  on('note', (d) => termAppend(h('span', { class: 'sys', text: '· ' + d.text })));
  on('exit', (d) => {
    clearInterval(S.termTimer); $('tElapsed').textContent = fmtDur(d.elapsed);
    const res = d.results && d.results[d.results.length - 1];
    let kind = 'fail', text, ico = 'i-fail';
    if (d.timed_out) text = 'killed · 60 s cap';
    else if (d.cancelled) { text = 'stopped'; ico = 'i-int'; kind = 'skip'; }
    else if (d.signal) text = 'signal ' + d.signal;
    else if (d.code === 0) { text = 'exit 0'; kind = 'pass'; ico = 'i-pass'; }
    else text = 'exit ' + d.code;
    const ver = res ? ' · verdict ' + res.verdict : '';
    tbadge(kind, text + ver, ico);
    termAppend(h('span', { class: 'sys', text: '· ' + text + ' after ' + fmtDur(d.elapsed) + (res ? ' · ' + res.name + ' → ' + res.verdict + ' (' + res.detail + ')' : ' · no new run_history row') }));
  });
  on('done', () => { clearInterval(S.termTimer); es.close(); if (S.es === es) S.es = null; S.running = false; $('stopBtn').disabled = true; poll(true); });
  es.onerror = () => { if (es.readyState === EventSource.CLOSED) { clearInterval(S.termTimer); S.es = null; } };
}
async function startRun(body) {
  setMsg('');
  try {
    const r = await fetch('/api/run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const j = await r.json();
    if (r.status === 409) { setMsg('A run is already in progress. Wait for it to finish or press Stop.', true); poll(true); return; }
    if (!r.ok) { setMsg(j.error || 'Request failed (' + r.status + ')', true); return; }
    $('term').scrollIntoView({ block: 'nearest', behavior: reduced() ? 'auto' : 'smooth' });
    S.st.job = { id: j.job, done: false, label: j.label, kind: j.kind }; renderRunPanel();
    attachStream(j.job); poll(true);
  } catch (e) { setMsg('Could not reach the dashboard server: ' + e.message, true); }
}
$('emptyRun').addEventListener('click', () => { $('emptyMsg').textContent = ''; startRun({ action: 'all' }).then(() => { $('emptyMsg').textContent = 'Running all 5 demos — output is in the Run panel below.'; }); });
$('stopBtn').addEventListener('click', async () => { try { await fetch('/api/stop', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }); } catch (e) {} });
$('customRun').addEventListener('click', () => {
  const code = $('code').value, exp = $('expected').value;
  if (!code.trim()) { setMsg('Paste some C code first.', true); $('code').focus(); return; }
  if (new TextEncoder().encode(code).length > S.st.meta.caps.code_max) { setMsg('Code is larger than the ' + S.st.meta.caps.code_max + '-byte limit.', true); return; }
  if (/[\r\n]/.test(exp)) { setMsg('Expected output must be a single line.', true); return; }
  $('cfPre').textContent = code.length > 600 ? code.slice(0, 600) + '\n… (' + (code.length - 600) + ' more characters)' : code;
  const dlg = $('confirm');
  dlg.returnValue = ''; dlg.addEventListener('close', function once() {
    dlg.removeEventListener('close', once);
    if (dlg.returnValue === 'ok') startRun({ action: 'custom', code, expected: exp });
  });
  dlg.showModal(); $('cfCancel').focus();
});

/* ---------- polling + orchestration ---------- */
function setLive(ok, text) { const l = $('live'); l.className = 'live ' + (ok ? 'ok' : 'bad'); $('liveText').textContent = text; }
function renderAll() {
  const st = S.st, list = runs(), empty = !list.length && !st.agent_lines;
  document.body.classList.toggle('is-empty', empty); $('empty').hidden = !empty;
  const m = st.meta;
  const sub = clear($('subtitle'));
  sub.append('Watching ', h('code', { text: 'logs/' }), ' and ', h('code', { text: 'pipeline.conf' }), ' for ', h('code', { text: m.source }), '. ', h('span', { class: 'muted', text: m.why + '.' }));
  renderStrip(); renderKpis(); renderChips(); renderHead(); renderTable(); renderRunPanel(); renderLimits();
  if (!empty) { renderTimeline(); renderPerformance(); renderBreakdown(); renderAgents(); }
  $('foot').textContent = 'Everything shown is read from the program’s own files in this folder: ' + (st.files.history ? 'run_history.log' : '(no run_history.log yet)') + ', ' + (st.files.activity ? 'agent_activity.log' : '(no agent_activity.log yet)') + ', pipeline.conf.' + (st.unparsed_history ? ' ' + plural(st.unparsed_history, 'history line') + ' could not be parsed and were ignored.' : '');
  if (S.openKey) loadDetail();
  const j = st.job; if (j && j.id !== S.jobSeen) { S.jobSeen = j.id; if (!S.es && !S.running && j.id !== S.jobId) attachStream(j.id); }
}
async function poll(once) {
  if (S.polling && !once) return; S.polling = true;
  try {
    const r = await fetch('/api/state?rev=' + encodeURIComponent(S.rev), { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json(); S.lastOk = Date.now();
    if (d.unchanged) { if (S.st) { const was = S.st.job; S.st.job = d.job; if (JSON.stringify(was) !== JSON.stringify(d.job)) { renderRunPanel(); renderStrip(); } } }
    else { S.st = d; S.rev = d.rev; renderAll(); }
    const busy = S.st && S.st.job && !S.st.job.done;
    setLive(true, busy ? 'Live · run in progress' : 'Live · updated ' + fmtClock(new Date()));
  } catch (e) { setLive(false, 'Offline · retrying'); }
  S.polling = false;
}
(function loop() { poll().finally(() => setTimeout(loop, document.hidden ? 4000 : (S.st && S.st.job && !S.st.job.done ? 1000 : 1500))); })();
$('q').addEventListener('input', (e) => { S.q = e.target.value; renderTable(); });
$('csvBtn').addEventListener('click', exportCsv);
let rz = 0; addEventListener('resize', () => { clearTimeout(rz); rz = setTimeout(() => { if (S.st && runs().length) { renderTimeline(); } fitAll(); }, 120); });
/* section nav highlight */
(function () {
  const links = [...document.querySelectorAll('.secnav a')], map = new Map();
  links.forEach((a) => { const t = document.querySelector(a.getAttribute('href')); if (t) map.set(t, a); });
  const io = new IntersectionObserver((es) => { es.forEach((e) => { if (e.isIntersecting) { links.forEach((a) => a.removeAttribute('aria-current')); map.get(e.target).setAttribute('aria-current', 'true'); } }); }, { rootMargin: '-45% 0px -50% 0px' });
  map.forEach((a, t) => io.observe(t));
})();
</script>
</body>
</html>
'''

if __name__ == "__main__":
    main()
