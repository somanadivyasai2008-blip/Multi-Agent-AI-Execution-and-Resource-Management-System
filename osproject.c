/*
 * ============================================================================
 * ossp_system.c
 *
 * Multi-Agent AI Execution and Resource Management System
 * -- single-file build --
 *
 * This file is every module of the project (logger, FIFO-based IPC,
 * restricted-workspace file guard, backup-before-write, cross-process
 * flock() synchronization, /proc-based resource monitoring, the agent
 * table, the agent worker program, and the manager process) merged into
 * one translation unit, so the whole system compiles and links as a
 * SINGLE executable with a single `gcc` command.
 *
 * WHY ONE PROCESS IMAGE CAN STILL BE "A MANAGER + SEPARATE AGENT
 * PROCESSES":
 *   The abstract requires each AI agent to run as an independent Linux
 *   process, created with fork() and started with execv(). That is still
 *   exactly what happens here -- the manager calls fork(), and the child
 *   calls execv() to reload a fresh process image before doing any agent
 *   work. The only difference from the original multi-file version is
 *   *which file* gets execv()'d: instead of a separate `bin/agent`
 *   binary, the child re-execs THIS SAME binary (found via
 *   /proc/self/exe), passing a hidden sentinel argument that tells the
 *   freshly loaded process to run the agent logic instead of the manager
 *   logic. This is the same "multi-call binary" technique used by
 *   busybox, toybox, and git (`git` itself dispatches on argv[0]/argv[1]
 *   to dozens of sub-programs) -- a single executable file, multiple
 *   independent process roles. fork()+execv() are still genuinely
 *   called; the process is still genuinely replaced (new PID, no
 *   inherited manager memory beyond what execv() by definition wipes).
 *
 * Build:   gcc -Wall -Wextra -std=gnu11 -O2 -pthread -o ossp_system ossp_system.c
 * Run:     ./ossp_system config/agents.conf
 *
 * See README.md for the full build/run/VS Code instructions and docs/ for
 * the architecture, the CO-1/CO-2/CO-3 traceability matrix, a demo
 * script, and viva preparation notes (all still accurate -- only the
 * packaging of the source into one file changed, not the design).
 * ============================================================================
 */

/* ---------------------------------------------------------------------- */
/*  Standard / POSIX includes (union of every module's includes)          */
/* ---------------------------------------------------------------------- */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <time.h>
#include <libgen.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/file.h>
#include <sys/select.h>
#include <sys/time.h>

/* ============================================================================
 * SECTION 1: common.h  -- shared constants, structs, wire protocol
 * ==========================================================================*/

#define DIR_RUNTIME     "runtime"
#define DIR_WORKSPACE   "runtime/workspace"
#define DIR_BACKUPS     "runtime/backups"
#define DIR_LOGS        "runtime/logs"
#define DIR_IPC         "runtime/ipc"
#define LOG_FILE_PATH   "runtime/logs/manager.log"
#define CONFIG_FILE_DEFAULT "config/agents.conf"

#define MAX_AGENTS          16
#define MAX_LINE            512
#define MAX_MSG             256
/* Must be >= PATH_MAX (4096 on Linux): glibc's fortified realpath()
 * (__realpath_chk, active whenever this file is built with -O1/-O2 and
 * the default _FORTIFY_SOURCE) requires any destination buffer passed
 * to realpath() to be at least PATH_MAX bytes, and aborts the process
 * with "buffer overflow detected" otherwise -- even if the actual
 * resolved path is much shorter. A smaller MAX_PATH_LEN here builds and
 * runs fine at -O0 (no fortify checks), which is why this only surfaces
 * under an optimized build; 4096 is correct at every optimization level.
 * (<limits.h>, included above, defines PATH_MAX on Linux; the fallback
 * below only matters on a libc that does not define it at all.) */
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#define MAX_PATH_LEN        PATH_MAX
#define MAX_NAME_LEN        64
#define MAX_LOG_MSG_LEN     384
#define MAX_LOG_LINE_LEN    512
#define MONITOR_INTERVAL_SEC 2
#define GRACEFUL_SHUTDOWN_TIMEOUT_SEC 5

#define DEFAULT_CPU_LIMIT_PERCENT   50.0
#define DEFAULT_MEM_LIMIT_KB        65536

/* The hidden sentinel passed as argv[0] when the manager re-execs this
 * same binary to become an agent (see spawn_agent() in Section 11). */
#define AGENT_MODE_SENTINEL "__ossp_agent_mode__"

typedef enum {
    AGENT_STATE_NEW = 0,
    AGENT_STATE_READY,
    AGENT_STATE_RUNNING,
    AGENT_STATE_WAITING,
    AGENT_STATE_VIOLATION,
    AGENT_STATE_TERMINATED,
    AGENT_STATE_CRASHED
} agent_state_t;

static inline const char *agent_state_str(agent_state_t s) {
    switch (s) {
        case AGENT_STATE_NEW:        return "NEW";
        case AGENT_STATE_READY:      return "READY";
        case AGENT_STATE_RUNNING:    return "RUNNING";
        case AGENT_STATE_WAITING:    return "WAITING";
        case AGENT_STATE_VIOLATION:  return "VIOLATION";
        case AGENT_STATE_TERMINATED: return "TERMINATED";
        case AGENT_STATE_CRASHED:    return "CRASHED";
        default:                     return "UNKNOWN";
    }
}

/*
 * Manager <-> Agent wire protocol (newline-terminated ASCII lines over
 * per-agent FIFOs):
 *   CMD|TASK|<taskfile>   CMD|PAUSE|-   CMD|RESUME|-   CMD|SHUTDOWN|-
 *   STAT|<pid>|<state>|<cpu_pct>|<mem_kb>|<detail>
 */
#define MSG_CMD_PREFIX   "CMD"
#define MSG_STAT_PREFIX  "STAT"
#define CMD_TASK     "TASK"
#define CMD_PAUSE    "PAUSE"
#define CMD_RESUME   "RESUME"
#define CMD_SHUTDOWN "SHUTDOWN"

typedef struct {
    int            id;
    char           name[MAX_NAME_LEN];
    pid_t          pid;
    agent_state_t  state;
    double         cpu_limit_percent;
    long           mem_limit_kb;
    double         last_cpu_percent;
    long           last_mem_kb;
    int            violation_strikes;
    char           cmd_fifo_path[MAX_PATH_LEN];
    char           stat_fifo_path[MAX_PATH_LEN];
    int            cmd_fifo_wfd;
    int            stat_fifo_rfd;
    int            active;
} agent_record_t;

/* argv layout used both when constructing the agent's argv (manager side)
 * and when parsing it (agent side) -- kept as one enum so they can never
 * drift apart, exactly as when they lived in separate files. */
enum {
    ARG_PROG = 0,
    ARG_ID,
    ARG_NAME,
    ARG_CMD_FIFO,
    ARG_STAT_FIFO,
    ARG_WORKSPACE_ROOT,
    ARG_TASK_FILE,
    ARG_CPU_LIMIT,
    ARG_MEM_LIMIT,
    ARG_COUNT
};

/* ============================================================================
 * SECTION 2: logger  -- thread-safe, append-atomic structured logging
 * ==========================================================================*/

typedef enum { LOG_INFO = 0, LOG_WARN, LOG_ERROR, LOG_VIOLATION } log_level_t;

static int g_log_fd = -1;
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;

static const char *level_str(log_level_t level) {
    switch (level) {
        case LOG_INFO:      return "INFO";
        case LOG_WARN:      return "WARN";
        case LOG_ERROR:     return "ERROR";
        case LOG_VIOLATION: return "VIOLATION";
        default:            return "?";
    }
}

static int logger_init(const char *path) {
    g_log_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    return (g_log_fd >= 0) ? 0 : -1;
}

/* On Linux, a single write() to an O_APPEND descriptor is atomic w.r.t.
 * the file offset, so the manager's threads (also serialized here by
 * g_log_mutex) and every agent process (its own fd to the same path) can
 * log concurrently without interleaving partial lines. */
static void logger_log(log_level_t level, const char *source, const char *fmt, ...) {
    char timebuf[32];
    char msgbuf[MAX_LOG_MSG_LEN];
    char line[MAX_LOG_LINE_LEN];
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", &tmv);

    pthread_mutex_lock(&g_log_mutex);

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msgbuf, sizeof(msgbuf), fmt, ap);
    va_end(ap);

    int n = snprintf(line, sizeof(line), "[%s] [%-9s] [%s] %s\n",
                      timebuf, level_str(level), source, msgbuf);

    if (g_log_fd >= 0 && n > 0) {
        ssize_t written = write(g_log_fd, line, (size_t)n);
        (void)written;
    } else {
        fputs(line, stderr);
    }

    pthread_mutex_unlock(&g_log_mutex);
}

static void logger_close(void) {
    pthread_mutex_lock(&g_log_mutex);
    if (g_log_fd >= 0) { close(g_log_fd); g_log_fd = -1; }
    pthread_mutex_unlock(&g_log_mutex);
}

/* ============================================================================
 * SECTION 3: ipc_fifo  -- named-pipe (FIFO) helpers
 * ==========================================================================*/

static void ipc_build_paths(int id, char *cmd_path, char *stat_path) {
    snprintf(cmd_path, MAX_PATH_LEN, "%s/mgr_to_agent%d.fifo", DIR_IPC, id);
    snprintf(stat_path, MAX_PATH_LEN, "%s/agent%d_to_mgr.fifo", DIR_IPC, id);
}

static int ipc_create_fifo(const char *path) {
    if (mkfifo(path, 0660) == -1) {
        if (errno == EEXIST) return 0;
        perror("mkfifo");
        return -1;
    }
    return 0;
}

static void ipc_remove_fifo(const char *path) {
    if (unlink(path) == -1 && errno != ENOENT) perror("unlink(fifo)");
}

static int ipc_open(const char *path, int flags) {
    return open(path, flags);
}

static int ipc_send_line(int fd, const char *line) {
    char buf[MAX_MSG];
    int n = snprintf(buf, sizeof(buf), "%s\n", line);
    if (n <= 0) return -1;

    ssize_t total = 0;
    while (total < n) {
        ssize_t w = write(fd, buf + total, (size_t)(n - total));
        if (w == -1) { if (errno == EINTR) continue; return -1; }
        total += w;
    }
    return 0;
}

static int ipc_recv_line(int fd, char *buf, size_t buf_len) {
    size_t pos = 0;
    int saw_newline = 0;

    while (pos + 1 < buf_len) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r == 0) {
            if (pos == 0) return 0;
            break;
        }
        if (r == -1) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return -2;
            return -1;
        }
        if (c == '\n') { saw_newline = 1; break; }
        buf[pos++] = c;
    }
    buf[pos] = '\0';

    if (pos == 0 && !saw_newline) return 0;
    return (int)pos;
}

/* ============================================================================
 * SECTION 4: file_guard  -- restricted-workspace path validation
 * ==========================================================================*/

static char g_sandbox_root[MAX_PATH_LEN];

static int file_guard_init(const char *workspace_root) {
    if (realpath(workspace_root, g_sandbox_root) == NULL) {
        perror("file_guard_init: realpath(workspace_root)");
        return -1;
    }
    return 0;
}

static int is_inside_sandbox(const char *resolved_dir) {
    size_t root_len = strlen(g_sandbox_root);
    if (strncmp(resolved_dir, g_sandbox_root, root_len) != 0) return 0;
    char next = resolved_dir[root_len];
    return (next == '\0' || next == '/');
}

/* Resolves the *directory* component with realpath() (so new output
 * files, which don't exist yet, are still allowed) and rejects anything
 * whose resolved directory falls outside the sandbox root -- this is
 * what stops ".."-traversal and symlink escapes. */
static int file_guard_validate(const char *requested_path, char *resolved_out) {
    if (g_sandbox_root[0] == '\0') {
        fprintf(stderr, "file_guard_validate: file_guard_init() was not called\n");
        return -1;
    }

    char path_copy_for_dir[MAX_PATH_LEN];
    char path_copy_for_base[MAX_PATH_LEN];
    snprintf(path_copy_for_dir, sizeof(path_copy_for_dir), "%s", requested_path);
    snprintf(path_copy_for_base, sizeof(path_copy_for_base), "%s", requested_path);

    char *dir  = dirname(path_copy_for_dir);
    char *base = basename(path_copy_for_base);

    if (strcmp(base, "..") == 0 || strcmp(base, ".") == 0) return -1;

    char resolved_dir[MAX_PATH_LEN];
    if (realpath(dir, resolved_dir) == NULL) return -1;
    if (!is_inside_sandbox(resolved_dir)) return -1;

    int n = snprintf(resolved_out, MAX_PATH_LEN, "%s/%s", resolved_dir, base);
    if (n <= 0 || (size_t)n >= MAX_PATH_LEN) return -1;
    return 0;
}

/* ============================================================================
 * SECTION 5: backup  -- copy-before-write data recovery
 * ==========================================================================*/

static char g_backups_dir[MAX_PATH_LEN];

static int backup_init(const char *backups_dir) {
    snprintf(g_backups_dir, sizeof(g_backups_dir), "%s", backups_dir);
    struct stat st;
    if (stat(g_backups_dir, &st) != 0) {
        fprintf(stderr, "backup_init: backups dir '%s' does not exist\n", g_backups_dir);
        return -1;
    }
    return 0;
}

static int backup_before_write(const char *original_path) {
    struct stat st;
    if (stat(original_path, &st) != 0) {
        if (errno == ENOENT) return 0; /* nothing to protect yet */
        perror("backup_before_write: stat");
        return -1;
    }

    char path_copy[MAX_PATH_LEN];
    snprintf(path_copy, sizeof(path_copy), "%s", original_path);
    char *base = basename(path_copy);

    char backup_path[MAX_PATH_LEN + 64];
    snprintf(backup_path, sizeof(backup_path), "%s/%s.%ld.bak",
              g_backups_dir, base, (long)time(NULL));

    int src = open(original_path, O_RDONLY);
    if (src == -1) { perror("backup_before_write: open(src)"); return -1; }
    int dst = open(backup_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dst == -1) { perror("backup_before_write: open(dst)"); close(src); return -1; }

    char buf[4096];
    ssize_t r;
    int ok = 1;
    while ((r = read(src, buf, sizeof(buf))) > 0) {
        ssize_t off = 0;
        while (off < r) {
            ssize_t w = write(dst, buf + off, (size_t)(r - off));
            if (w == -1) { ok = 0; break; }
            off += w;
        }
        if (!ok) break;
    }
    if (r == -1) ok = 0;

    close(src);
    close(dst);
    return ok ? 0 : -1;
}

/* ============================================================================
 * SECTION 6: sync_lock  -- flock()-based cross-process mutual exclusion
 * ==========================================================================*/

#define LOCK_SUFFIX ".lock"

static int sync_lock_acquire(const char *target_path) {
    char lock_path[1024];
    int n = snprintf(lock_path, sizeof(lock_path), "%s%s", target_path, LOCK_SUFFIX);
    if (n <= 0 || (size_t)n >= sizeof(lock_path)) return -1;

    int fd = open(lock_path, O_CREAT | O_RDWR, 0660);
    if (fd == -1) { perror("sync_lock_acquire: open"); return -1; }

    if (flock(fd, LOCK_EX) == -1) {
        perror("sync_lock_acquire: flock");
        close(fd);
        return -1;
    }
    return fd;
}

static void sync_lock_release(int lock_fd) {
    if (lock_fd < 0) return;
    flock(lock_fd, LOCK_UN);
    close(lock_fd);
}

/* ============================================================================
 * SECTION 7: resource_monitor  -- /proc-based CPU% and RSS sampling
 * ==========================================================================*/

typedef struct {
    pid_t pid;
    unsigned long long prev_total_ticks;
    struct timespec prev_wall;
    int have_prev;
} resource_monitor_t;

static void resource_monitor_init(resource_monitor_t *mon, pid_t pid) {
    memset(mon, 0, sizeof(*mon));
    mon->pid = pid;
    mon->have_prev = 0;
}

static int read_cpu_ticks(pid_t pid, unsigned long long *total_ticks) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/stat", pid);

    FILE *f = fopen(path, "r");
    if (!f) return -1;

    char line[1024];
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
    fclose(f);

    char *close_paren = strrchr(line, ')');
    if (!close_paren) return -1;

    char *cursor = close_paren + 2;
    unsigned long long utime = 0, stime = 0;
    int field = 3;
    char *tok = strtok(cursor, " ");
    while (tok) {
        if (field == 14) utime = strtoull(tok, NULL, 10);
        if (field == 15) { stime = strtoull(tok, NULL, 10); break; }
        tok = strtok(NULL, " ");
        field++;
    }

    *total_ticks = utime + stime;
    return 0;
}

static int read_rss_kb(pid_t pid, long *rss_kb) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/status", pid);

    FILE *f = fopen(path, "r");
    if (!f) return -1;

    char line[256];
    int found = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, "%ld", rss_kb);
            found = 1;
            break;
        }
    }
    fclose(f);
    return found ? 0 : -1;
}

static int resource_monitor_sample(resource_monitor_t *mon, double *cpu_percent, long *mem_kb) {
    unsigned long long ticks;
    if (read_cpu_ticks(mon->pid, &ticks) != 0) {
        *cpu_percent = 0.0; *mem_kb = 0;
        return -1;
    }
    if (read_rss_kb(mon->pid, mem_kb) != 0) *mem_kb = 0;

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    if (!mon->have_prev) {
        mon->prev_total_ticks = ticks;
        mon->prev_wall = now;
        mon->have_prev = 1;
        *cpu_percent = 0.0;
        return 0;
    }

    long clk_tck = sysconf(_SC_CLK_TCK);
    double elapsed_sec = (now.tv_sec - mon->prev_wall.tv_sec) +
                         (now.tv_nsec - mon->prev_wall.tv_nsec) / 1e9;
    unsigned long long delta_ticks = ticks - mon->prev_total_ticks;

    if (elapsed_sec > 0.0 && clk_tck > 0) {
        double delta_sec = (double)delta_ticks / (double)clk_tck;
        *cpu_percent = (delta_sec / elapsed_sec) * 100.0;
    } else {
        *cpu_percent = 0.0;
    }

    mon->prev_total_ticks = ticks;
    mon->prev_wall = now;
    return 0;
}

/* ============================================================================
 * SECTION 8: agent_table  -- mutex-protected shared manager state
 * ==========================================================================*/

static agent_record_t  g_agents[MAX_AGENTS];
static int              g_agent_count = 0;
static pthread_mutex_t  g_agents_mutex = PTHREAD_MUTEX_INITIALIZER;

static void at_init(void) {
    pthread_mutex_lock(&g_agents_mutex);
    memset(g_agents, 0, sizeof(g_agents));
    for (int i = 0; i < MAX_AGENTS; i++) {
        g_agents[i].pid = -1;
        g_agents[i].cmd_fifo_wfd = -1;
        g_agents[i].stat_fifo_rfd = -1;
    }
    g_agent_count = 0;
    pthread_mutex_unlock(&g_agents_mutex);
}

static int at_register(const char *name, double cpu_limit_percent, long mem_limit_kb,
                        const char *cmd_fifo_path, const char *stat_fifo_path) {
    pthread_mutex_lock(&g_agents_mutex);
    if (g_agent_count >= MAX_AGENTS) { pthread_mutex_unlock(&g_agents_mutex); return -1; }
    int id = g_agent_count++;
    agent_record_t *a = &g_agents[id];
    a->id = id;
    snprintf(a->name, sizeof(a->name), "%s", name);
    a->pid = -1;
    a->state = AGENT_STATE_NEW;
    a->cpu_limit_percent = cpu_limit_percent;
    a->mem_limit_kb = mem_limit_kb;
    a->last_cpu_percent = 0.0;
    a->last_mem_kb = 0;
    a->violation_strikes = 0;
    snprintf(a->cmd_fifo_path, sizeof(a->cmd_fifo_path), "%s", cmd_fifo_path);
    snprintf(a->stat_fifo_path, sizeof(a->stat_fifo_path), "%s", stat_fifo_path);
    a->cmd_fifo_wfd = -1;
    a->stat_fifo_rfd = -1;
    a->active = 1;
    pthread_mutex_unlock(&g_agents_mutex);
    return id;
}

static void at_set_pid(int id, pid_t pid, agent_state_t state) {
    if (id < 0 || id >= MAX_AGENTS) return;
    pthread_mutex_lock(&g_agents_mutex);
    g_agents[id].pid = pid;
    g_agents[id].state = state;
    pthread_mutex_unlock(&g_agents_mutex);
}

static void at_set_fds(int id, int cmd_wfd, int stat_rfd) {
    if (id < 0 || id >= MAX_AGENTS) return;
    pthread_mutex_lock(&g_agents_mutex);
    g_agents[id].cmd_fifo_wfd = cmd_wfd;
    g_agents[id].stat_fifo_rfd = stat_rfd;
    pthread_mutex_unlock(&g_agents_mutex);
}

static void at_update_resource_sample(int id, double cpu_percent, long mem_kb) {
    if (id < 0 || id >= MAX_AGENTS) return;
    pthread_mutex_lock(&g_agents_mutex);
    g_agents[id].last_cpu_percent = cpu_percent;
    g_agents[id].last_mem_kb = mem_kb;
    pthread_mutex_unlock(&g_agents_mutex);
}

static void at_set_state(int id, agent_state_t state) {
    if (id < 0 || id >= MAX_AGENTS) return;
    pthread_mutex_lock(&g_agents_mutex);
    g_agents[id].state = state;
    pthread_mutex_unlock(&g_agents_mutex);
}

static int at_find_by_pid(pid_t pid) {
    int found = -1;
    pthread_mutex_lock(&g_agents_mutex);
    for (int i = 0; i < g_agent_count; i++) {
        if (g_agents[i].active && g_agents[i].pid == pid) { found = i; break; }
    }
    pthread_mutex_unlock(&g_agents_mutex);
    return found;
}

static void at_print(void) {
    pthread_mutex_lock(&g_agents_mutex);
    printf("\n%-4s %-10s %-8s %-12s %8s %10s %8s\n",
           "ID", "NAME", "PID", "STATE", "CPU%", "MEM(KB)", "STRIKES");
    printf("--------------------------------------------------------------\n");
    for (int i = 0; i < g_agent_count; i++) {
        agent_record_t *a = &g_agents[i];
        if (!a->active) continue;
        printf("%-4d %-10s %-8d %-12s %8.2f %10ld %8d\n",
               a->id, a->name, (int)a->pid, agent_state_str(a->state),
               a->last_cpu_percent, a->last_mem_kb, a->violation_strikes);
    }
    printf("\n");
    pthread_mutex_unlock(&g_agents_mutex);
}

/* ============================================================================
 * SECTION 9: AGENT program  (formerly agent.c) -- run_agent()
 *
 * This is what the freshly-loaded process image executes after
 * spawn_agent() (Section 11) calls fork() then execv()'s this same
 * binary with AGENT_MODE_SENTINEL as argv[0].
 * ==========================================================================*/

static volatile sig_atomic_t g_agent_shutdown_requested = 0;
static int  g_agent_id;
static char g_agent_name[MAX_NAME_LEN];
static int  g_agent_cmd_rfd = -1;
static int  g_agent_stat_wfd = -1;

static void agent_on_sigterm(int signo) {
    (void)signo;
    g_agent_shutdown_requested = 1; /* async-signal-safe: flag only */
}

static void agent_send_status(agent_state_t state, double cpu, long mem, const char *detail) {
    char line[MAX_MSG];
    snprintf(line, sizeof(line), "%s|%d|%s|%.2f|%ld|%s",
             MSG_STAT_PREFIX, (int)getpid(), agent_state_str(state), cpu, mem, detail);
    if (g_agent_stat_wfd >= 0) ipc_send_line(g_agent_stat_wfd, line);
}

/* Appends one line to a file SHARED by every agent -- the genuine point
 * of contention that exercises flock() and backup_before_write() for
 * real, rather than against files that never collide. */
static void agent_append_shared_summary(const char *workspace_root, const char *line) {
    char requested[MAX_PATH_LEN], resolved[MAX_PATH_LEN];
    snprintf(requested, sizeof(requested), "%s/output/shared_summary.txt", workspace_root);

    if (file_guard_validate(requested, resolved) != 0) {
        logger_log(LOG_VIOLATION, g_agent_name, "shared summary path rejected by file guard");
        return;
    }

    int lock_fd = sync_lock_acquire(resolved);
    if (lock_fd < 0) {
        logger_log(LOG_ERROR, g_agent_name, "could not acquire shared-summary lock");
        return;
    }

    logger_log(LOG_INFO, g_agent_name, "acquired shared-summary lock (cross-process flock)");
    backup_before_write(resolved);

    int fd = open(resolved, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd >= 0) {
        ssize_t len = (ssize_t)strlen(line);
        ssize_t off = 0;
        while (off < len) {
            ssize_t w = write(fd, line + off, (size_t)(len - off));
            if (w == -1) { if (errno == EINTR) continue; break; }
            off += w;
        }
        close(fd);
    } else {
        logger_log(LOG_ERROR, g_agent_name, "cannot open shared summary %s: %s", resolved, strerror(errno));
    }

    sync_lock_release(lock_fd);
    logger_log(LOG_INFO, g_agent_name, "released shared-summary lock");
}

/* Reads the task file, counts lines/words/chars, writes a per-agent
 * result file plus a line in the shared summary. */
static int agent_run_bounded_task(const char *workspace_root, const char *task_relpath) {
    char resolved_in[MAX_PATH_LEN];
    char requested_in[MAX_PATH_LEN];
    snprintf(requested_in, sizeof(requested_in), "%s/tasks/%s", workspace_root, task_relpath);

    if (file_guard_validate(requested_in, resolved_in) != 0) {
        logger_log(LOG_VIOLATION, g_agent_name, "task file '%s' rejected by file guard", requested_in);
        agent_send_status(AGENT_STATE_CRASHED, 0, 0, "file-guard-reject");
        return -1;
    }

    int in_fd = open(resolved_in, O_RDONLY);
    if (in_fd == -1) {
        logger_log(LOG_ERROR, g_agent_name, "cannot open task file %s: %s", resolved_in, strerror(errno));
        agent_send_status(AGENT_STATE_CRASHED, 0, 0, "open-failed");
        return -1;
    }

    char buf[4096];
    long chars = 0, lines = 0, words = 0;
    int in_word = 0;
    ssize_t r;
    while ((r = read(in_fd, buf, sizeof(buf))) > 0) {
        for (ssize_t i = 0; i < r; i++) {
            chars++;
            if (buf[i] == '\n') lines++;
            if (buf[i] == ' ' || buf[i] == '\n' || buf[i] == '\t') {
                in_word = 0;
            } else if (!in_word) {
                in_word = 1;
                words++;
            }
        }
    }
    close(in_fd);

    sleep(1); /* simulate real processing time */

    char requested_out[MAX_PATH_LEN];
    char resolved_out[MAX_PATH_LEN];
    snprintf(requested_out, sizeof(requested_out), "%s/output/%s.result", workspace_root, g_agent_name);

    if (file_guard_validate(requested_out, resolved_out) != 0) {
        logger_log(LOG_VIOLATION, g_agent_name, "output path '%s' rejected by file guard", requested_out);
        agent_send_status(AGENT_STATE_CRASHED, 0, 0, "file-guard-reject-output");
        return -1;
    }

    int lock_fd = sync_lock_acquire(resolved_out);
    if (lock_fd < 0) {
        logger_log(LOG_ERROR, g_agent_name, "could not acquire sync lock on %s", resolved_out);
        return -1;
    }

    if (backup_before_write(resolved_out) != 0) {
        logger_log(LOG_WARN, g_agent_name, "backup_before_write failed for %s (continuing)", resolved_out);
    }

    int out_fd = open(resolved_out, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd == -1) {
        logger_log(LOG_ERROR, g_agent_name, "cannot open output file %s: %s", resolved_out, strerror(errno));
        sync_lock_release(lock_fd);
        return -1;
    }

    char result[256];
    int n = snprintf(result, sizeof(result),
                      "agent=%s pid=%d chars=%ld lines=%ld words=%ld\n",
                      g_agent_name, (int)getpid(), chars, lines, words);
    ssize_t off = 0;
    while (off < n) {
        ssize_t w = write(out_fd, result + off, (size_t)(n - off));
        if (w == -1) { if (errno == EINTR) continue; break; }
        off += w;
    }
    close(out_fd);
    sync_lock_release(lock_fd);

    logger_log(LOG_INFO, g_agent_name, "task complete: %s", result);
    agent_append_shared_summary(workspace_root, result);
    agent_send_status(AGENT_STATE_WAITING, 0, 0, "task-complete");
    return 0;
}

/* Deliberately uncontrolled CPU-bound workload used to demonstrate the
 * manager's SIGSTOP -> SIGTERM -> SIGKILL enforcement ladder. */
static void agent_run_spin_task(void) {
    logger_log(LOG_WARN, g_agent_name, "entering SPIN mode (intentional runaway workload for demo)");
    agent_send_status(AGENT_STATE_RUNNING, 0, 0, "spin-mode");
    volatile unsigned long long counter = 0;
    while (!g_agent_shutdown_requested) {
        counter++;
        if (counter % 200000000ULL == 0) {
            agent_send_status(AGENT_STATE_RUNNING, 0, 0, "spin-mode-alive");
        }
    }
}

static int run_agent(int argc, char *argv[]) {
    if (argc < ARG_COUNT) {
        fprintf(stderr, "agent: wrong argument count (%d), expected %d\n", argc, ARG_COUNT);
        return 1;
    }

    g_agent_id = atoi(argv[ARG_ID]);
    snprintf(g_agent_name, sizeof(g_agent_name), "%s", argv[ARG_NAME]);
    const char *cmd_fifo   = argv[ARG_CMD_FIFO];
    const char *stat_fifo  = argv[ARG_STAT_FIFO];
    const char *workspace_root = argv[ARG_WORKSPACE_ROOT];
    const char *task_file  = argv[ARG_TASK_FILE];

    logger_init(LOG_FILE_PATH);
    if (file_guard_init(workspace_root) != 0) {
        logger_log(LOG_ERROR, g_agent_name, "file_guard_init failed for %s", workspace_root);
        return 1;
    }
    backup_init(DIR_BACKUPS);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = agent_on_sigterm;
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    logger_log(LOG_INFO, g_agent_name, "agent starting, pid=%d, task=%s", (int)getpid(), task_file);

    /* Rendezvous with the manager. Opening order (cmd fifo then stat
     * fifo) must mirror spawn_agent() in Section 11 exactly. */
    g_agent_cmd_rfd = ipc_open(cmd_fifo, O_RDONLY);
    if (g_agent_cmd_rfd == -1) {
        logger_log(LOG_ERROR, g_agent_name, "failed to open cmd fifo %s: %s", cmd_fifo, strerror(errno));
        return 1;
    }
    g_agent_stat_wfd = ipc_open(stat_fifo, O_WRONLY);
    if (g_agent_stat_wfd == -1) {
        logger_log(LOG_ERROR, g_agent_name, "failed to open stat fifo %s: %s", stat_fifo, strerror(errno));
        return 1;
    }

    agent_send_status(AGENT_STATE_RUNNING, 0, 0, "started");

    char requested_in[MAX_PATH_LEN], resolved_in[MAX_PATH_LEN];
    snprintf(requested_in, sizeof(requested_in), "%s/tasks/%s", workspace_root, task_file);
    int spin_mode = 0;
    if (file_guard_validate(requested_in, resolved_in) == 0) {
        FILE *f = fopen(resolved_in, "r");
        if (f) {
            char first_line[64] = {0};
            if (fgets(first_line, sizeof(first_line), f)) {
                if (strncmp(first_line, "MODE=SPIN", 9) == 0) spin_mode = 1;
            }
            fclose(f);
        }
    }

    if (spin_mode) {
        agent_run_spin_task();
    } else {
        agent_run_bounded_task(workspace_root, task_file);
    }

    int flags = fcntl(g_agent_cmd_rfd, F_GETFL, 0);
    fcntl(g_agent_cmd_rfd, F_SETFL, flags | O_NONBLOCK);

    while (!g_agent_shutdown_requested) {
        char line[MAX_MSG];
        int n = ipc_recv_line(g_agent_cmd_rfd, line, sizeof(line));
        if (n > 0) {
            logger_log(LOG_INFO, g_agent_name, "received command: %s", line);
            if (strstr(line, CMD_SHUTDOWN)) {
                g_agent_shutdown_requested = 1;
                break;
            } else if (strstr(line, CMD_TASK)) {
                char *last_bar = strrchr(line, '|');
                if (last_bar && *(last_bar + 1) != '\0') {
                    agent_send_status(AGENT_STATE_RUNNING, 0, 0, "new-task-received");
                    agent_run_bounded_task(workspace_root, last_bar + 1);
                }
            }
        } else if (n == 0) {
            break; /* manager closed its write end */
        }
        struct timespec ts = {0, 200 * 1000 * 1000};
        nanosleep(&ts, NULL);
    }

    logger_log(LOG_INFO, g_agent_name, "shutting down cleanly, pid=%d", (int)getpid());
    agent_send_status(AGENT_STATE_TERMINATED, 0, 0, "bye");

    if (g_agent_cmd_rfd >= 0) close(g_agent_cmd_rfd);
    if (g_agent_stat_wfd >= 0) close(g_agent_stat_wfd);
    logger_close();
    return 0;
}

/* ============================================================================
 * SECTION 10: MANAGER globals + filesystem setup + config parsing
 * ==========================================================================*/

static volatile sig_atomic_t g_mgr_shutdown_requested = 0;
static volatile sig_atomic_t g_alarm_fired = 0;
static resource_monitor_t g_mon[MAX_AGENTS];
static char g_task_files[MAX_AGENTS][MAX_NAME_LEN];
static char g_self_exe_path[MAX_PATH_LEN]; /* this binary's own absolute path */

static void mgr_on_sigint_term(int signo) {
    (void)signo;
    g_mgr_shutdown_requested = 1;
}

static void mgr_on_alarm(int signo) {
    (void)signo;
    g_alarm_fired = 1;
}

static void ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        if (mkdir(path, 0775) != 0 && errno != EEXIST) {
            fprintf(stderr, "manager: failed to create directory %s: %s\n", path, strerror(errno));
        }
    }
}

static void ensure_runtime_dirs(void) {
    ensure_dir(DIR_RUNTIME);
    ensure_dir(DIR_WORKSPACE);
    ensure_dir(DIR_WORKSPACE "/tasks");
    ensure_dir(DIR_WORKSPACE "/output");
    ensure_dir(DIR_BACKUPS);
    ensure_dir(DIR_LOGS);
    ensure_dir(DIR_IPC);
    ensure_dir("config");
}

/* Writes `content` to `path` only if nothing is there yet -- never
 * overwrites a file the user has already created or edited. Used so
 * that this single executable is genuinely self-contained: someone who
 * downloads only the binary, with none of the project's config/ or
 * runtime/workspace/tasks/ files, still gets a working default setup
 * the first time they run it. */
static void write_file_if_missing(const char *path, const char *content) {
    struct stat st;
    if (stat(path, &st) == 0) return; /* already exists - leave it alone */

    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) return; /* created by someone else in a race, or a real error either way */

    size_t len = strlen(content);
    size_t off = 0;
    while (off < len) {
        ssize_t w = write(fd, content + off, len - off);
        if (w == -1) { if (errno == EINTR) continue; break; }
        off += (size_t)w;
    }
    close(fd);
}

/* Bootstraps a default 3-agent config and matching sample task files
 * (identical to the ones this project ships with) so `./ossp_system`
 * with no other files present in the working directory just works. */
static void ensure_self_contained_defaults(void) {
    write_file_if_missing(CONFIG_FILE_DEFAULT,
        "# Multi-Agent AI Execution and Resource Management System\n"
        "# Agent configuration file (auto-created default -- edit freely)\n"
        "#\n"
        "# Format (one agent per line, '#' starts a comment):\n"
        "#   name,cpu_limit_percent,mem_limit_kb,task_file\n"
        "#\n"
        "# task_file is relative to runtime/workspace/tasks/\n"
        "\n"
        "agent0,40.0,32768,task_alpha.txt\n"
        "agent1,25.0,32768,task_beta.txt\n"
        "agent2,80.0,65536,task_gamma.txt\n");

    write_file_if_missing(DIR_WORKSPACE "/tasks/task_alpha.txt",
        "The manager process creates each agent using fork and exec.\n"
        "Named pipes are used so unrelated processes can still talk to each other.\n"
        "This is a bounded task: read this file, count words and lines, write a result.\n");

    write_file_if_missing(DIR_WORKSPACE "/tasks/task_beta.txt",
        "File access is validated before any operation is allowed to proceed.\n"
        "A restricted workspace keeps every agent inside its sandbox.\n"
        "Backups are taken automatically before a shared file is overwritten.\n"
        "Synchronization prevents two agents from corrupting the same file.\n");

    write_file_if_missing(DIR_WORKSPACE "/tasks/task_gamma.txt",
        "MODE=SPIN\n"
        "This task deliberately never finishes: it is used to demonstrate the\n"
        "manager's resource monitoring and signal-based enforcement escalating\n"
        "from SIGSTOP to SIGTERM to SIGKILL against a runaway agent.\n");
}

/* Resolves the absolute path of the currently running binary via
 * /proc/self/exe (Linux-specific, always available in this sandbox),
 * falling back to realpath(argv[0]) if that ever fails. This is what
 * lets spawn_agent() re-exec "itself" regardless of the relative path
 * the user typed to launch the manager. */
static int resolve_self_exe(const char *argv0) {
    ssize_t len = readlink("/proc/self/exe", g_self_exe_path, sizeof(g_self_exe_path) - 1);
    if (len > 0) {
        g_self_exe_path[len] = '\0';
        return 0;
    }
    if (realpath(argv0, g_self_exe_path) != NULL) return 0;
    return -1;
}

/* Config format: name,cpu_limit_percent,mem_limit_kb,task_file */
static int load_config(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "manager: cannot open config file '%s': %s\n", path, strerror(errno));
        return -1;
    }

    char line[MAX_LINE];
    int loaded = 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || *p == '\n' || *p == '\0') continue;

        char name[MAX_NAME_LEN];
        double cpu_limit;
        long mem_limit;
        char task_file[MAX_NAME_LEN];

        char *tok = strtok(p, ",\n");
        if (!tok) continue;
        snprintf(name, sizeof(name), "%s", tok);

        tok = strtok(NULL, ",\n");
        if (!tok) continue;
        cpu_limit = atof(tok);

        tok = strtok(NULL, ",\n");
        if (!tok) continue;
        mem_limit = atol(tok);

        tok = strtok(NULL, ",\n");
        if (!tok) continue;
        snprintf(task_file, sizeof(task_file), "%s", tok);

        char cmd_fifo[MAX_PATH_LEN], stat_fifo[MAX_PATH_LEN];
        int id_guess = g_agent_count;
        ipc_build_paths(id_guess, cmd_fifo, stat_fifo);

        int id = at_register(name, cpu_limit, mem_limit, cmd_fifo, stat_fifo);
        if (id < 0) {
            fprintf(stderr, "manager: too many agents in config (max %d)\n", MAX_AGENTS);
            break;
        }
        snprintf(g_task_files[id], MAX_NAME_LEN, "%s", task_file);
        loaded++;
    }
    fclose(f);
    return loaded;
}

/* ============================================================================
 * SECTION 11: MANAGER process creation -- fork() + execv(self)
 * ==========================================================================*/

static int spawn_agent(int id) {
    agent_record_t *a = &g_agents[id];

    if (ipc_create_fifo(a->cmd_fifo_path) != 0) return -1;
    if (ipc_create_fifo(a->stat_fifo_path) != 0) return -1;

    pid_t pid = fork();
    if (pid == -1) {
        logger_log(LOG_ERROR, "manager", "fork() failed for agent %s: %s", a->name, strerror(errno));
        return -1;
    }

    if (pid == 0) {
        /* Child: replace this process image with a fresh copy of the
         * SAME binary, but pass AGENT_MODE_SENTINEL as argv[0] so that
         * this program's own top-level main() (Section 12) dispatches
         * into run_agent() instead of run_manager(). This is still a
         * genuine execv() -- new memory image, no manager state carried
         * over except open file descriptors, exactly like exec()-ing a
         * separate agent binary would behave. */
        char id_str[16], cpu_str[32], mem_str[32];
        snprintf(id_str, sizeof(id_str), "%d", id);
        snprintf(cpu_str, sizeof(cpu_str), "%.2f", a->cpu_limit_percent);
        snprintf(mem_str, sizeof(mem_str), "%ld", a->mem_limit_kb);

        char *argv[ARG_COUNT + 1];
        argv[ARG_PROG]           = (char *)AGENT_MODE_SENTINEL;
        argv[ARG_ID]             = id_str;
        argv[ARG_NAME]           = a->name;
        argv[ARG_CMD_FIFO]       = a->cmd_fifo_path;
        argv[ARG_STAT_FIFO]      = a->stat_fifo_path;
        argv[ARG_WORKSPACE_ROOT] = (char *)DIR_WORKSPACE;
        argv[ARG_TASK_FILE]      = g_task_files[id];
        argv[ARG_CPU_LIMIT]      = cpu_str;
        argv[ARG_MEM_LIMIT]      = mem_str;
        argv[ARG_COUNT]          = NULL;

        execv(g_self_exe_path, argv);
        fprintf(stderr, "agent %s: execv(%s) failed: %s\n", a->name, g_self_exe_path, strerror(errno));
        _exit(127);
    }

    /* Parent (manager): rendezvous with the new agent over its FIFOs. */
    struct sigaction sa_alarm, sa_old;
    memset(&sa_alarm, 0, sizeof(sa_alarm));
    sa_alarm.sa_handler = mgr_on_alarm;
    sigaction(SIGALRM, &sa_alarm, &sa_old);

    g_alarm_fired = 0;
    alarm(5);
    int cmd_wfd = open(a->cmd_fifo_path, O_WRONLY);
    alarm(0);
    if (cmd_wfd == -1) {
        logger_log(LOG_ERROR, "manager", "timed out/failed connecting cmd fifo for agent %s (%s)",
                   a->name, strerror(errno));
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        sigaction(SIGALRM, &sa_old, NULL);
        return -1;
    }

    g_alarm_fired = 0;
    alarm(5);
    int stat_rfd = open(a->stat_fifo_path, O_RDONLY);
    alarm(0);
    sigaction(SIGALRM, &sa_old, NULL);
    if (stat_rfd == -1) {
        logger_log(LOG_ERROR, "manager", "timed out/failed connecting stat fifo for agent %s (%s)",
                   a->name, strerror(errno));
        close(cmd_wfd);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        return -1;
    }

    int flags = fcntl(stat_rfd, F_GETFL, 0);
    fcntl(stat_rfd, F_SETFL, flags | O_NONBLOCK);

    at_set_pid(id, pid, AGENT_STATE_RUNNING);
    at_set_fds(id, cmd_wfd, stat_rfd);
    resource_monitor_init(&g_mon[id], pid);

    logger_log(LOG_INFO, "manager", "spawned agent '%s' pid=%d cpu_limit=%.1f%% mem_limit=%ldKB task=%s",
               a->name, (int)pid, a->cpu_limit_percent, a->mem_limit_kb, g_task_files[id]);
    return 0;
}

static void reap_finished_children(void) {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        int id = at_find_by_pid(pid);
        if (id < 0) continue;

        if (WIFEXITED(status)) {
            logger_log(LOG_INFO, "manager", "agent '%s' (pid=%d) exited, code=%d",
                       g_agents[id].name, (int)pid, WEXITSTATUS(status));
            at_set_state(id, AGENT_STATE_TERMINATED);
        } else if (WIFSIGNALED(status)) {
            logger_log(LOG_WARN, "manager", "agent '%s' (pid=%d) killed by signal %d",
                       g_agents[id].name, (int)pid, WTERMSIG(status));
            at_set_state(id, AGENT_STATE_CRASHED);
        }
        at_set_pid(id, -1, g_agents[id].state);
    }
}

/* ============================================================================
 * SECTION 12: MANAGER threads -- resource monitor, IPC listener
 * ==========================================================================*/

static void *monitor_thread_fn(void *arg) {
    (void)arg;
    while (!g_mgr_shutdown_requested) {
        reap_finished_children();

        pthread_mutex_lock(&g_agents_mutex);
        int count = g_agent_count;
        agent_record_t snapshot[MAX_AGENTS];
        memcpy(snapshot, g_agents, sizeof(agent_record_t) * count);
        pthread_mutex_unlock(&g_agents_mutex);

        for (int i = 0; i < count; i++) {
            agent_record_t *a = &snapshot[i];
            if (!a->active || a->pid <= 0) continue;
            if (a->state == AGENT_STATE_TERMINATED || a->state == AGENT_STATE_CRASHED) continue;

            if (a->state == AGENT_STATE_VIOLATION) {
                pthread_mutex_lock(&g_agents_mutex);
                g_agents[i].violation_strikes++;
                int strikes = g_agents[i].violation_strikes;
                pthread_mutex_unlock(&g_agents_mutex);

                if (strikes == 2) {
                    logger_log(LOG_VIOLATION, "manager",
                        "agent '%s' (pid=%d) still over limit after SIGSTOP -> escalating to SIGTERM",
                        a->name, (int)a->pid);
                    kill(a->pid, SIGCONT);
                    kill(a->pid, SIGTERM);
                } else if (strikes >= 4) {
                    logger_log(LOG_VIOLATION, "manager",
                        "agent '%s' (pid=%d) unresponsive to SIGTERM -> escalating to SIGKILL",
                        a->name, (int)a->pid);
                    kill(a->pid, SIGKILL);
                }
                continue;
            }

            double cpu_pct = 0.0;
            long mem_kb = 0;
            if (resource_monitor_sample(&g_mon[i], &cpu_pct, &mem_kb) != 0) continue;
            at_update_resource_sample(i, cpu_pct, mem_kb);

            if (cpu_pct > a->cpu_limit_percent || mem_kb > a->mem_limit_kb) {
                logger_log(LOG_VIOLATION, "manager",
                    "agent '%s' (pid=%d) exceeded limits (cpu=%.1f%%/%.1f%% mem=%ldKB/%ldKB) -> SIGSTOP",
                    a->name, (int)a->pid, cpu_pct, a->cpu_limit_percent, mem_kb, a->mem_limit_kb);
                kill(a->pid, SIGSTOP);
                pthread_mutex_lock(&g_agents_mutex);
                g_agents[i].state = AGENT_STATE_VIOLATION;
                g_agents[i].violation_strikes = 1;
                pthread_mutex_unlock(&g_agents_mutex);
            } else {
                logger_log(LOG_INFO, "manager", "agent '%s' sample: cpu=%.1f%% mem=%ldKB",
                           a->name, cpu_pct, mem_kb);
            }
        }

        for (int slept_ms = 0; slept_ms < MONITOR_INTERVAL_SEC * 1000 && !g_mgr_shutdown_requested; slept_ms += 200) {
            struct timespec ts = {0, 200L * 1000 * 1000};
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

static agent_state_t parse_state(const char *s) {
    for (int i = 0; i <= AGENT_STATE_CRASHED; i++) {
        if (strcmp(agent_state_str((agent_state_t)i), s) == 0) return (agent_state_t)i;
    }
    return AGENT_STATE_RUNNING;
}

static void handle_stat_line(int id, const char *line) {
    char copy[MAX_MSG];
    snprintf(copy, sizeof(copy), "%s", line);

    char *tok = strtok(copy, "|");
    if (!tok || strcmp(tok, MSG_STAT_PREFIX) != 0) return;

    tok = strtok(NULL, "|"); if (!tok) return;
    char *state_str = strtok(NULL, "|"); if (!state_str) return;
    char *cpu_str   = strtok(NULL, "|");
    char *mem_str   = strtok(NULL, "|");
    char *detail    = strtok(NULL, "|");

    agent_state_t state = parse_state(state_str);
    double cpu = cpu_str ? atof(cpu_str) : 0.0;
    long mem = mem_str ? atol(mem_str) : 0;

    pthread_mutex_lock(&g_agents_mutex);
    if (g_agents[id].state != AGENT_STATE_VIOLATION) {
        g_agents[id].state = state;
    }
    pthread_mutex_unlock(&g_agents_mutex);

    logger_log(LOG_INFO, "manager", "agent '%s' status: %s (%s)",
               g_agents[id].name, state_str, detail ? detail : "-");
    (void)cpu; (void)mem;
}

static void *ipc_listener_thread_fn(void *arg) {
    (void)arg;
    while (!g_mgr_shutdown_requested) {
        pthread_mutex_lock(&g_agents_mutex);
        int capacity = g_agent_count;
        pthread_mutex_unlock(&g_agents_mutex);

        if (capacity == 0) { usleep(200 * 1000); continue; }

        struct pollfd *pfds = malloc(sizeof(struct pollfd) * (size_t)capacity);
        int *ids = malloc(sizeof(int) * (size_t)capacity);
        if (!pfds || !ids) {
            logger_log(LOG_ERROR, "manager", "ipc_listener: out of memory allocating poll arrays");
            free(pfds); free(ids);
            usleep(200 * 1000);
            continue;
        }
        int nfds = 0;

        pthread_mutex_lock(&g_agents_mutex);
        for (int i = 0; i < g_agent_count; i++) {
            if (g_agents[i].active && g_agents[i].stat_fifo_rfd >= 0) {
                pfds[nfds].fd = g_agents[i].stat_fifo_rfd;
                pfds[nfds].events = POLLIN;
                pfds[nfds].revents = 0;
                ids[nfds] = i;
                nfds++;
            }
        }
        pthread_mutex_unlock(&g_agents_mutex);

        if (nfds == 0) { free(pfds); free(ids); usleep(200 * 1000); continue; }

        int ready = poll(pfds, nfds, 500);
        if (ready <= 0) { free(pfds); free(ids); continue; }

        for (int i = 0; i < nfds; i++) {
            if (pfds[i].revents & POLLIN) {
                char line[MAX_MSG];
                int n = ipc_recv_line(pfds[i].fd, line, sizeof(line));
                if (n > 0) handle_stat_line(ids[i], line);
            }
        }

        free(pfds);
        free(ids);
    }
    return NULL;
}

/* ============================================================================
 * SECTION 13: MANAGER shutdown sequence + command shell + run_manager()
 * ==========================================================================*/

static void shutdown_sequence(void) {
    logger_log(LOG_INFO, "manager", "shutdown sequence initiated");
    printf("\n[manager] shutting down: notifying all agents...\n");

    pthread_mutex_lock(&g_agents_mutex);
    int count = g_agent_count;
    agent_record_t snapshot[MAX_AGENTS];
    memcpy(snapshot, g_agents, sizeof(agent_record_t) * count);
    pthread_mutex_unlock(&g_agents_mutex);

    for (int i = 0; i < count; i++) {
        if (!snapshot[i].active) continue;
        if (snapshot[i].pid > 0 && snapshot[i].cmd_fifo_wfd >= 0) {
            char line[MAX_MSG];
            snprintf(line, sizeof(line), "%s|%s|-", MSG_CMD_PREFIX, CMD_SHUTDOWN);
            kill(snapshot[i].pid, SIGCONT);
            ipc_send_line(snapshot[i].cmd_fifo_wfd, line);
        }
    }

    time_t deadline = time(NULL) + GRACEFUL_SHUTDOWN_TIMEOUT_SEC;
    int remaining;
    do {
        reap_finished_children();
        remaining = 0;
        pthread_mutex_lock(&g_agents_mutex);
        for (int i = 0; i < g_agent_count; i++) {
            if (g_agents[i].active && g_agents[i].pid > 0) remaining++;
        }
        pthread_mutex_unlock(&g_agents_mutex);
        if (remaining > 0) usleep(200 * 1000);
    } while (remaining > 0 && time(NULL) < deadline);

    pthread_mutex_lock(&g_agents_mutex);
    for (int i = 0; i < g_agent_count; i++) {
        if (g_agents[i].active && g_agents[i].pid > 0) {
            logger_log(LOG_WARN, "manager", "agent '%s' (pid=%d) did not exit gracefully -> SIGKILL",
                       g_agents[i].name, (int)g_agents[i].pid);
            kill(g_agents[i].pid, SIGKILL);
        }
    }
    pthread_mutex_unlock(&g_agents_mutex);
    reap_finished_children();
    usleep(300 * 1000);
    reap_finished_children();

    pthread_mutex_lock(&g_agents_mutex);
    for (int i = 0; i < g_agent_count; i++) {
        if (!g_agents[i].active) continue;
        if (g_agents[i].cmd_fifo_wfd >= 0) close(g_agents[i].cmd_fifo_wfd);
        if (g_agents[i].stat_fifo_rfd >= 0) close(g_agents[i].stat_fifo_rfd);
        ipc_remove_fifo(g_agents[i].cmd_fifo_path);
        ipc_remove_fifo(g_agents[i].stat_fifo_path);
    }
    pthread_mutex_unlock(&g_agents_mutex);

    logger_log(LOG_INFO, "manager", "shutdown complete");
    printf("[manager] shutdown complete.\n");
    logger_close();
}

static void print_help(void) {
    printf(
        "Commands:\n"
        "  list                 show the agent table\n"
        "  task <id> <file>     assign a new task file (in runtime/workspace/tasks/)\n"
        "  pause <id>           SIGSTOP an agent\n"
        "  resume <id>          SIGCONT a paused agent\n"
        "  kill <id>            SIGTERM an agent immediately (operator override)\n"
        "  shutdown | quit      graceful shutdown of the whole system\n"
        "  help                 show this message\n");
}

static void handle_command_line(char *line) {
    char *cmd = strtok(line, " \t\r\n");
    if (!cmd) return;

    if (strcmp(cmd, "help") == 0) {
        print_help();
    } else if (strcmp(cmd, "list") == 0 || strcmp(cmd, "status") == 0) {
        at_print();
    } else if (strcmp(cmd, "task") == 0) {
        char *id_str = strtok(NULL, " \t\r\n");
        char *file = strtok(NULL, " \t\r\n");
        if (!id_str || !file) { printf("usage: task <id> <file>\n"); return; }
        int id = atoi(id_str);
        if (id < 0 || id >= g_agent_count || !g_agents[id].active || g_agents[id].pid <= 0) {
            printf("no such running agent %d\n", id); return;
        }
        char msg[MAX_MSG];
        snprintf(msg, sizeof(msg), "%s|%s|%s", MSG_CMD_PREFIX, CMD_TASK, file);
        ipc_send_line(g_agents[id].cmd_fifo_wfd, msg);
        logger_log(LOG_INFO, "manager", "assigned new task '%s' to agent '%s'", file, g_agents[id].name);
    } else if (strcmp(cmd, "pause") == 0 || strcmp(cmd, "resume") == 0) {
        char *id_str = strtok(NULL, " \t\r\n");
        if (!id_str) { printf("usage: %s <id>\n", cmd); return; }
        int id = atoi(id_str);
        if (id < 0 || id >= g_agent_count || !g_agents[id].active || g_agents[id].pid <= 0) {
            printf("no such running agent %d\n", id); return;
        }
        if (strcmp(cmd, "pause") == 0) {
            kill(g_agents[id].pid, SIGSTOP);
            at_set_state(id, AGENT_STATE_WAITING);
            logger_log(LOG_INFO, "manager", "operator paused agent '%s' (SIGSTOP)", g_agents[id].name);
        } else {
            kill(g_agents[id].pid, SIGCONT);
            at_set_state(id, AGENT_STATE_RUNNING);
            logger_log(LOG_INFO, "manager", "operator resumed agent '%s' (SIGCONT)", g_agents[id].name);
        }
    } else if (strcmp(cmd, "kill") == 0) {
        char *id_str = strtok(NULL, " \t\r\n");
        if (!id_str) { printf("usage: kill <id>\n"); return; }
        int id = atoi(id_str);
        if (id < 0 || id >= g_agent_count || !g_agents[id].active || g_agents[id].pid <= 0) {
            printf("no such running agent %d\n", id); return;
        }
        kill(g_agents[id].pid, SIGCONT);
        kill(g_agents[id].pid, SIGTERM);
        logger_log(LOG_INFO, "manager", "operator killed agent '%s' (SIGTERM)", g_agents[id].name);
    } else if (strcmp(cmd, "shutdown") == 0 || strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
        g_mgr_shutdown_requested = 1;
    } else {
        printf("unknown command '%s' (try 'help')\n", cmd);
    }
}

static int run_manager(int argc, char *argv[]) {
    const char *config_path = (argc > 1) ? argv[1] : CONFIG_FILE_DEFAULT;

    if (resolve_self_exe(argv[0]) != 0) {
        fprintf(stderr, "manager: could not resolve this binary's own path (needed to launch agents)\n");
        return 1;
    }

    ensure_runtime_dirs();

    /* Only auto-bootstrap defaults when the caller didn't name a specific
     * config file -- if they explicitly pointed at one and it's missing
     * or misspelled, that should still fail loudly, not silently swap in
     * unrelated sample data. */
    if (argc <= 1) {
        ensure_self_contained_defaults();
    }

    if (logger_init(LOG_FILE_PATH) != 0) {
        fprintf(stderr, "manager: could not open log file %s\n", LOG_FILE_PATH);
        return 1;
    }
    logger_log(LOG_INFO, "manager", "=== Multi-Agent AI Execution and Resource Management System starting ===");
    logger_log(LOG_INFO, "manager", "self exe path: %s", g_self_exe_path);

    at_init();
    int loaded = load_config(config_path);
    if (loaded <= 0) {
        fprintf(stderr, "manager: no agents loaded from '%s'\n", config_path);
        logger_close();
        return 1;
    }
    printf("[manager] loaded %d agent(s) from %s\n", loaded, config_path);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = mgr_on_sigint_term;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    for (int i = 0; i < g_agent_count; i++) {
        if (spawn_agent(i) != 0) {
            logger_log(LOG_ERROR, "manager", "failed to spawn agent id=%d ('%s')", i, g_agents[i].name);
        }
    }

    pthread_t mon_tid, ipc_tid;
    pthread_create(&mon_tid, NULL, monitor_thread_fn, NULL);
    pthread_create(&ipc_tid, NULL, ipc_listener_thread_fn, NULL);

    printf("[manager] all agents spawned. Type 'help' for commands.\n");
    at_print();

    char line[MAX_LINE];
    while (!g_mgr_shutdown_requested) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        struct timeval tv = {0, 500 * 1000};

        printf("ossp> ");
        fflush(stdout);

        int rv = select(STDIN_FILENO + 1, &rfds, NULL, NULL, &tv);
        if (rv < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (rv == 0) continue;

        if (!fgets(line, sizeof(line), stdin)) {
            g_mgr_shutdown_requested = 1;
            break;
        }
        handle_command_line(line);
    }

    shutdown_sequence();

    pthread_join(mon_tid, NULL);
    pthread_join(ipc_tid, NULL);

    return 0;
}

/* ============================================================================
 * SECTION 14: top-level main() -- dispatches manager vs. agent role
 * ==========================================================================*/

int main(int argc, char *argv[]) {
    if (argc >= 1 && strcmp(argv[0], AGENT_MODE_SENTINEL) == 0) {
        return run_agent(argc, argv);
    }
    return run_manager(argc, argv);
}
