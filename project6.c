/* ================================================================
   MULTI-AGENT AI EXECUTION AND TESTING PIPELINE  --  v4

   Four cooperating agent processes (Code Generator, Code Review,
   Execution, Testing) talk to each other over named pipes to take a
   small C program from "idea" to "verified working output":

       Generator -> Reviewer -> Executor -> Tester

   v2 added, on top of the original demo:
     - The Code Generator actually generates one of several small
       programs (your choice from a menu), instead of always
       assuming a fixed factorial.c already exists.
     - The Code Review Agent runs a real static check on the
       generated source (banned functions, brace balance) BEFORE
       anything is compiled - review now gates execution, the way a
       real CI pipeline would, rather than happening after the fact.
     - Every FIFO open gives up after a timeout instead of blocking
       forever if a peer agent has already crashed.
     - Ctrl+C during a pipeline run signals every agent to stop
       instead of leaving orphaned children behind.
     - A "Reset Environment" menu option wipes generated files and
       rebuilds a clean workspace.

   v3 adds, on top of v2:
     - A 6th catalog option lets you paste in your own C program
       instead of picking a canned demo - the whole pipeline (review,
       compile, execute, test) runs on it exactly like any other.
     - Running the compiled program now has a hard timeout, so a
       custom program with an infinite loop gets killed instead of
       hanging the entire pipeline forever.
     - A compile failure captures gcc's real stderr output and shows
       the actual diagnostics, instead of just "Compilation failed".
     - Any Execution Agent failure (bad file, sync failure, compiler
       error, timeout) now reports a specific reason to the Testing
       Agent instead of exiting silently and leaving it to time out.
     - A persistent, timestamped run history (with a pass/fail tally)
       is kept across runs and viewable from the menu.

   v4 adds, on top of v3:
     - Resource limits and the execution timeout are now read from
       "pipeline.conf" at startup (auto-created with the current
       defaults) instead of being fixed at compile time.
     - An flock()-based single-instance lock stops two copies of the
       program from running in the same workspace at once and
       corrupting each other's FIFOs and files.
     - "Run All Demos" runs every catalog program back to back and
       prints one consolidated pass/fail table.
     - A "--run <1-5>" command-line mode runs one catalog program
       non-interactively and exits with 0 (pass) or 1 (anything else),
       for use from a script or grading harness; "--help" shows usage.
     - Fixed a real bug this rewrite surfaced: when stdout isn't a
       terminal (piped, redirected to a file, or run via --run in a
       script), libc fully-buffers it, so a lot of not-yet-flushed
       parent output was sitting in the buffer at each fork() call and
       getting duplicated whenever the resulting agent process later
       called exit(). Fixed at the root with a forced line-buffered
       stdout plus an explicit fflush() right before every agent fork.
   ================================================================ */

#include <stdio.h>
#include <unistd.h>          // fork(), access(), read(), write(), sleep(), execl()
#include <sys/types.h>       // pid_t
#include <sys/wait.h>        // waitpid(), WIFEXITED(), WEXITSTATUS()
#include <sys/stat.h>        // mkdir(), mkfifo(), stat()
#include <stdlib.h>          // exit(), atol()
#include <fcntl.h>           // open()
#include <string.h>          // strcpy(), memset(), strlen(), strstr(), strtok()
#include <signal.h>          // signal(), sigaction(), SIGTERM, SIGINT, SIGALRM, kill()
#include <sys/file.h>        // flock()
#include <limits.h>          // PATH_MAX
#include <errno.h>
#include <dirent.h>          // opendir()/readdir() - used to list/clear directories
                             // generically instead of hard-coding filenames
#include <time.h>            // time(), localtime(), strftime(), nanosleep() - run
                             // history timestamps and the execution-timeout poll loop


/* ============================================================
   TERMINAL UI HELPERS
   A small set of helpers that give the whole program one clean,
   consistent look: boxed headers, aligned key/value lines, and
   colored status markers. Colors are switched on only when stdout
   is an actual terminal (isatty), so redirecting output to a file
   or a log still comes out perfectly plain.
   ============================================================ */

static int g_color = 0;

#define C_RESET   "\033[0m"
#define C_BOLD    "\033[1m"
#define C_DIM     "\033[2m"
#define C_CYAN    "\033[36m"
#define C_BLUE    "\033[34m"
#define C_MAGENTA "\033[35m"
#define C_YELLOW  "\033[33m"
#define C_GREEN   "\033[32m"
#define C_RED     "\033[31m"
#define C_WHITE   "\033[37m"

/* Box-drawing characters, written as raw UTF-8 byte escapes so they
   never depend on the compiler's source-file encoding. */
#define BOX_H  "\xe2\x95\x90"   /* ═ */
#define BOX_TL "\xe2\x95\x94"   /* ╔ */
#define BOX_TR "\xe2\x95\x97"   /* ╗ */
#define BOX_BL "\xe2\x95\x9a"   /* ╚ */
#define BOX_BR "\xe2\x95\x9d"   /* ╝ */
#define BOX_V  "\xe2\x95\x91"   /* ║ */

#define LINE_H  "\xe2\x94\x80"  /* ─ */
#define LINE_TL "\xe2\x94\x8c"  /* ┌ */
#define LINE_TR "\xe2\x94\x90"  /* ┐ */
#define LINE_BL "\xe2\x94\x94"  /* └ */
#define LINE_BR "\xe2\x94\x98"  /* ┘ */
#define LINE_V  "\xe2\x94\x82"  /* │ */

#define GLYPH_CHECK "\xe2\x9c\x93"  /* ✓ */
#define GLYPH_CROSS "\xe2\x9c\x97"  /* ✗ */

static void ui_init(void)
{
    g_color = isatty(fileno(stdout));
}

static const char *clr(const char *code)
{
    return g_color ? code : "";
}

/* Bold double-line box, centered title. Used for the program banner,
   the main menu, and the big "start/summary" pipeline markers. */
static void print_banner_box(const char *title, const char *color)
{
    int width = 60;
    int len = (int)strlen(title);
    if (len > width - 2) len = width - 2;
    int pad = width - len;
    int left = pad / 2;
    int right = pad - left;

    printf("\n%s%s", clr(color), clr(C_BOLD));
    printf(BOX_TL);
    for (int i = 0; i < width; i++) printf(BOX_H);
    printf(BOX_TR "\n");

    printf(BOX_V);
    for (int i = 0; i < left; i++) printf(" ");
    printf("%.*s", len, title);
    for (int i = 0; i < right; i++) printf(" ");
    printf(BOX_V "\n");

    printf(BOX_BL);
    for (int i = 0; i < width; i++) printf(BOX_H);
    printf(BOX_BR "\n");
    printf("%s\n", clr(C_RESET));
}

/* Light single-line box sized to fit its title exactly. Used for each
   agent's own identity header, e.g. "CODE GENERATOR AGENT (PID 4213)". */
static void print_agent_box(const char *label, const char *color, pid_t pid)
{
    char title[96];
    snprintf(title, sizeof(title), "%s  (PID %d)", label, (int)pid);

    int title_len = (int)strlen(title);
    int width = title_len + 4;

    printf("\n%s%s", clr(color), clr(C_BOLD));
    printf(LINE_TL);
    for (int i = 0; i < width; i++) printf(LINE_H);
    printf(LINE_TR "\n");

    printf(LINE_V "  %s", title);
    for (int i = 0; i < width - 2 - title_len; i++) printf(" ");
    printf(LINE_V "\n");

    printf(LINE_BL);
    for (int i = 0; i < width; i++) printf(LINE_H);
    printf(LINE_BR "\n");
    printf("%s", clr(C_RESET));
}

/* Thin divider used between blocks of related output. */
static void print_rule(void)
{
    printf("%s", clr(C_DIM));
    for (int i = 0; i < 60; i++) printf(LINE_H);
    printf("%s\n", clr(C_RESET));
}

/* A named sub-section inside an agent's output, e.g. "FILE
   SYNCHRONIZATION" or "RESOURCE MONITOR" - lighter than a full box. */
static void print_section(const char *title)
{
    print_rule();
    printf(" %s%s%s\n", clr(C_BOLD), title, clr(C_RESET));
    print_rule();
}

/* Aligned "label : value" line for dumping structured messages. */
static void print_kv(const char *label, const char *value)
{
    printf("   %-8s: %s\n", label, value);
}

/* Same, but colors the value based on common status words. */
static void print_kv_status(const char *label, const char *value)
{
    const char *color = C_WHITE;
    if (strstr(value, "SUCCESS") || strstr(value, "PASS"))
        color = C_GREEN;
    else if (strstr(value, "FAIL") || strstr(value, "ERROR"))
        color = C_RED;

    printf("   %-8s: %s%s%s\n", label, clr(color), value, clr(C_RESET));
}

static void print_step(int step, int total, const char *text)
{
    printf("%s   [%d/%d]%s %s\n", clr(C_CYAN), step, total, clr(C_RESET), text);
}

static void print_ok(const char *text)
{
    printf("   %s" GLYPH_CHECK "%s %s\n", clr(C_GREEN), clr(C_RESET), text);
}

static void print_fail(const char *text)
{
    printf("   %s" GLYPH_CROSS "%s %s\n", clr(C_RED), clr(C_RESET), text);
}

static void print_warn(const char *text)
{
    printf("   %s!%s %s\n", clr(C_YELLOW), clr(C_RESET), text);
}

/* Strips trailing \n / \r so program output can be shown on a single
   aligned key/value line instead of spilling onto the next line. */
static void strip_newline(char *s)
{
    size_t len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r'))
        s[--len] = '\0';
}


/* ============================================================
   FIFO NAMES
   Named pipes are used for communication between agents. Named to
   match the v2 pipeline order: Generator -> Reviewer -> Executor -> Tester.
   ============================================================ */

#define FIFO_GEN_TO_REVIEW  "fifo/generator_to_reviewer"
#define FIFO_REVIEW_TO_EXEC "fifo/reviewer_to_executor"
#define FIFO_EXEC_TO_TEST   "fifo/executor_to_tester"

/* No agent should ever block forever on a pipe: if the peer it is
   waiting on has crashed or was never started, give up after this
   many seconds instead of hanging the whole pipeline. */
#define FIFO_TIMEOUT_SEC 15


/* ============================================================
   RESTRICTED WORKSPACE
   ============================================================ */

#define WORKSPACE "workspace"
#define INPUT_DIR "workspace/input"
#define OUTPUT_DIR "workspace/output"
#define BACKUP_DIR "workspace/backup"

/* Every generated program is compiled to this one fixed binary name,
   regardless of which source file produced it - keeps the Execution
   and Testing Agents simple, since they never need to know the name
   of whatever source the Code Generator happened to write this run. */
#define PROGRAM_BIN "workspace/output/app"


/* ============================================================
   ACTIVITY LOG
   ============================================================ */

#define LOG_FILE "logs/agent_activity.log"

/* Per-run pass/fail history, plus a tiny scratch file the Testing Agent
   uses to hand its verdict back to the Main Controller (a forked child
   can't return a value directly - a file is the simplest handoff). */
#define HISTORY_FILE "logs/run_history.log"
#define LAST_RESULT_FILE "logs/last_result.txt"

/* Captures gcc's own stderr so a compile failure can show the actual
   compiler diagnostics instead of just "Compilation failed". */
#define COMPILE_LOG "workspace/output/compile_errors.log"


/* ============================================================
   RESOURCE LIMITS
   These used to be fixed #defines. They're now the fallback
   defaults - see RUNTIME CONFIGURATION below for how "pipeline.conf"
   can override them without recompiling.
   ============================================================ */

#define MEMORY_LIMIT_DEFAULT 50000       // 50000 KB
#define CPU_LIMIT_DEFAULT 1000           // 1000 CPU ticks

/* A generated program (especially a user-pasted custom one) might
   contain an infinite loop - this caps how long the Execution Agent
   will ever wait for it before forcibly killing it. */
#define PROGRAM_TIMEOUT_DEFAULT 8

static long g_memory_limit = MEMORY_LIMIT_DEFAULT;
static long g_cpu_limit = CPU_LIMIT_DEFAULT;
static int g_program_timeout_sec = PROGRAM_TIMEOUT_DEFAULT;


/* ============================================================
   RUNTIME CONFIGURATION
   Reads MEMORY_LIMIT_KB / CPU_LIMIT_TICKS / PROGRAM_TIMEOUT_SEC from
   "pipeline.conf" if present, so the limits above can be tuned by
   editing a text file instead of recompiling. A default copy of the
   file is created the first time the program runs, matching the
   compiled-in defaults exactly, so nothing changes until the user
   actually edits it.
   ============================================================ */

#define CONFIG_FILE "pipeline.conf"

static void write_default_config_if_missing(void)
{
    struct stat st;

    /* if the file already exists, leave it - and whatever the user
       has changed in it - completely untouched */
    if (stat(CONFIG_FILE, &st) == 0) return;

    FILE *f = fopen(CONFIG_FILE, "w");
    if (!f) return;

    fprintf(f,
        "# Multi-Agent Pipeline configuration\n"
        "# Edit these values and they take effect on the next run - no\n"
        "# recompiling needed. Delete this file to go back to the defaults.\n"
        "\n"
        "MEMORY_LIMIT_KB=%ld\n"
        "CPU_LIMIT_TICKS=%ld\n"
        "PROGRAM_TIMEOUT_SEC=%d\n",
        (long)MEMORY_LIMIT_DEFAULT, (long)CPU_LIMIT_DEFAULT, PROGRAM_TIMEOUT_DEFAULT);

    fclose(f);
}

static void load_config(void)
{
    write_default_config_if_missing();

    FILE *f = fopen(CONFIG_FILE, "r");
    if (!f) return;

    char line[128];

    // while loop: parse one "KEY=VALUE" pair per line - blank lines,
    // comments, and anything that isn't a recognized key are silently
    // skipped, so a hand-edited file with typos or extra notes never
    // breaks startup
    while (fgets(line, sizeof(line), f))
    {
        strip_newline(line);

        if (line[0] == '\0' || line[0] == '#')
            continue;

        char *equals = strchr(line, '=');
        if (equals == NULL) continue;

        *equals = '\0';
        const char *key = line;
        long value = atol(equals + 1);

        // if statement: a zero or negative value would either disable
        // the limit entirely or break the timeout math, so only a
        // genuinely positive number is allowed to override the default
        if (value <= 0) continue;

        if (strcmp(key, "MEMORY_LIMIT_KB") == 0)
            g_memory_limit = value;
        else if (strcmp(key, "CPU_LIMIT_TICKS") == 0)
            g_cpu_limit = value;
        else if (strcmp(key, "PROGRAM_TIMEOUT_SEC") == 0)
            g_program_timeout_sec = (int)value;
    }

    fclose(f);
}


/* ============================================================
   SINGLE-INSTANCE LOCK
   The FIFOs and workspace paths are fixed, shared names - two copies
   of this program running in the same directory at once would step
   on each other's pipes and files mid-run. An flock() on a small lock
   file (the same synchronization primitive already used for workspace
   files in synchronize_file()) makes that impossible, rather than
   just hoping it never happens.
   ============================================================ */

#define LOCK_FILE "logs/.pipeline.lock"

static void acquire_instance_lock_or_exit(void)
{
    int lock_fd = open(LOCK_FILE, O_CREAT | O_RDWR, 0644);

    if (lock_fd == -1)
    {
        perror("Could not open lock file");
        exit(1);
    }

    // if statement: LOCK_NB makes flock() fail immediately instead of
    // blocking if another instance already holds the lock - that
    // failure is exactly the signal that a second copy must not proceed
    if (flock(lock_fd, LOCK_EX | LOCK_NB) == -1)
    {
        if (errno == EWOULDBLOCK)
        {
            print_fail("Another instance of this program is already running here");
            printf("      Only one copy can safely use this workspace at a time.\n");
        }
        else
        {
            perror("Could not lock the workspace");
        }

        exit(1);
    }

    /* deliberately left open and never closed - the lock is held for
       the entire life of this process, and the OS releases it
       automatically (along with the file descriptor) on exit */
}


void log_activity(const char *agent, const char *activity);


/* ============================================================
   STRUCTURED MESSAGE
   This structure is transferred between agents through FIFOs.
   expected_output is new in v2: the Code Generator fills in what the
   compiled program should print, and that same value rides along
   through every hop so the Testing Agent can check against it
   without needing to know which program type was picked.
   ============================================================ */

typedef struct
{
    char agent[32];
    char task[32];
    char file[128];
    char status[32];
    char result[128];
    char expected_output[64];

} TaskMessage;


/* ============================================================
   ACTIVITY LOGGING
   ============================================================ */

void log_activity(const char *agent, const char *activity)
{
    /*
       open()
       System call used to open/create the activity log file.
       O_APPEND ensures new entries are added at the end.
    */

    int fd = open(LOG_FILE,
                  O_WRONLY | O_CREAT | O_APPEND,
                  0644);

    if (fd == -1)
    {
        perror("Log file opening failed");
        return;
    }

    char log_entry[500];

    int length = snprintf(log_entry,
                          sizeof(log_entry),
                          "[%s] PID: %d | %s\n",
                          agent,
                          getpid(),
                          activity);

    /*
       write()
       System call used to write the activity information
       into the log file.
    */

    ssize_t written = write(fd, log_entry, length);
    (void)written; /* logging is best-effort; a failed write here shouldn't crash the agent */

    /*
       close()
       Closes the file descriptor after logging.
    */

    close(fd);
}


/* ============================================================
   SELF-CONTAINED ENVIRONMENT SETUP
   Creates every directory and named pipe the pipeline needs, so the
   program can be built and run standalone -- nothing has to be
   prepared by hand beforehand.
   ============================================================ */

static void ensure_dir(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) mkdir(path, 0755);
}

static void write_default_source_if_missing(const char *path)
{
    struct stat st;

    /* if the file already exists, leave it completely untouched */
    if (stat(path, &st) == 0) return;

    FILE *f = fopen(path, "w");
    if (!f) return;

    fputs(
        "#include <stdio.h>\n"
        "\n"
        "int main(void)\n"
        "{\n"
        "    int n = 5;\n"
        "    long factorial = 1;\n"
        "\n"
        "    for (int i = 1; i <= n; i++)\n"
        "        factorial *= i;\n"
        "\n"
        "    printf(\"Factorial of %d = %ld\\n\", n, factorial);\n"
        "    return 0;\n"
        "}\n", f);

    fclose(f);
}

static void ensure_fifo(const char *path)
{
    /* if it already exists, that's fine - only report a genuine failure */
    if (mkfifo(path, 0666) == -1 && errno != EEXIST)
    {
        print_fail("Could not create named pipe");
        printf("      %s\n", path);
        perror("      reason");
    }
}

void ensure_environment(void)
{
    ensure_dir("fifo");
    ensure_dir(WORKSPACE);
    ensure_dir(INPUT_DIR);
    ensure_dir(OUTPUT_DIR);
    ensure_dir(BACKUP_DIR);
    ensure_dir("logs");

    ensure_fifo(FIFO_GEN_TO_REVIEW);
    ensure_fifo(FIFO_REVIEW_TO_EXEC);
    ensure_fifo(FIFO_EXEC_TO_TEST);

    /* a friendly example file so the workspace isn't completely empty
       even before the first pipeline run */
    write_default_source_if_missing(INPUT_DIR "/factorial.c");
}


/* ============================================================
   RESET ENVIRONMENT
   Wipes every generated source/output/backup/log file and rebuilds a
   completely clean workspace - useful for starting a fresh demo run
   without any leftovers from earlier ones.
   ============================================================ */

/* Deletes every regular file inside a directory (but keeps the
   directory itself) so Reset Environment can give a clean slate
   without needing to know every filename that might be in there. */
static void clear_dir_contents(const char *dir_path)
{
    DIR *d = opendir(dir_path);
    if (d == NULL) return;

    struct dirent *entry;

    // while loop: walk every entry in the directory and remove only the
    // regular files, leaving "." / ".." and any subdirectories alone
    while ((entry = readdir(d)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        char full_path[PATH_MAX];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);

        struct stat st;
        if (stat(full_path, &st) == 0 && S_ISREG(st.st_mode))
            remove(full_path);
    }

    closedir(d);
}

void reset_environment(void)
{
    print_banner_box("RESETTING ENVIRONMENT", C_YELLOW);

    printf("   Clearing generated source, output, backup, and log files...\n");

    clear_dir_contents(INPUT_DIR);
    clear_dir_contents(OUTPUT_DIR);
    clear_dir_contents(BACKUP_DIR);
    clear_dir_contents("logs");

    print_ok("Workspace and logs cleared");

    printf("   Rebuilding a fresh environment...\n");

    ensure_environment();

    print_ok("Environment reset complete - ready for a new run");
}


/* ============================================================
   RESTRICTED WORKSPACE VALIDATION
   ============================================================ */

int validate_workspace_path(const char *filename)
{
    char workspace_path[PATH_MAX];
    char requested_path[PATH_MAX];


    /*
       realpath()
       Converts a path into its absolute/canonical path.
       It is used to check whether the requested file
       belongs to the restricted workspace.
    */

    if (realpath(WORKSPACE, workspace_path) == NULL)
    {
        print_fail("Workspace path error");
        perror("      reason");
        return 0;
    }

    if (realpath(filename, requested_path) == NULL)
    {
        print_fail("File does not exist");
        printf("      %s\n", filename);
        return 0;
    }


    size_t workspace_length = strlen(workspace_path);


    /*
       strcmp/strncmp logic:
       Check whether the requested file path starts
       inside the allowed workspace path.
    */

    if (strncmp(requested_path,
                workspace_path,
                workspace_length) != 0)
    {
        print_fail("Access denied - file is outside the restricted workspace");
        return 0;
    }

    if (requested_path[workspace_length] != '/' &&
        requested_path[workspace_length] != '\0')
    {
        print_fail("Access denied - invalid workspace path");
        return 0;
    }

    print_ok("Workspace access validated");
    printf("      %s\n", requested_path);

    return 1;
}


/* ============================================================
   FILE ACCESS VALIDATION
   ============================================================ */

int validate_file(const char *filename)
{
    /*
       First check whether the requested path belongs
       to the restricted workspace.
    */

    if (!validate_workspace_path(filename))
    {
        log_activity("Main Controller",
                     "Unauthorized file access rejected");

        return 0;
    }


    /*
       access()
       System call used to check:
       F_OK -> whether file exists
       R_OK -> whether file can be read
    */

    if (access(filename, F_OK) == 0 &&
        access(filename, R_OK) == 0)
    {
        print_ok("File validation successful");

        log_activity("Main Controller",
                     "File access validation successful");

        return 1;
    }

    print_fail("File validation failed");

    log_activity("Main Controller",
                 "File access validation failed");

    return 0;
}


/* ============================================================
   BACKUP WORKSPACE FILE
   ============================================================ */

int backup_file(const char *source)
{
    char backup_path[PATH_MAX];

    const char *filename = strrchr(source, '/');

    if (filename == NULL)
    {
        print_fail("Invalid source file");
        return 0;
    }

    filename++;


    /*
       Create the backup filename.

       Example:
       workspace/input/factorial.c

       becomes:

       workspace/backup/factorial.c.bak
    */

    snprintf(backup_path,
             sizeof(backup_path),
             "%s/%s.bak",
             BACKUP_DIR,
             filename);


    /*
       open()
       Opens the original workspace file for reading.
    */

    int source_fd = open(source, O_RDONLY);

    if (source_fd == -1)
    {
        print_fail("Backup source opening failed");
        perror("      reason");
        return 0;
    }


    /*
       open()
       Creates/opens the backup file.

       O_CREAT -> create if it doesn't exist
       O_TRUNC -> clear previous contents
       O_WRONLY -> write only
    */

    int backup_fd = open(backup_path,
                         O_WRONLY | O_CREAT | O_TRUNC,
                         0644);

    if (backup_fd == -1)
    {
        print_fail("Backup file creation failed");
        perror("      reason");

        close(source_fd);

        return 0;
    }


    char buffer[1024];
    ssize_t bytes_read;


    /*
       read()
       Reads data from the original file.

       write()
       Copies the data into the backup file.
    */

    while ((bytes_read = read(source_fd,
                              buffer,
                              sizeof(buffer))) > 0)
    {
        ssize_t total_written = 0;

        while (total_written < bytes_read)
        {
            ssize_t bytes_written =
                write(backup_fd,
                      buffer + total_written,
                      bytes_read - total_written);

            if (bytes_written == -1)
            {
                print_fail("Backup writing failed");
                perror("      reason");

                close(source_fd);
                close(backup_fd);

                return 0;
            }

            total_written += bytes_written;
        }
    }


    close(source_fd);
    close(backup_fd);


    print_ok("Backup created");
    printf("      %s\n", backup_path);

    log_activity("Execution Agent",
                 "Backup copy created before workspace operation");

    return 1;
}


/* ============================================================
   FILE SYNCHRONIZATION
   ============================================================ */

int synchronize_file(const char *filename)
{
    print_section("FILE SYNCHRONIZATION");


    if (!validate_workspace_path(filename))
    {
        print_fail("Synchronization denied");
        return 0;
    }


    /*
       open()
       Opens the workspace file for protected access.
    */

    int fd = open(filename, O_RDWR);

    if (fd == -1)
    {
        print_fail("File opening failed");
        perror("      reason");
        return 0;
    }

    printf("   Requesting exclusive lock...\n");


    /*
       flock()
       Linux system call used for file synchronization.

       LOCK_EX = exclusive lock

       This prevents multiple agents from performing
       the protected operation at the same time.
    */

    if (flock(fd, LOCK_EX) == -1)
    {
        print_fail("File lock failed");
        perror("      reason");

        close(fd);

        return 0;
    }


    print_ok("Exclusive lock acquired - only one agent can access the file now");


    printf("   Creating backup before protected operation...\n");


    if (!backup_file(filename))
    {
        print_fail("Backup failed - protected operation cancelled");

        /*
           LOCK_UN
           Releases the file lock.
        */

        flock(fd, LOCK_UN);

        close(fd);

        return 0;
    }


    printf("   Performing protected file operation...\n");

    sleep(2);

    print_ok("Protected file operation completed");


    /*
       flock(LOCK_UN)
       Releases the exclusive file lock.
    */

    if (flock(fd, LOCK_UN) == -1)
    {
        print_fail("File unlock failed");
        perror("      reason");

        close(fd);

        return 0;
    }

    print_ok("File lock released successfully");

    close(fd);

    print_ok("File synchronization completed");

    log_activity("Execution Agent",
                 "Workspace file synchronized using exclusive lock");

    return 1;
}


/* ============================================================
   FIFO OPEN WITH TIMEOUT
   A plain open() on a FIFO blocks until a peer opens the other end -
   which means one crashed or missing agent would leave every agent
   after it in the pipeline hanging forever. This wraps open() with
   an alarm() so it gives up cleanly after FIFO_TIMEOUT_SEC seconds.
   ============================================================ */

static volatile sig_atomic_t g_fifo_open_timed_out = 0;

static void handle_fifo_alarm(int signum)
{
    (void)signum;
    g_fifo_open_timed_out = 1;
}

static int open_fifo_timeout(const char *path, int flags, int timeout_sec)
{
    struct sigaction sa, old_sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_fifo_alarm;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; /* deliberately no SA_RESTART - open() must be interruptible */

    g_fifo_open_timed_out = 0;

    sigaction(SIGALRM, &sa, &old_sa);
    alarm(timeout_sec);

    int fd = open(path, flags);

    alarm(0);
    sigaction(SIGALRM, &old_sa, NULL);

    // if statement: the open() call was interrupted by our own alarm,
    // not by a normal error - report it as a timeout so the caller can
    // give the user a clear "peer never showed up" message
    if (fd == -1 && g_fifo_open_timed_out)
    {
        errno = ETIMEDOUT;
    }

    return fd;
}


/* ============================================================
   PROCESS WAIT WITH TIMEOUT
   A generated program - especially a custom, user-pasted one - could
   easily contain an infinite loop. A plain waitpid() would then block
   the Execution Agent (and the whole pipeline behind it) forever, so
   this polls instead and force-kills the child if it overstays its
   welcome.
   ============================================================ */

static int waitpid_timeout(pid_t pid, int *status, int timeout_sec)
{
    int elapsed_ms = 0;
    const int poll_ms = 100;

    // while loop: poll every 100ms instead of blocking, so the caller
    // can notice a program that never finishes and kill it, instead of
    // being stuck inside a single waitpid() call with no way out
    while (elapsed_ms < timeout_sec * 1000)
    {
        pid_t result = waitpid(pid, status, WNOHANG);

        if (result == pid) return 0;   /* finished normally within the limit */
        if (result == -1) return -1;   /* a genuine waitpid() error */

        struct timespec delay = { 0, poll_ms * 1000000L };
        nanosleep(&delay, NULL);

        elapsed_ms += poll_ms;
    }

    // if statement: still running after the full timeout - it isn't
    // going to stop on its own (e.g. a while(1) loop), so force it
    print_warn("Program did not finish in time - forcing it to stop");

    kill(pid, SIGKILL);
    waitpid(pid, status, 0);

    return 1; /* timed out */
}


/* ============================================================
   SIGNAL HANDLER
   ============================================================ */

void handle_signal(int signal_number)
{
    if (signal_number == SIGTERM)
    {
        printf("\n");
        print_warn("Execution Agent received SIGTERM - resource limit exceeded");
        print_warn("Execution Agent is terminating safely");

        log_activity("Execution Agent",
                     "Execution Agent terminated due to resource limit");


        /*
           _exit()
           Terminates the current process immediately.
        */

        _exit(2);
    }
}


/* ============================================================
   PROGRAM CATALOG
   The small set of programs the Code Generator can actually
   produce. Index 0 is left unused so the menu numbers (1-5) line up
   directly with array indices. expected_output is what a correct,
   compiled run of that program should print - the Testing Agent
   checks against this instead of a single hard-coded string.
   ============================================================ */

typedef struct
{
    const char *filename;      // where the source is written, inside workspace/input
    const char *task_name;     // short human label used in log/status output
    const char *source;        // the C source code the Code Generator "writes"
    const char *expected;      // the exact text the compiled program should print
} ProgramSpec;

static const ProgramSpec PROGRAM_CATALOG[6] =
{
    { NULL, NULL, NULL, NULL }, /* unused - keeps menu choice == array index */

    {
        "factorial.c", "Factorial Calculator",
        "#include <stdio.h>\n"
        "\n"
        "int main(void)\n"
        "{\n"
        "    int n = 5;\n"
        "    long factorial = 1;\n"
        "\n"
        "    for (int i = 1; i <= n; i++)\n"
        "        factorial *= i;\n"
        "\n"
        "    printf(\"Factorial of %d = %ld\\n\", n, factorial);\n"
        "    return 0;\n"
        "}\n",
        "Factorial of 5 = 120"
    },

    {
        "fibonacci.c", "Fibonacci Sequence",
        "#include <stdio.h>\n"
        "\n"
        "int main(void)\n"
        "{\n"
        "    int n = 10;\n"
        "    long a = 0, b = 1;\n"
        "\n"
        "    for (int i = 0; i < n; i++)\n"
        "    {\n"
        "        long next = a + b;\n"
        "        a = b;\n"
        "        b = next;\n"
        "    }\n"
        "\n"
        "    printf(\"Fibonacci(%d) = %ld\\n\", n, a);\n"
        "    return 0;\n"
        "}\n",
        "Fibonacci(10) = 55"
    },

    {
        "prime_check.c", "Prime Number Check",
        "#include <stdio.h>\n"
        "\n"
        "int main(void)\n"
        "{\n"
        "    int n = 29;\n"
        "    int is_prime = (n > 1);\n"
        "\n"
        "    for (int i = 2; i * i <= n; i++)\n"
        "    {\n"
        "        if (n % i == 0)\n"
        "        {\n"
        "            is_prime = 0;\n"
        "            break;\n"
        "        }\n"
        "    }\n"
        "\n"
        "    printf(\"%d is %s\\n\", n, is_prime ? \"Prime\" : \"Not Prime\");\n"
        "    return 0;\n"
        "}\n",
        "29 is Prime"
    },

    {
        "digit_sum.c", "Digit Sum Calculator",
        "#include <stdio.h>\n"
        "\n"
        "int main(void)\n"
        "{\n"
        "    long n = 12345;\n"
        "    long remaining = n;\n"
        "    int sum = 0;\n"
        "\n"
        "    while (remaining > 0)\n"
        "    {\n"
        "        sum += remaining % 10;\n"
        "        remaining /= 10;\n"
        "    }\n"
        "\n"
        "    printf(\"Sum of digits of %ld = %d\\n\", n, sum);\n"
        "    return 0;\n"
        "}\n",
        "Sum of digits of 12345 = 15"
    },

    {
        "unsafe_demo.c", "Unsafe Demo (fails review)",
        "#include <stdio.h>\n"
        "\n"
        "int main(void)\n"
        "{\n"
        "    char name[16];\n"
        "\n"
        "    printf(\"Enter your name: \");\n"
        "    gets(name);\n"
        "    printf(\"Hello, %s!\\n\", name);\n"
        "    return 0;\n"
        "}\n",
        "Hello"
    }
};

static int g_program_choice = 1;

/* Choice 6 - "paste your own program" - doesn't fit in the fixed
   PROGRAM_CATALOG table (its source isn't known at compile time), so
   it lives in these globals instead. Set once in the Main Controller
   before forking, and inherited by every child the same way
   g_program_choice is. */
#define CUSTOM_SOURCE_MAX 4096

static char g_custom_source[CUSTOM_SOURCE_MAX];
static char g_custom_expected[64];

/* Lets the user type or paste in their own small C program instead of
   picking from the catalog. Reads line by line until a line containing
   only "ENDCODE", then asks what a correct run should print - leaving
   that blank tells the Testing Agent to just check the exit code
   instead of matching specific text. */
static void read_custom_program(void)
{
    print_section("CUSTOM PROGRAM SOURCE");

    printf("   Paste your C program below.\n");
    printf("   Type a line containing only %sENDCODE%s when you are done.\n\n",
           clr(C_BOLD), clr(C_RESET));

    // clears the leftover newline still sitting in stdin from the
    // scanf("%d", ...) that just read the menu choice, so the fgets()
    // loop below doesn't immediately read an empty first line
    int leftover;
    while ((leftover = getchar()) != '\n' && leftover != EOF);

    g_custom_source[0] = '\0';
    size_t used = 0;

    char line[512];

    // while loop: keep appending pasted lines to the custom source
    // buffer until the user's "ENDCODE" sentinel line or the buffer
    // limit is reached, whichever comes first
    while (fgets(line, sizeof(line), stdin) != NULL)
    {
        if (strncmp(line, "ENDCODE", 7) == 0)
            break;

        size_t line_len = strlen(line);

        // if statement: stop accepting more lines once the buffer is
        // full, rather than overflowing it - better a truncated program
        // than corrupted memory
        if (used + line_len >= sizeof(g_custom_source) - 1)
        {
            print_warn("Source too long - truncating at the buffer limit");
            break;
        }

        memcpy(g_custom_source + used, line, line_len);
        used += line_len;
        g_custom_source[used] = '\0';
    }

    printf("\n   Expected output (leave blank to just check the exit code): ");

    if (fgets(g_custom_expected, sizeof(g_custom_expected), stdin) != NULL)
        strip_newline(g_custom_expected);

    print_ok("Custom program captured");
}

/* Lets the user pick which program the Code Generator will produce
   this run. Called once, in the Main Controller, before any agent is
   forked - the child processes inherit g_program_choice automatically
   since fork() copies the parent's memory. */
static void select_program_choice(void)
{
    print_section("SELECT A PROGRAM FOR THE CODE GENERATOR");

    printf("   %s1.%s Factorial calculator\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s2.%s Fibonacci sequence\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s3.%s Prime number check\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s4.%s Digit sum calculator\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s5.%s Unsafe demo %s(deliberately fails code review)%s\n",
           clr(C_BOLD), clr(C_RESET), clr(C_DIM), clr(C_RESET));
    printf("   %s6.%s Custom program %s(paste your own C code)%s\n",
           clr(C_BOLD), clr(C_RESET), clr(C_DIM), clr(C_RESET));

    printf("\n   Enter your choice [1-6]: ");

    int choice;

    // if statement: non-numeric input is treated the same way the main
    // menu treats it - clear the bad input and fall back to a sane default
    if (scanf("%d", &choice) != 1)
    {
        int junk;
        // while loop: also stops at EOF, not just '\n' - without that,
        // a closed/redirected stdin with no newline left in it would
        // make getchar() return EOF forever and spin this loop forever
        while ((junk = getchar()) != '\n' && junk != EOF);
        choice = 1;
    }

    if (choice < 1 || choice > 6)
    {
        print_warn("Invalid choice - defaulting to Factorial calculator");
        choice = 1;
    }

    g_program_choice = choice;

    // if statement: choice 6 needs its source typed in right now, before
    // any agent is forked, since every agent inherits g_custom_source
    // as it stood at fork() time
    if (g_program_choice == 6)
    {
        read_custom_program();
    }
}

static int write_generated_source(const char *path, const char *content)
{
    FILE *f = fopen(path, "w");
    if (!f) return 0;

    fputs(content, f);
    fclose(f);
    return 1;
}


/* ============================================================
   CODE GENERATOR AGENT
   ============================================================ */

void code_generator(void)
{
    print_agent_box("CODE GENERATOR AGENT", C_CYAN, getpid());

    char file_path[PATH_MAX];
    const char *task_name;
    const char *source;
    const char *expected;

    // if statement: choice 6 is the user's own pasted program, living in
    // globals rather than the fixed PROGRAM_CATALOG table - everything
    // else below treats it identically to a catalog entry from this
    // point on, so the rest of the pipeline never needs to know the
    // difference
    if (g_program_choice == 6)
    {
        snprintf(file_path, sizeof(file_path), "%s/custom.c", INPUT_DIR);
        task_name = "Custom Program";
        source = g_custom_source;
        expected = g_custom_expected;
    }
    else
    {
        const ProgramSpec *spec = &PROGRAM_CATALOG[g_program_choice];
        snprintf(file_path, sizeof(file_path), "%s/%s", INPUT_DIR, spec->filename);
        task_name = spec->task_name;
        source = spec->source;
        expected = spec->expected;
    }

    printf("   Generating \"%s\"...\n", task_name);

    log_activity("Code Generator",
                 "Code generation started");

    sleep(2);


    // if statement: the source file is (re)written every run - the whole
    // point of this agent is to actually produce fresh code, not just
    // point at whatever happened to already be sitting in the workspace
    if (!write_generated_source(file_path, source))
    {
        print_fail("Could not write generated source file");
        perror("      reason");

        log_activity("Code Generator",
                     "Could not write generated source file");

        exit(1);
    }

    print_ok("Source file written");
    printf("      %s\n", file_path);


    /*
       open_fifo_timeout()
       Opens the named FIFO for writing, giving up after
       FIFO_TIMEOUT_SEC seconds instead of blocking forever.

       FIFO:
       generator_to_reviewer
    */

    int fd = open_fifo_timeout(FIFO_GEN_TO_REVIEW, O_WRONLY, FIFO_TIMEOUT_SEC);

    if (fd == -1)
    {
        if (errno == ETIMEDOUT)
        {
            print_fail("Timed out opening the pipe to the Code Review Agent");
        }
        else
        {
            print_fail("Generator FIFO open failed");
            perror("      reason");
        }

        log_activity("Code Generator",
                     "Generator FIFO open failed");

        exit(1);
    }


    /*
       Create structured task information.
    */

    TaskMessage message;

    memset(&message, 0, sizeof(message));

    strcpy(message.agent, "Code Generator");
    snprintf(message.task, sizeof(message.task), "%s", task_name);
    snprintf(message.file, sizeof(message.file), "%.127s", file_path);
    strcpy(message.status, "SUCCESS");
    strcpy(message.result, "Code generated successfully");
    snprintf(message.expected_output, sizeof(message.expected_output), "%s", expected);


    /*
       write()
       Sends the structured TaskMessage through
       the named FIFO to the Code Review Agent.
    */

    ssize_t written = write(fd, &message, sizeof(message));
    (void)written; /* small fixed-size struct, well under PIPE_BUF - a full write is guaranteed */

    print_ok("Structured task information sent to Code Review Agent");

    log_activity("Code Generator",
                 "Structured task information sent to Code Review Agent");

    close(fd);

    print_ok("Code Generator finished");

    log_activity("Code Generator",
                 "Code Generator finished");

    exit(0);
}


/* ============================================================
   STATIC CODE REVIEW
   A small stand-in for a real static analyzer: it scans the
   generated source text for a couple of well-known danger signs
   before anything is ever compiled or executed.
   ============================================================ */

typedef struct
{
    int passed;
    char reason[128];
} ReviewResult;

static ReviewResult review_source_file(const char *path)
{
    ReviewResult result;
    result.passed = 1;
    strcpy(result.reason, "No issues found - clean brace balance, no banned calls");

    FILE *f = fopen(path, "r");

    if (!f)
    {
        result.passed = 0;
        snprintf(result.reason, sizeof(result.reason), "Could not open source file for review");
        return result;
    }

    char buffer[8192];
    size_t len = fread(buffer, 1, sizeof(buffer) - 1, f);
    buffer[len] = '\0';
    fclose(f);

    int open_braces = 0;
    int close_braces = 0;

    // for loop: count braces one character at a time to check the file
    // is at least structurally well-formed before anyone tries to compile it
    for (size_t i = 0; i < len; i++)
    {
        if (buffer[i] == '{') open_braces++;
        if (buffer[i] == '}') close_braces++;
    }

    // if statement: a banned function anywhere in the source is an
    // automatic fail - these are classic sources of buffer overflows and
    // command injection, so nothing gets built or run past this point
    if (strstr(buffer, "gets(") != NULL)
    {
        result.passed = 0;
        snprintf(result.reason, sizeof(result.reason),
                 "Uses gets() - unbounded input, a classic buffer overflow risk");
    }
    else if (strstr(buffer, "system(") != NULL)
    {
        result.passed = 0;
        snprintf(result.reason, sizeof(result.reason),
                 "Uses system() - shell command injection risk");
    }
    // if statement: mismatched braces means the file likely will not even
    // compile correctly, so there's no point handing it to the compiler
    else if (open_braces != close_braces)
    {
        result.passed = 0;
        snprintf(result.reason, sizeof(result.reason),
                 "Mismatched braces (%d open vs %d close)", open_braces, close_braces);
    }
    else if (len == 0)
    {
        result.passed = 0;
        snprintf(result.reason, sizeof(result.reason), "Source file is empty");
    }

    return result;
}

void code_review_agent(void)
{
    print_agent_box("CODE REVIEW AGENT", C_MAGENTA, getpid());

    printf("   Waiting for generated code...\n");

    log_activity("Code Review Agent",
                 "Code Review Agent started");


    /*
       open_fifo_timeout()
       Opens the Generator-to-Reviewer FIFO for reading.
    */

    int fd_read = open_fifo_timeout(FIFO_GEN_TO_REVIEW, O_RDONLY, FIFO_TIMEOUT_SEC);

    if (fd_read == -1)
    {
        if (errno == ETIMEDOUT)
        {
            print_fail("Timed out waiting for the Code Generator");
        }
        else
        {
            print_fail("Generator FIFO open failed");
            perror("      reason");
        }

        log_activity("Code Review Agent",
                     "Generator FIFO open failed");

        exit(1);
    }


    TaskMessage message;

    memset(&message, 0, sizeof(message));


    /*
       read()
       Receives the structured TaskMessage
       from the Code Generator Agent.
    */

    ssize_t bytes =
        read(fd_read,
             &message,
             sizeof(message));

    close(fd_read);


    if (bytes != sizeof(message))
    {
        print_fail("Invalid structured message received");

        log_activity("Code Review Agent",
                     "Invalid structured message received");

        exit(1);
    }


    print_rule();
    printf(" %sReceived Generated Code%s\n", clr(C_BOLD), clr(C_RESET));
    print_kv("Agent", message.agent);
    print_kv("Task", message.task);
    print_kv("File", message.file);
    print_kv_status("Status", message.status);
    print_rule();

    log_activity("Code Review Agent",
                 "Structured task information received");


    /* ========================================================
       STATIC REVIEW
       ======================================================== */

    printf("   Running static review checks...\n");

    ReviewResult review = review_source_file(message.file);

    if (review.passed)
    {
        print_ok(review.reason);
    }
    else
    {
        print_fail(review.reason);
    }

    log_activity("Code Review Agent",
                 review.passed ? "Static review passed" : "Static review failed");


    /* ========================================================
       SEND REVIEW DECISION TO EXECUTION AGENT
       ======================================================== */

    int fd_write = open_fifo_timeout(FIFO_REVIEW_TO_EXEC, O_WRONLY, FIFO_TIMEOUT_SEC);

    if (fd_write == -1)
    {
        if (errno == ETIMEDOUT)
        {
            print_fail("Timed out opening the pipe to the Execution Agent");
        }
        else
        {
            print_fail("Executor FIFO open failed");
            perror("      reason");
        }

        log_activity("Code Review Agent",
                     "Executor FIFO open failed");

        exit(1);
    }


    TaskMessage outgoing;

    memset(&outgoing, 0, sizeof(outgoing));

    strcpy(outgoing.agent, "Code Review Agent");
    strcpy(outgoing.task, "Review Code");
    snprintf(outgoing.file, sizeof(outgoing.file), "%s", message.file);
    snprintf(outgoing.expected_output, sizeof(outgoing.expected_output), "%s", message.expected_output);


    // if statement: whether the pipeline is allowed to keep going hinges
    // entirely on this review result - a failed review still gets handed
    // to the Execution Agent, just tagged so it knows to skip compiling
    if (review.passed)
    {
        strcpy(outgoing.status, "SUCCESS");
        snprintf(outgoing.result, sizeof(outgoing.result),
                "%s", "Code review passed - cleared for execution");
    }
    else
    {
        strcpy(outgoing.status, "REVIEW_FAILED");
        snprintf(outgoing.result, sizeof(outgoing.result), "Review failed: %.100s", review.reason);
    }


    /*
       write()
       Sends the review decision to the Execution Agent.
    */

    ssize_t written = write(fd_write,
          &outgoing,
          sizeof(outgoing));
    (void)written; /* small fixed-size struct, well under PIPE_BUF - a full write is guaranteed */

    print_ok("Structured review result sent to Execution Agent");

    log_activity("Code Review Agent",
                 "Structured review result sent to Execution Agent");

    close(fd_write);

    print_ok("Code Review Agent finished");

    log_activity("Code Review Agent",
                 "Code Review Agent finished");

    exit(0);
}


/* ============================================================
   EXECUTION AGENT
   ============================================================ */

void execution_agent(void)
{
    /*
       signal()
       Registers the SIGTERM handler.

       If the resource monitor sends SIGTERM,
       handle_signal() will execute.
    */

    signal(SIGTERM, handle_signal);

    print_agent_box("EXECUTION AGENT", C_BLUE, getpid());

    printf("   Waiting for the Code Review Agent's decision...\n");

    log_activity("Execution Agent",
                 "Execution Agent started");


    /*
       open_fifo_timeout()
       Opens the Reviewer-to-Executor FIFO for reading.
    */

    int fd_read = open_fifo_timeout(FIFO_REVIEW_TO_EXEC, O_RDONLY, FIFO_TIMEOUT_SEC);

    if (fd_read == -1)
    {
        if (errno == ETIMEDOUT)
        {
            print_fail("Timed out waiting for the Code Review Agent");
        }
        else
        {
            print_fail("Reviewer FIFO open failed");
            perror("      reason");
        }

        log_activity("Execution Agent",
                     "Reviewer FIFO open failed");

        exit(1);
    }


    TaskMessage message;

    memset(&message, 0, sizeof(message));


    /*
       read()
       Receives the structured review decision
       from the Code Review Agent.
    */

    ssize_t bytes =
        read(fd_read,
             &message,
             sizeof(message));

    close(fd_read);


    if (bytes != sizeof(message))
    {
        print_fail("Invalid structured message received");

        log_activity("Execution Agent",
                     "Invalid structured message received");

        exit(1);
    }


    print_rule();
    printf(" %sReceived Review Decision%s\n", clr(C_BOLD), clr(C_RESET));
    print_kv("Agent", message.agent);
    print_kv("Task", message.task);
    print_kv("File", message.file);
    print_kv_status("Status", message.status);
    print_kv("Result", message.result);
    print_rule();

    log_activity("Execution Agent",
                 "Structured review decision received");


    TaskMessage result;

    memset(&result, 0, sizeof(result));

    strcpy(result.agent, "Execution Agent");
    snprintf(result.file, sizeof(result.file), "%s", message.file);
    snprintf(result.expected_output, sizeof(result.expected_output), "%s", message.expected_output);


    // if statement: a failed code review is a hard gate - the whole point
    // of reviewing before building is that unsafe code never reaches the
    // compiler, so this branch skips straight to notifying the Testing
    // Agent instead of touching gcc at all
    if (strcmp(message.status, "REVIEW_FAILED") == 0)
    {
        print_warn("Code review failed upstream - skipping compilation and execution");

        log_activity("Execution Agent",
                     "Skipped: code review failed upstream");

        strcpy(result.task, "Skipped (Review Failed)");
        strcpy(result.status, "REVIEW_FAILED");
        snprintf(result.result, sizeof(result.result), "%s", message.result);
    }
    else
    {
        strcpy(result.task, "Execute Program");


        /* ========================================================
           FILE ACCESS VALIDATION
           ======================================================== */

        print_step(1, 4, "Checking requested workspace file");

        // if statement: an invalid file no longer kills the whole agent -
        // it hands a clear FAILED reason to the Testing Agent instead, so
        // the pipeline still finishes cleanly rather than leaving the
        // Testing Agent to time out 15 seconds later for no clear reason
        if (!validate_file(message.file))
        {
            print_fail("Execution denied due to invalid file access");

            log_activity("Execution Agent",
                         "Execution denied due to file access validation");

            strcpy(result.status, "FAILED");
            snprintf(result.result, sizeof(result.result), "%s", "File access validation failed");

            goto send_result;
        }


        /* ========================================================
           FILE SYNCHRONIZATION + BACKUP
           ======================================================== */

        print_step(2, 4, "Synchronizing and backing up the file");

        if (!synchronize_file(message.file))
        {
            print_fail("Workspace operation failed");

            log_activity("Execution Agent",
                         "Workspace operation failed");

            strcpy(result.status, "FAILED");
            snprintf(result.result, sizeof(result.result), "%s", "Workspace synchronization failed");

            goto send_result;
        }


        /* ========================================================
           COMPILE PROGRAM
           ======================================================== */

        print_step(3, 4, "Compiling the generated program");

        log_activity("Execution Agent",
                     "Compilation started");


        /*
           fork()
           Creates a child process.

           The child will run GCC.
        */

        pid_t compiler_pid = fork();

        if (compiler_pid < 0)
        {
            print_fail("Compiler process creation failed");
            perror("      reason");

            log_activity("Execution Agent",
                         "Compiler process creation failed");

            strcpy(result.status, "FAILED");
            snprintf(result.result, sizeof(result.result), "%s", "Could not create compiler process");

            goto send_result;
        }


        if (compiler_pid == 0)
        {
            /*
               open()/dup2()
               Redirects gcc's own stderr into COMPILE_LOG, so a
               compile failure can show the real diagnostics instead
               of just "Compilation failed".
            */

            int err_fd = open(COMPILE_LOG, O_WRONLY | O_CREAT | O_TRUNC, 0644);

            if (err_fd != -1)
            {
                dup2(err_fd, STDERR_FILENO);
                close(err_fd);
            }

            /*
               execlp()
               Replaces this child process with the GCC compiler.
               -Wall surfaces compiler warnings too, not just hard
               errors - useful now that a "custom program" option
               lets the user compile arbitrary code.
            */

            execlp("gcc",
                   "gcc",
                   "-Wall",
                   message.file,
                   "-o",
                   PROGRAM_BIN,
                   NULL);

            perror("Compilation failed");

            _exit(1);
        }


        /*
           waitpid()
           Parent waits for the compiler child process
           to finish. Compilation of these small programs is always
           fast, so no timeout is needed here (unlike running the
           compiled program itself, below).
        */

        int compiler_status;

        waitpid(compiler_pid,
                &compiler_status,
                0);


        if (WIFEXITED(compiler_status) &&
            WEXITSTATUS(compiler_status) == 0)
        {
            print_ok("Compilation successful");

            log_activity("Execution Agent",
                         "Compilation successful");
        }
        else
        {
            print_fail("Compilation failed");

            log_activity("Execution Agent",
                         "Compilation failed");

            /*
               fopen()/fgets()
               Shows the compiler's own diagnostics (captured above)
               instead of leaving the user to guess why it failed.
            */

            FILE *compile_log = fopen(COMPILE_LOG, "r");

            if (compile_log != NULL)
            {
                printf("\n");
                print_section("COMPILER OUTPUT");

                char diag_line[256];
                int shown = 0;

                // while loop: print only the first handful of compiler
                // diagnostic lines - enough to see what went wrong
                // without flooding the screen on a badly broken source file
                while (fgets(diag_line, sizeof(diag_line), compile_log) && shown < 12)
                {
                    printf("   %s", diag_line);
                    shown++;
                }

                fclose(compile_log);
                print_rule();
            }

            strcpy(result.status, "FAILED");
            snprintf(result.result, sizeof(result.result),
                    "%s", "Compilation failed - see compiler diagnostics above");

            goto send_result;
        }


        /* ========================================================
           EXECUTE PROGRAM
           ======================================================== */

        print_step(4, 4, "Executing the compiled program");

        log_activity("Execution Agent",
                     "Program execution started");

        sleep(2);


        /*
           fork()
           Creates another child process for running
           the compiled program.
        */

        pid_t program_pid = fork();

        if (program_pid < 0)
        {
            print_fail("Program process creation failed");
            perror("      reason");

            log_activity("Execution Agent",
                         "Program process creation failed");

            strcpy(result.status, "FAILED");
            snprintf(result.result, sizeof(result.result), "%s", "Could not create program process");

            goto send_result;
        }


        if (program_pid == 0)
        {
            /*
               execl()
               Replaces the child process with the
               compiled program.
            */

            execl("./" PROGRAM_BIN,
                  "./" PROGRAM_BIN,
                  NULL);

            perror("Program execution failed");

            _exit(1);
        }


        /*
           waitpid_timeout()
           Waits for the compiled program, but won't wait forever -
           a custom program with an infinite loop gets force-killed
           after PROGRAM_TIMEOUT_SEC instead of hanging the pipeline.
        */

        int program_status;

        int timed_out = waitpid_timeout(program_pid, &program_status, g_program_timeout_sec);

        // if statement: the program never finished on its own and had
        // to be killed - treat that as a clear, specific failure rather
        // than lumping it in with an ordinary non-zero exit code
        if (timed_out == 1)
        {
            print_fail("Program timed out and was terminated");

            log_activity("Execution Agent",
                         "Program execution timed out - killed");

            strcpy(result.status, "FAILED");
            snprintf(result.result, sizeof(result.result),
                    "Program did not finish within %d seconds (possible infinite loop) - killed",
                    g_program_timeout_sec);
        }
        else if (WIFEXITED(program_status) &&
            WEXITSTATUS(program_status) == 0)
        {
            print_ok("Program executed successfully");

            log_activity("Execution Agent",
                         "Program execution successful");

            strcpy(result.status, "SUCCESS");
            snprintf(result.result, sizeof(result.result),
                    "%s", "Program compiled and executed successfully");
        }
        else
        {
            print_fail("Program execution failed");

            log_activity("Execution Agent",
                         "Program execution failed");

            strcpy(result.status, "FAILED");
            snprintf(result.result, sizeof(result.result),
                    "%s", "Program compiled but exited with an error");
        }
    }


send_result:

    /* ========================================================
       SEND RESULT TO TESTING AGENT
       ======================================================== */

    printf("\n   Sending result to Testing Agent...\n");

    /*
       open_fifo_timeout()
       Opens the Executor-to-Tester FIFO for writing.
    */

    int fd_write = open_fifo_timeout(FIFO_EXEC_TO_TEST, O_WRONLY, FIFO_TIMEOUT_SEC);

    if (fd_write == -1)
    {
        if (errno == ETIMEDOUT)
        {
            print_fail("Timed out opening the pipe to the Testing Agent");
        }
        else
        {
            print_fail("Tester FIFO open failed");
            perror("      reason");
        }

        log_activity("Execution Agent",
                     "Tester FIFO open failed");

        exit(1);
    }


    /*
       write()
       Sends structured execution information
       to the Testing Agent.
    */

    ssize_t written = write(fd_write,
          &result,
          sizeof(result));
    (void)written; /* small fixed-size struct, well under PIPE_BUF - a full write is guaranteed */

    print_ok("Structured execution result sent to Testing Agent");

    log_activity("Execution Agent",
                 "Structured execution result sent to Testing Agent");

    close(fd_write);

    print_ok("Execution Agent finished");

    log_activity("Execution Agent",
                 "Execution Agent finished");

    exit(0);
}


/* ============================================================
   TESTING AGENT
   ============================================================ */

/* Hands the Testing Agent's final verdict back to the Main Controller.
   A forked child can't return a value to its parent directly, so this
   tiny scratch file is the handoff - the parent reads it right after
   waitpid() on the Testing Agent to build the run history entry. */
static void write_last_result(const char *verdict, const char *detail)
{
    int fd = open(LAST_RESULT_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) return;

    char line[300];
    int len = snprintf(line, sizeof(line), "%s|%s\n", verdict, detail);

    ssize_t written = write(fd, line, len);
    (void)written; /* best-effort - a missed write here only affects the history log, not the test itself */

    close(fd);
}

void testing_agent(void)
{
    print_agent_box("TESTING AGENT", C_YELLOW, getpid());

    printf("   Waiting for execution result...\n");

    log_activity("Testing Agent",
                 "Testing Agent started");


    /*
       open_fifo_timeout()
       Opens the Executor-to-Tester FIFO for reading.
    */

    int fd = open_fifo_timeout(FIFO_EXEC_TO_TEST, O_RDONLY, FIFO_TIMEOUT_SEC);

    if (fd == -1)
    {
        if (errno == ETIMEDOUT)
        {
            print_fail("Timed out waiting for the Execution Agent");
        }
        else
        {
            print_fail("Tester FIFO open failed");
            perror("      reason");
        }

        log_activity("Testing Agent",
                     "Tester FIFO open failed");

        exit(1);
    }


    TaskMessage result;

    memset(&result, 0, sizeof(result));


    /*
       read()
       Receives the execution result.
    */

    ssize_t bytes =
        read(fd,
             &result,
             sizeof(result));

    close(fd);


    if (bytes != sizeof(result))
    {
        print_fail("Invalid execution information received");

        log_activity("Testing Agent",
                     "Invalid execution information received");

        exit(1);
    }


    print_rule();
    printf(" %sReceived Execution Result%s\n", clr(C_BOLD), clr(C_RESET));
    print_kv("Agent", result.agent);
    print_kv("Task", result.task);
    print_kv("File", result.file);
    print_kv_status("Status", result.status);
    print_kv("Result", result.result);
    print_rule();

    log_activity("Testing Agent",
                 "Structured execution result received");


    // if statement: only a program that actually compiled and ran gets
    // put through the real test - a review failure or a run failure both
    // land here as a clean SKIPPED result instead of a crash
    if (strcmp(result.status, "SUCCESS") != 0)
    {
        printf("   %s%sTEST RESULT: SKIPPED%s\n", clr(C_BOLD), clr(C_YELLOW), clr(C_RESET));
        print_kv("Reason", result.result);

        write_last_result("SKIPPED", result.result);

        log_activity("Testing Agent",
                     "TEST RESULT: SKIPPED");

        print_ok("Testing Agent finished");

        log_activity("Testing Agent",
                     "Testing Agent finished");

        exit(0);
    }


    /* ========================================================
       RUN ACTUAL TEST
       ======================================================== */

    printf("   Running actual test...\n");

    log_activity("Testing Agent",
                 "Testing started");


    /*
       popen()
       Opens a process and allows the Testing Agent
       to read the program's output.
    */

    FILE *process;

    char output[200] = {0};

    process = popen("./" PROGRAM_BIN,
                    "r");


    if (process == NULL)
    {
        print_fail("Testing process failed");
        perror("      reason");

        log_activity("Testing Agent",
                     "Testing process failed");

        exit(1);
    }


    /*
       fgets()
       Reads the output produced by the compiled program.
    */

    if (fgets(output,
              sizeof(output),
              process) != NULL)
    {
        printf("   Program output: %s", output);
    }


    /*
       pclose()
       Closes the process opened using popen().
    */

    int status = pclose(process);

    strip_newline(output);


    /* ========================================================
       CHECK EXPECTED RESULT
       ======================================================== */

    print_rule();

    // if statement: a custom program with no expected output typed in
    // (blank at the prompt) can't be checked for exact text, so it's
    // graded as a smoke test instead - PASS just means it ran to
    // completion cleanly
    if (result.expected_output[0] == '\0')
    {
        if (status == 0)
        {
            printf(" %s%sTEST RESULT: PASS%s %s(exit code only - no expected text was given)%s\n",
                   clr(C_BOLD), clr(C_GREEN), clr(C_RESET), clr(C_DIM), clr(C_RESET));
            print_kv("Actual", output);

            write_last_result("PASS", output);

            log_activity("Testing Agent",
                         "TEST RESULT: PASS (exit code only)");
        }
        else
        {
            printf(" %s%sTEST RESULT: FAIL%s %s(program exited with an error)%s\n",
                   clr(C_BOLD), clr(C_RED), clr(C_RESET), clr(C_DIM), clr(C_RESET));
            print_kv("Actual", output);

            write_last_result("FAIL", output);

            log_activity("Testing Agent",
                         "TEST RESULT: FAIL (exit code only)");
        }
    }
    else if (strstr(output,
               result.expected_output) != NULL &&
        status == 0)
    {
        printf(" %s%sTEST RESULT: PASS%s\n", clr(C_BOLD), clr(C_GREEN), clr(C_RESET));
        print_kv("Expected", result.expected_output);
        print_kv("Actual", output);

        write_last_result("PASS", output);

        log_activity("Testing Agent",
                     "TEST RESULT: PASS");
    }
    else
    {
        printf(" %s%sTEST RESULT: FAIL%s\n", clr(C_BOLD), clr(C_RED), clr(C_RESET));
        print_kv("Expected", result.expected_output);
        print_kv("Actual", output);

        write_last_result("FAIL", output);

        log_activity("Testing Agent",
                     "TEST RESULT: FAIL");
    }

    print_rule();


    print_ok("Testing Agent finished");

    log_activity("Testing Agent",
                 "Testing Agent finished");

    exit(0);
}


/* ============================================================
   RESOURCE MONITORING
   ============================================================ */

void monitor_process(pid_t pid)
{
    char status_path[100];
    char stat_path[100];


    /*
       /proc/<PID>/status
       Linux virtual file containing process information,
       including memory usage.

       /proc/<PID>/stat
       Contains process CPU statistics.
    */

    snprintf(status_path,
             sizeof(status_path),
             "/proc/%d/status",
             pid);

    snprintf(stat_path,
             sizeof(stat_path),
             "/proc/%d/stat",
             pid);


    print_agent_box("RESOURCE MONITOR", C_YELLOW, pid);

    printf("   Watching PID %d  |  Memory limit %ld KB  |  CPU limit %ld ticks\n\n",
           pid, g_memory_limit, g_cpu_limit);

    log_activity("Resource Monitor",
                 "Resource monitoring started");

    printf("   %-8s %-14s %-14s %s\n", "SAMPLE", "MEMORY (KB)", "CPU (ticks)", "STATUS");
    print_rule();


    for (int i = 0; i < 5; i++)
    {
        /*
           fopen()
           Opens the /proc status file to read
           memory information.
        */

        FILE *status_file =
            fopen(status_path, "r");


        if (status_file == NULL)
        {
            printf("   Process %d has finished.\n",
                   pid);

            log_activity("Resource Monitor",
                         "Execution Agent has finished");

            break;
        }


        char line[256];

        long memory = 0;


        /*
           Read /proc/<PID>/status line by line.
        */

        while (fgets(line,
                     sizeof(line),
                     status_file))
        {
            if (strncmp(line,
                        "VmRSS:",
                        6) == 0)
            {
                sscanf(line,
                       "VmRSS: %ld",
                       &memory);

                break;
            }
        }

        fclose(status_file);


        /* ====================================================
           CPU MONITORING
           ==================================================== */

        /*
           fopen()
           Opens /proc/<PID>/stat.
        */

        FILE *stat_file =
            fopen(stat_path, "r");

        long utime = 0;
        long stime = 0;


        if (stat_file != NULL)
        {
            char buffer[2048];


            if (fgets(buffer,
                      sizeof(buffer),
                      stat_file))
            {
                char *closing_bracket =
                    strrchr(buffer, ')');


                if (closing_bracket != NULL)
                {
                    char *fields =
                        closing_bracket + 2;

                    int field = 3;

                    char *token =
                        strtok(fields, " ");


                    /*
                       Extract CPU user time and system time
                       from /proc/<PID>/stat.
                    */

                    while (token != NULL)
                    {
                        if (field == 14)
                        {
                            utime = atol(token);
                        }

                        if (field == 15)
                        {
                            stime = atol(token);
                            break;
                        }

                        token = strtok(NULL, " ");

                        field++;
                    }
                }
            }

            fclose(stat_file);
        }


        long cpu_time =
            utime + stime;


        const char *state = "OK";
        const char *state_color = C_GREEN;

        if (memory > g_memory_limit)
        {
            state = "MEM LIMIT";
            state_color = C_RED;
        }
        else if (cpu_time > g_cpu_limit)
        {
            state = "CPU LIMIT";
            state_color = C_RED;
        }

        printf("   %-8d %-14ld %-14ld %s%s%s\n",
               i + 1, memory, cpu_time, clr(state_color), state, clr(C_RESET));


        /* ====================================================
           MEMORY LIMIT
           ==================================================== */

        if (memory > g_memory_limit)
        {
            printf("\n");
            print_warn("Memory limit exceeded - sending SIGTERM to Execution Agent");

            log_activity("Resource Monitor",
                         "Memory limit exceeded - SIGTERM sent");


            /*
               kill()
               Sends SIGTERM to the Execution Agent.
            */

            kill(pid, SIGTERM);

            break;
        }


        /* ====================================================
           CPU LIMIT
           ==================================================== */

        if (cpu_time > g_cpu_limit)
        {
            printf("\n");
            print_warn("CPU limit exceeded - sending SIGTERM to Execution Agent");

            log_activity("Resource Monitor",
                         "CPU limit exceeded - SIGTERM sent");


            /*
               kill()
               Sends SIGTERM to the monitored process.
            */

            kill(pid, SIGTERM);

            break;
        }


        sleep(1);
    }

    printf("\n");
    print_ok("Resource monitoring completed");

    log_activity("Resource Monitor",
                 "Resource monitoring completed");
}


/* ============================================================
   PIPELINE SUMMARY
   ============================================================ */

static void print_pipeline_result(const char *name, int status)
{
    int ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;

    printf("   %-20s %s%s%s\n",
           name,
           clr(ok ? C_GREEN : C_RED),
           ok ? "COMPLETED" : "FAILED",
           clr(C_RESET));
}


/* ============================================================
   PIPELINE-WIDE Ctrl+C HANDLING
   Pressing Ctrl+C mid-pipeline now signals every still-running agent
   to stop, instead of leaving orphaned children behind when the Main
   Controller alone reacted to the interrupt.
   ============================================================ */

static volatile sig_atomic_t g_pipeline_interrupted = 0;
static pid_t g_agent_pids[4];
static int g_agent_pid_count = 0;

static void handle_pipeline_sigint(int signum)
{
    (void)signum;

    g_pipeline_interrupted = 1;

    // for loop: forward the interrupt to every agent we've spawned so
    // far, instead of only stopping the Main Controller and leaving them
    // running as orphans
    for (int i = 0; i < g_agent_pid_count; i++)
    {
        if (g_agent_pids[i] > 0)
        {
            kill(g_agent_pids[i], SIGTERM);
        }
    }
}


/* ============================================================
   RUN HISTORY
   Appends one line per pipeline run to HISTORY_FILE, built from the
   verdict the Testing Agent left behind in LAST_RESULT_FILE - keeps a
   permanent, timestamped record across runs instead of only the
   scrolling on-screen summary.
   ============================================================ */

/* Reads back the verdict the Testing Agent left in LAST_RESULT_FILE.
   Shared by record_run_history(), run_all_demos()'s consolidated
   table, and the --run command-line mode's exit code - one place that
   knows the scratch file's "VERDICT|detail" format instead of three. */
static void read_last_verdict(char *verdict_out, size_t verdict_size,
                               char *detail_out, size_t detail_size)
{
    snprintf(verdict_out, verdict_size, "%s", "UNKNOWN");
    if (detail_out != NULL && detail_size > 0) detail_out[0] = '\0';

    FILE *f = fopen(LAST_RESULT_FILE, "r");
    if (f == NULL) return;

    char line[350];

    if (fgets(line, sizeof(line), f))
    {
        strip_newline(line);

        char *separator = strchr(line, '|');

        // if statement: the scratch file is well-formed
        // ("VERDICT|detail") - split it into the two pieces instead of
        // treating the whole raw line as the verdict
        if (separator != NULL)
        {
            *separator = '\0';
            snprintf(verdict_out, verdict_size, "%s", line);

            if (detail_out != NULL && detail_size > 0)
                snprintf(detail_out, detail_size, "%s", separator + 1);
        }
    }

    fclose(f);
}

static void record_run_history(void)
{
    const char *program_name =
        (g_program_choice == 6) ? "Custom Program" : PROGRAM_CATALOG[g_program_choice].task_name;

    char verdict[32];
    char detail[300];

    // if statement: the user interrupted the run - there may be no
    // (or a stale) verdict file, so record the interruption itself
    // rather than whatever leftover result happens to be sitting there
    if (g_pipeline_interrupted)
    {
        snprintf(verdict, sizeof(verdict), "%s", "INTERRUPTED");
        snprintf(detail, sizeof(detail), "%s", "Pipeline stopped by user (Ctrl+C)");
    }
    else
    {
        read_last_verdict(verdict, sizeof(verdict), detail, sizeof(detail));
    }

    time_t now = time(NULL);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", localtime(&now));

    int fd = open(HISTORY_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd == -1) return;

    char entry[512];
    int len = snprintf(entry, sizeof(entry), "[%s] %-24s %-12s %s\n",
                        timestamp, program_name, verdict, detail);

    ssize_t written = write(fd, entry, len);
    (void)written; /* best-effort - a missed write only affects the history log */

    close(fd);
}


/* ============================================================
   RUN COMPLETE MULTI-AGENT SYSTEM
   ============================================================ */

/* Runs one full pipeline - fork the four agents, monitor, wait, log
   the result - assuming g_program_choice (and, for choice 6,
   g_custom_source/g_custom_expected) are already set. Shared by the
   single-run menu option, the "Run All Demos" batch mode, and the
   --run command-line mode, so all three go through exactly the same
   pipeline logic instead of three slightly different copies of it. */
static void run_pipeline_once(void)
{
    printf("\n   %sPipeline:%s  Generator -> Reviewer -> Executor -> Tester\n",
           clr(C_BOLD), clr(C_RESET));


    g_agent_pid_count = 0;
    g_pipeline_interrupted = 0;


    /* ========================================================
       CREATE CODE GENERATOR PROCESS
       ======================================================== */

    /*
       fork()
       Creates a separate process representing
       the Code Generator Agent.
    */

    /* Belt-and-suspenders alongside the setvbuf(_IOLBF) in main(): flush
     * explicitly right before every agent fork so a stray partial line
     * (no trailing '\n', which line-buffering alone wouldn't flush)
     * can never be duplicated by the child when it later exit()s. */
    fflush(stdout);
    pid_t generator_pid = fork();


    if (generator_pid < 0)
    {
        print_fail("Generator process creation failed");
        perror("      reason");
        return;
    }


    if (generator_pid == 0)
    {
        code_generator();
    }

    g_agent_pids[g_agent_pid_count++] = generator_pid;


    /* ========================================================
       CREATE CODE REVIEW AGENT PROCESS
       ======================================================== */

    /*
       fork()
       Creates a separate Code Review Agent process.
    */

    fflush(stdout);
    pid_t reviewer_pid = fork();


    if (reviewer_pid < 0)
    {
        print_fail("Review process creation failed");
        perror("      reason");
        return;
    }


    if (reviewer_pid == 0)
    {
        code_review_agent();
    }

    g_agent_pids[g_agent_pid_count++] = reviewer_pid;


    /* ========================================================
       CREATE EXECUTION AGENT PROCESS
       ======================================================== */

    /*
       fork()
       Creates a separate Execution Agent process.
    */

    fflush(stdout);
    pid_t execution_pid = fork();


    if (execution_pid < 0)
    {
        print_fail("Execution process creation failed");
        perror("      reason");
        return;
    }


    if (execution_pid == 0)
    {
        execution_agent();
    }

    g_agent_pids[g_agent_pid_count++] = execution_pid;


    /* ========================================================
       CREATE TESTING AGENT PROCESS
       ======================================================== */

    /*
       fork()
       Creates a separate Testing Agent process.
    */

    fflush(stdout);
    pid_t tester_pid = fork();


    if (tester_pid < 0)
    {
        print_fail("Testing process creation failed");
        perror("      reason");
        return;
    }


    if (tester_pid == 0)
    {
        testing_agent();
    }

    g_agent_pids[g_agent_pid_count++] = tester_pid;


    /* ========================================================
       ARM Ctrl+C HANDLING
       Installed only now, after every agent already exists - so a
       Ctrl+C press signals real, already-running agents instead of an
       empty list, and the agents themselves keep their normal default
       signal behaviour (they were forked before this was registered).
       ======================================================== */

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_pipeline_sigint;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);


    /* ========================================================
       RESOURCE MONITORING
       ======================================================== */

    monitor_process(execution_pid);


    /* ========================================================
       WAIT FOR ALL AGENTS
       ======================================================== */

    /*
       waitpid()
       Makes the Main Controller wait for every
       agent process to finish, and records each one's
       exit status for the summary below.
    */

    int gen_status = 0, review_status = 0, exec_status = 0, test_status = 0;

    waitpid(generator_pid, &gen_status, 0);
    waitpid(reviewer_pid, &review_status, 0);
    waitpid(execution_pid, &exec_status, 0);
    waitpid(tester_pid, &test_status, 0);

    /* pipeline is over - go back to letting Ctrl+C behave normally at
       the menu instead of keeping this handler armed */
    signal(SIGINT, SIG_DFL);

    if (g_pipeline_interrupted)
    {
        printf("\n");
        print_warn("Pipeline interrupted by user - remaining agents were signalled to stop");
    }

    record_run_history();

    print_banner_box("PIPELINE SUMMARY", C_CYAN);

    print_pipeline_result("Code Generator", gen_status);
    print_pipeline_result("Code Review Agent", review_status);
    print_pipeline_result("Execution Agent", exec_status);
    print_pipeline_result("Testing Agent", test_status);
    printf("\n");

    log_activity("Main Controller",
                 "All AI Agents completed");
}


/* Gets the workspace ready and lets the user pick a program (including
   the custom-paste option), then hands off to run_pipeline_once(). */
void run_multi_agent_system(void)
{
    print_banner_box("STARTING MULTI-AGENT SYSTEM", C_CYAN);

    log_activity("Main Controller",
                 "Multi-Agent System execution started");


    /* ========================================================
       PREPARE WORKSPACE, LOG, AND IPC DIRECTORIES
       ======================================================== */

    printf("   Preparing workspace, log, and IPC directories...\n");

    ensure_environment();

    print_ok("Environment ready");


    printf("\n   Checking workspace access...\n");


    if (!validate_workspace_path(INPUT_DIR))
    {
        print_fail("Workspace is not accessible");

        return;
    }


    print_ok("Workspace is accessible");

    printf("\n");
    select_program_choice();

    run_pipeline_once();
}


/* ============================================================
   RUN ALL DEMOS
   Runs every catalog program (1-5) back to back with no further
   input needed, then prints one consolidated table - a quick way to
   demonstrate, or grade, the whole system in a single menu choice
   instead of five separate manual runs. Choice 6 (the pasted custom
   program) is deliberately left out, since typing source in is
   inherently interactive.
   ============================================================ */

void run_all_demos(void)
{
    print_banner_box("RUNNING ALL DEMOS", C_CYAN);

    log_activity("Main Controller",
                 "Run All Demos started");

    printf("   Preparing workspace, log, and IPC directories...\n");

    ensure_environment();

    print_ok("Environment ready");


    printf("\n   Checking workspace access...\n");

    if (!validate_workspace_path(INPUT_DIR))
    {
        print_fail("Workspace is not accessible");

        return;
    }

    print_ok("Workspace is accessible");

    char verdicts[6][32];

    // for loop: run each catalog program (1-5) exactly the way the
    // interactive menu would, one right after another - the whole
    // point is that no one has to sit at the keyboard between runs
    for (int choice = 1; choice <= 5; choice++)
    {
        g_program_choice = choice;

        printf("\n");
        print_section(PROGRAM_CATALOG[choice].task_name);

        run_pipeline_once();

        read_last_verdict(verdicts[choice], sizeof(verdicts[choice]), NULL, 0);
    }

    print_banner_box("ALL DEMOS - CONSOLIDATED RESULT", C_CYAN);

    // for loop: print one aligned, color-coded row per program so the
    // whole system's health shows up at a glance instead of having to
    // scroll back through five separate pipeline summaries
    for (int choice = 1; choice <= 5; choice++)
    {
        const char *color = C_WHITE;

        if (strcmp(verdicts[choice], "PASS") == 0) color = C_GREEN;
        else if (strcmp(verdicts[choice], "FAIL") == 0) color = C_RED;
        else if (strcmp(verdicts[choice], "SKIPPED") == 0) color = C_YELLOW;

        printf("   %-24s %s%-10s%s\n",
               PROGRAM_CATALOG[choice].task_name, clr(color), verdicts[choice], clr(C_RESET));
    }

    printf("\n");

    log_activity("Main Controller",
                 "Run All Demos completed");
}


/* ============================================================
   VIEW ACTIVITY LOG
   ============================================================ */

void view_activity_log(void)
{
    print_section("ACTIVITY LOG");

    /*
       fopen()
       Opens the activity log for reading.
    */

    FILE *file = fopen(LOG_FILE, "r");


    if (file == NULL)
    {
        print_warn("No activity log found yet");
        return;
    }


    char line[500];


    /*
       fgets()
       Reads and displays the log one line at a time.
    */

    while (fgets(line,
                 sizeof(line),
                 file))
    {
        printf("   %s", line);
    }


    fclose(file);

    print_rule();
}


/* ============================================================
   VIEW BACKUP FILES
   Lists every backup currently sitting in the workspace, since v2
   can generate several differently-named source files across
   different runs, not just one fixed factorial.c.
   ============================================================ */

void view_backup_file(void)
{
    print_section("BACKUP FILES");

    DIR *d = opendir(BACKUP_DIR);

    if (d == NULL)
    {
        print_warn("Backup directory not found - run the Multi-Agent System first");
        return;
    }

    int count = 0;
    struct dirent *entry;


    // while loop: walk every entry in the backup directory and print the
    // contents of each real backup file, skipping "." and ".."
    while ((entry = readdir(d)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        char full_path[PATH_MAX];
        snprintf(full_path, sizeof(full_path), "%s/%s", BACKUP_DIR, entry->d_name);

        FILE *file = fopen(full_path, "r");
        if (!file) continue;

        count++;

        printf("\n   %s%s%s\n", clr(C_BOLD), entry->d_name, clr(C_RESET));

        char line[500];

        while (fgets(line, sizeof(line), file))
        {
            printf("   %s", line);
        }

        fclose(file);
    }

    closedir(d);

    if (count == 0)
    {
        print_warn("No backup files found yet - run the Multi-Agent System first");
    }

    printf("\n");
    print_rule();
}


/* ============================================================
   VIEW RUN HISTORY
   Shows every past pipeline run recorded in HISTORY_FILE, plus a
   quick tally of how many passed, failed, were skipped, or were
   interrupted - a permanent record that survives across runs, unlike
   the on-screen pipeline summary which scrolls away.
   ============================================================ */

void view_run_history(void)
{
    print_section("RUN HISTORY");

    FILE *f = fopen(HISTORY_FILE, "r");

    if (f == NULL)
    {
        print_warn("No run history yet - run the Multi-Agent System first");
        return;
    }

    char line[512];
    int total = 0, passed = 0, failed = 0, skipped = 0, interrupted = 0;

    // while loop: print every recorded run and tally its verdict at the
    // same time, one pass over the file instead of reading it twice
    while (fgets(line, sizeof(line), f))
    {
        printf("   %s", line);

        total++;

        if (strstr(line, "PASS")) passed++;
        else if (strstr(line, "FAIL")) failed++;
        else if (strstr(line, "SKIPPED")) skipped++;
        else if (strstr(line, "INTERRUPTED")) interrupted++;
    }

    fclose(f);

    print_rule();
    printf("   Total: %d   %sPass: %d%s   %sFail: %d%s   Skipped: %d   Interrupted: %d\n",
           total,
           clr(C_GREEN), passed, clr(C_RESET),
           clr(C_RED), failed, clr(C_RESET),
           skipped, interrupted);
    print_rule();
}


/* ============================================================
   MENU
   ============================================================ */

void show_menu(void)
{
    print_banner_box("MAIN MENU", C_CYAN);

    printf("   %s1.%s Run Multi-Agent System\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s2.%s Run All Demos %s(all 5 catalog programs, back to back)%s\n",
           clr(C_BOLD), clr(C_RESET), clr(C_DIM), clr(C_RESET));
    printf("   %s3.%s View Activity Log\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s4.%s View Backup Files\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s5.%s View Run History\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s6.%s Reset Environment\n", clr(C_BOLD), clr(C_RESET));
    printf("   %s7.%s Exit\n", clr(C_BOLD), clr(C_RESET));

    printf("\n   Enter your choice: ");
}


/* ============================================================
   COMMAND-LINE USAGE
   Lets the pipeline run outside the interactive menu entirely - handy
   for a grading script or CI job that just wants one program run and
   a process exit code, not a person sitting at a keyboard.
   ============================================================ */

static void print_usage(const char *program_name)
{
    printf("Usage:\n");
    printf("  %s                Interactive menu (default)\n", program_name);
    printf("  %s --run <1-5>    Run one catalog program non-interactively and exit\n", program_name);
    printf("                       Exit code 0 = TEST RESULT: PASS, 1 = anything else\n");
    printf("  %s --help         Show this message\n", program_name);
}


/* ============================================================
   MAIN CONTROLLER
   ============================================================ */

int main(int argc, char *argv[])
{
    /* v4: force line-buffered stdout regardless of whether it's a
     * terminal. By default, libc fully-buffers stdout (large blocks,
     * flushed only when the buffer fills or the process exits) whenever
     * it's NOT connected to a tty - e.g. piped, redirected to a file,
     * or run via --run in a script. Combined with fork(), that's a
     * classic trap: each agent process below is a fork() that keeps
     * running this same binary's code and eventually calls exit() at
     * the end of its own function, which flushes stdio - including
     * whatever of the PARENT's output was still sitting unflushed in
     * the (duplicated-by-fork) buffer at the moment fork() was called.
     * The result is every not-yet-flushed line getting printed again
     * by each child. Forcing line buffering here (before ensure_environment()
     * and any fork() ever happens) means every printf ending in '\n' is
     * flushed immediately, so there's essentially nothing left over for
     * a child to duplicate - matching the already-correct behavior seen
     * when stdout happens to be a real terminal. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    ui_init();

    /* Set up the fifo, workspace, and logs directories (plus a default
     * example source file) right away, before the very first log_activity()
     * call - otherwise that call would fail because logs/ doesn't exist yet. */
    ensure_environment();

    /* Only one instance may touch this workspace's FIFOs and files at
       once - exits immediately (before anything else happens) if
       another copy is already running here. */
    acquire_instance_lock_or_exit();

    load_config();

    // if statement: "--run N" / "--help" let this be driven from a
    // script or grading harness without ever showing the interactive
    // menu - checked before the menu loop so it can return/exit right away
    if (argc >= 2)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)
        {
            print_usage(argv[0]);
            return 0;
        }

        if (strcmp(argv[1], "--run") == 0)
        {
            if (argc < 3)
            {
                fprintf(stderr, "Missing program number.\n\n");
                print_usage(argv[0]);
                return 1;
            }

            int choice = atoi(argv[2]);

            if (choice < 1 || choice > 5)
            {
                fprintf(stderr,
                        "Invalid program number '%s' - only 1-5 (catalog programs) can run "
                        "non-interactively. Use the interactive menu for a custom program.\n",
                        argv[2]);
                return 1;
            }

            g_program_choice = choice;

            print_banner_box("MULTI-AGENT AI EXECUTION SYSTEM", C_CYAN);

            if (!validate_workspace_path(INPUT_DIR))
            {
                return 1;
            }

            run_pipeline_once();

            char verdict[32];
            read_last_verdict(verdict, sizeof(verdict), NULL, 0);

            return (strcmp(verdict, "PASS") == 0) ? 0 : 1;
        }

        fprintf(stderr, "Unrecognized option '%s'.\n\n", argv[1]);
        print_usage(argv[0]);
        return 1;
    }

    int choice;

    print_banner_box("MULTI-AGENT AI EXECUTION SYSTEM", C_CYAN);
    printf("   Main Controller started (PID %d)\n", getpid());

    log_activity("Main Controller",
                 "Main Controller started");


    /* ========================================================
       MENU LOOP
       ======================================================== */

    while (1)
    {
        show_menu();


        /*
           scanf()
           Reads the user's menu choice.
        */

        if (scanf("%d", &choice) != 1)
        {
            // if statement: scanf() only fails this way with no more
            // input coming (stdin closed or redirected from an
            // exhausted file) - exit cleanly instead of falling through
            // to the getchar() loop below, which would otherwise spin
            // forever since getchar() keeps returning EOF, never '\n'
            if (feof(stdin))
            {
                printf("\n");
                print_warn("Input stream closed - exiting");

                log_activity("Main Controller",
                             "System exited (stdin closed)");

                return 0;
            }

            print_warn("Invalid input - please enter a number");


            /*
               getchar()
               Clears invalid (non-EOF) input from the input buffer.
            */

            int junk;
            while ((junk = getchar()) != '\n' && junk != EOF);

            continue;
        }


        /*
           switch()
           Selects the requested menu operation.
        */

        switch (choice)
        {
            case 1:

                /*
                   Runs the complete multi-agent workflow
                   for one, interactively chosen program.
                */

                run_multi_agent_system();

                break;


            case 2:

                /*
                   Runs every catalog program back to back
                   and shows one consolidated result table.
                */

                run_all_demos();

                break;


            case 3:

                /*
                   Displays automatically generated
                   agent activity and execution logs.
                */

                view_activity_log();

                break;


            case 4:

                /*
                   Displays every backup copy currently
                   sitting in the workspace.
                */

                view_backup_file();

                break;


            case 5:

                /*
                   Displays every past pipeline run and a
                   pass/fail/skipped/interrupted tally.
                */

                view_run_history();

                break;


            case 6:

                /*
                   Wipes generated source/output/backup/log
                   files and rebuilds a clean environment.
                */

                reset_environment();

                break;


            case 7:

                print_banner_box("EXITING SYSTEM", C_CYAN);

                log_activity("Main Controller",
                             "System exited");


                /*
                   return 0
                   Terminates the Main Controller normally.
                */

                return 0;


            default:

                print_warn("Invalid choice - please select 1, 2, 3, 4, 5, 6, or 7");
        }
    }


    return 0;
}
