# Phantasm
Phantasm is a multithreaded operating system simulator written in C that models process scheduling, synchronization, zombie processes, and concurrent thread coordination entirely in userspace using mutexes and condition variables.

> A concurrent process manager simulator that brings OS internals to life — fork, wait, kill, and zombie reaping, all running across real POSIX threads.

---

## What It Is

Phantasm simulates the process management subsystem of an operating system entirely in userspace. It maintains a live process table of up to 64 simulated processes, each with its own PCB (Process Control Block), state machine, and parent-child relationships. Multiple worker threads drive the simulation concurrently, each reading a script of commands, while a dedicated monitor thread silently watches for every table change and logs a snapshot — without ever polling.

It is written in a single C file with no external dependencies beyond `pthreads`.

---

## How It Works

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  Worker  0  │   │  Worker  1  │   │  Worker  N  │
│ thread0.txt │   │ thread1.txt │   │ threadN.txt │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                 │                 │
       └─────────────────┼─────────────────┘
                         │  concurrent ops
                         ▼
              ┌──────────────────────┐
              │   Process Table      │  ← guarded by table_mutex
              │  PCB[0] … PCB[63]    │
              └──────────┬───────────┘
                         │  signal on change
                         ▼
              ┌──────────────────────┐
              │   Monitor Thread     │  ← sleeps on condition variable
              │   snapshots.txt      │  ← one snapshot per operation
              └──────────────────────┘
```

Every operation — `fork`, `exit`, `wait`, `kill` — acquires the table mutex, mutates state, then signals the monitor. The monitor wakes, writes a labelled snapshot, and goes back to sleep. No polling, ever.

---

## Process Lifecycle

```
fork()
  └──► RUNNING
          │
    exit() / kill()
          │
          ▼
        ZOMBIE ──── parent calls wait() ───► TERMINATED
                                               (slot freed)

RUNNING ──── wait() with no zombie yet ───► BLOCKED
BLOCKED ──── child exits ─────────────────► RUNNING
```

---

## Build

```bash
gcc -Wall -Wextra -pthread -o phantasm pm_sim.c
```

Requires a C99-compatible compiler and POSIX threads. No third-party libraries.

---

## Usage

```bash
./phantasm thread0.txt thread1.txt thread2.txt ...
```

Each argument is a script file. Phantasm spawns one worker thread per file, numbered from 0. Pass as many scripts as you need.

---

## Script Commands

| Command | Syntax | Effect |
|---|---|---|
| `fork` | `fork <ppid>` | Create a new child process under the given parent PID |
| `exit` | `exit <pid> <status>` | Terminate a process; records exit status, transitions to ZOMBIE |
| `wait` | `wait <ppid> <cpid>` | Block parent until child exits and reap it; use `-1` to wait for any child |
| `kill` | `kill <pid>` | Force-terminate a process (equivalent to exit with status 0) |
| `sleep` | `sleep <ms>` | Pause the worker thread for the given number of milliseconds |

Blank lines are ignored.

**Example — `thread0.txt`**
```
fork 1
sleep 200
fork 1
wait 1 -1
```

**Example — `thread1.txt`**
```
sleep 100
fork 1
sleep 300
exit 2 10
```

Run them together:
```bash
./phantasm thread0.txt thread1.txt
```

---

## Output

### Terminal

When all workers finish, the final process table is printed:

```
Final Process Table:
PID     PPID    STATE           EXIT_STATUS
----------------------------------------------
1       0       RUNNING         -
3       1       RUNNING         -

Simulation complete. Snapshots saved to snapshots.txt
```

### snapshots.txt

A chronological log with one labelled snapshot per operation:

```
Initial Process Table
PID     PPID    STATE           EXIT_STATUS
----------------------------------------------
1       0       RUNNING         -

Thread 0 calls pm_fork 1
PID     PPID    STATE           EXIT_STATUS
----------------------------------------------
1       0       RUNNING         -
2       1       RUNNING         -

Thread 1 calls pm_exit 2 10
PID     PPID    STATE           EXIT_STATUS
----------------------------------------------
1       0       RUNNING         -
2       1       ZOMBIE          10

Thread 0 calls pm_wait 1 -1
PID     PPID    STATE           EXIT_STATUS
----------------------------------------------
1       0       RUNNING         -
```

---

## Concurrency Model

| Primitive | Where | Purpose |
|---|---|---|
| `table_mutex` | `ProcessTable` | Serialises all reads and writes to the process table |
| `wait_mutex` + `wait_cond` | Per `PCB` | Blocks a parent in `pm_wait` until a child calls `pm_exit` |
| `monitor_mutex` + `monitor_cond` | Global | Wakes the monitor thread when any operation modifies the table |

Lock ordering is always `table_mutex` → `wait_mutex`, eliminating circular-wait deadlocks.

---

## Limits

| Parameter | Value |
|---|---|
| Max simultaneous processes | 64 |
| Max children per process | 64 |
| Max script line length | 256 characters |
| PIDs | Monotonically increasing, not reused |

---

## License

MIT
