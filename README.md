# Parallel Queueing Simulation

A high-performance, multi-threaded task queueing simulator written in C++17. Demonstrates concurrent producer/consumer patterns using a lock-free priority queue, a configurable thread pool, and real-time statistics collection.

## Features

- **Priority-aware scheduling** — tasks with lower priority numbers are served first (P1 > P2 > P3)
- **Thread-safe queue** — uses `std::mutex` + `std::condition_variable`; workers block instead of spin
- **Configurable worker pool** — spawn 1–N worker threads competing for tasks in parallel
- **Poisson arrival generator** — a dedicated producer thread enqueues tasks at a configurable rate
- **Lock-free statistics** — throughput, wait times, and service times tracked via `std::atomic`
- **Live activity log** — rolling window of the last 200 events printed to stdout
- **Summary report** — formatted table of key metrics after the run completes

## Requirements

|Dependency  |Version                                  |
|------------|-----------------------------------------|
|C++ compiler|C++17 or later                           |
|pthreads    |POSIX threads (Linux/macOS) or equivalent|

No third-party libraries required.

## Build

```bash
g++ -std=c++17 -O2 -pthread -o sim parallel_queue_sim.cpp
```

On Windows (MSVC):

```bash
cl /std:c++17 /O2 parallel_queue_sim.cpp
```

## Usage

```bash
./sim [workers] [arrival_rate] [duration_sec]
```

|Argument      |Default|Description                         |
|--------------|-------|------------------------------------|
|`workers`     |`8`    |Number of worker threads            |
|`arrival_rate`|`20`   |Task arrival rate (tasks per second)|
|`duration_sec`|`5`    |How long to run the simulation      |

### Examples

```bash
# Default: 8 workers, 20 tasks/s, 5 seconds
./sim

# High load: 4 workers, 40 tasks/s, 10 seconds (queue will back up)
./sim 4 40 10

# Overprovisioned: 16 workers, 5 tasks/s, 8 seconds (low utilization)
./sim 16 5 8
```

### Sample Output

```
▶  Parallel Queue Simulation
   Workers : 8
   Arrival : 20 tasks/s
   Runtime : 5s

── Recent Activity ──────────────────────
[GEN] Enqueued T#1 type=CPU priority=2 queue_size=1
[W00] START T#1 (CPU, P2, 43.2ms)
[GEN] Enqueued T#2 type=IO  priority=1 queue_size=1
[W01] START T#2 (IO,  P1, 18.7ms)
...

╔══════════════════════════════════════╗
║   PARALLEL QUEUE SIMULATION REPORT  ║
╠══════════════════════════════════════╣
║  Workers         :                 8 ║
║  Total submitted :               102 ║
║  Completed       :               102 ║
║  Elapsed (s)     :              7.01 ║
║  Throughput(t/s) :             14.55 ║
║  Avg wait  (ms)  :              0.03 ║
║  Avg svc   (ms)  :             31.20 ║
╚══════════════════════════════════════╝
```

## Architecture

```
Producer Thread
     │
     ▼  enqueue (mutex + condvar)
┌─────────────────────┐
│   PriorityQueue     │  ← min-heap, sorted by task.priority
└─────────────────────┘
     │
     ▼  try_pop (blocking with timeout)
┌────────────────────────────────────┐
│           WorkerPool               │
│  [W0] [W1] [W2] ... [Wn]          │  ← N std::thread workers
└────────────────────────────────────┘
     │
     ▼
┌─────────────────────┐
│   Stats (atomic)    │  ← wait_us, service_us, completed count
└─────────────────────┘
```

## Task Types

Each task has a randomly assigned type that is informational (all types share the same queue and workers):

|Type |Description            |
|-----|-----------------------|
|`CPU`|Compute-bound work     |
|`IO` |Input/output-bound work|
|`NET`|Network-bound work     |

Service time is drawn from a uniform distribution between 5ms and 60ms.

## License

MIT