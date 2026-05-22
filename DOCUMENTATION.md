# Technical Documentation

## parallel_queue_sim.cpp

-----

## Table of Contents

1. [Overview](#overview)
1. [Parallel Framework](#parallel-framework)
1. [Components](#components)
- [Task](#task)
- [Stats](#stats)
- [PriorityQueue](#priorityqueue)
- [WorkerPool](#workerpool)
- [TaskGenerator](#taskgenerator)
1. [Concurrency Model](#concurrency-model)
1. [Configuration & Tuning](#configuration--tuning)
1. [Performance Notes](#performance-notes)

-----

## Overview

`parallel_queue_sim.cpp` simulates a multi-server queueing system (analogous to an M/G/c queue in queueing theory). A single producer thread generates tasks at a configurable rate and places them into a shared priority queue. A pool of worker threads concurrently dequeues and processes those tasks. All coordination is done with standard C++17 primitives — no external libraries.

-----

## Parallel Framework

The simulation uses **C++17 standard threading primitives** as its parallel framework:

|Primitive                |Used For                                                                                           |
|-------------------------|---------------------------------------------------------------------------------------------------|
|`std::thread`            |Worker threads and the producer thread                                                             |
|`std::mutex`             |Protecting the priority queue and the log buffer                                                   |
|`std::condition_variable`|Blocking workers when the queue is empty; waking them on new arrivals                              |
|`std::atomic<T>`         |Lock-free counters for statistics (completed count, total enqueued, wait/service time accumulators)|
|`std::unique_lock`       |RAII lock for `condition_variable::wait_for`                                                       |
|`std::lock_guard`        |RAII lock for short critical sections (push, log write)                                            |

This gives the program thread-safe producer/consumer coordination without busy-waiting.

-----

## Components

### Task

```cpp
struct Task {
    int          id;
    int          priority;          // 1 = high, 3 = low
    double       service_time_ms;
    std::string  type;              // "CPU" | "IO" | "NET"
    std::chrono::steady_clock::time_point enqueue_time;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point finish_time;
};
```

A plain data struct holding all per-task metadata. Timestamps use `steady_clock` (monotonic) to avoid wall-clock skew. Priority follows the convention that **lower number = higher priority** (P1 is served before P3).

-----

### Stats

```cpp
struct Stats {
    std::atomic<long long> total_tasks;
    std::atomic<long long> completed;
    std::atomic<long long> total_wait_us;
    std::atomic<long long> total_service_us;
    std::atomic<long long> throughput_ticks;
    std::mutex             log_mutex;
    std::vector<std::string> log;

    void record(const Task& t);
    void add_log(const std::string& msg);
};
```

`Stats` is shared across all threads. Numeric counters use `std::atomic` so workers can update them concurrently without a mutex. The rolling log buffer (`std::vector<std::string>`) does require a mutex because `std::vector` is not thread-safe; it is capped at 200 entries.

**`record(task)`** — called by each worker on completion. Computes wait time (`start - enqueue`) and service time (`finish - start`) in microseconds and accumulates them atomically.

-----

### PriorityQueue

```cpp
class PriorityQueue {
public:
    void push(Task t);
    bool try_pop(Task& out, int timeout_ms = 50);
    void stop();
    int  size() const;
    bool empty() const;
private:
    std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> pq_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::atomic<int>  size_;
    std::atomic<bool> stopped_;
};
```

A thread-safe wrapper around `std::priority_queue`. The internal heap uses `std::greater<Entry>` so the entry with the lowest `priority` value sits at the top (min-heap behaviour).

**`push(task)`**

1. Acquires `mtx_` via `lock_guard`.
1. Pushes the task wrapped in an `Entry{priority, task}`.
1. Increments the atomic `size_`.
1. Calls `cv_.notify_one()` to wake a waiting worker.

**`try_pop(out, timeout_ms)`**

1. Acquires `mtx_` via `unique_lock`.
1. Calls `cv_.wait_for(lock, timeout, predicate)` — the predicate checks `!pq_.empty() || stopped_`. This releases the lock and puts the thread to sleep until a task arrives or the timeout expires.
1. If a task is available, moves it into `out`, decrements `size_`, and returns `true`.
1. Returns `false` on timeout or if stopped with an empty queue.

**`stop()`** — sets `stopped_ = true` and calls `cv_.notify_all()` so all sleeping workers wake up and can exit cleanly.

-----

### WorkerPool

```cpp
class WorkerPool {
public:
    WorkerPool(int n_workers, PriorityQueue& q, Stats& stats);
    ~WorkerPool();
    std::map<int,bool> worker_status();
private:
    void worker_loop(int id);
    ...
};
```

The constructor spawns `n_workers` threads, each running `worker_loop(id)`.

**`worker_loop(id)`** — the main per-worker logic:

```
while running:
    try_pop(task, 30ms timeout)   ← blocks here when queue is empty
    if no task: continue

    mark worker busy
    log "START T#id ..."
    sleep(task.service_time_ms)   ← simulates real work
    record completion in Stats
    log "DONE  T#id ..."
    mark worker idle
```

The `sleep_for` call simulates actual work duration. In a real system this would be replaced with CPU computation, I/O calls, or network requests.

The destructor sets `running_ = false` and joins all threads, ensuring clean shutdown.

-----

### TaskGenerator

```cpp
class TaskGenerator {
public:
    TaskGenerator(PriorityQueue& q, Stats& stats,
                  double arrival_rate_hz,
                  std::atomic<bool>& stop_flag);
    void run();
private:
    std::mt19937 rng_;
    std::uniform_int_distribution<int> type_dist_, priority_dist_;
    std::uniform_real_distribution<double> svc_dist_;
    ...
};
```

Runs on a dedicated thread. Each iteration:

1. Creates a `Task` with randomly sampled `type` (CPU/IO/NET), `priority` (1–3), and `service_time_ms` (5–60ms) from a uniform distribution.
1. Pushes the task onto the queue.
1. Sleeps for `(1000 / arrival_rate_hz) ms` to approximate a Poisson arrival process.

The stop flag (`std::atomic<bool>`) is checked at the top of each loop iteration for clean shutdown without a join timeout.

-----

## Concurrency Model

```
Thread: Producer (TaskGenerator::run)
  └─ push() → acquires mtx_, inserts into heap, releases mtx_, notifies cv_

Thread: Worker 0..N (WorkerPool::worker_loop)
  └─ try_pop() → blocks on cv_ until task available or timeout
  └─ processes task (sleep)
  └─ Stats::record() → atomic increments only, no lock needed
  └─ Stats::add_log() → acquires log_mutex, appends string, releases

Shared state:
  PriorityQueue.pq_   → protected by PriorityQueue::mtx_
  PriorityQueue.size_ → std::atomic (read without lock for monitoring)
  Stats.*_us / completed / total_tasks → std::atomic<long long>
  Stats.log           → protected by Stats::log_mutex
```

There are no deadlocks: each mutex protects a single resource, no thread holds two mutexes simultaneously, and `condition_variable::wait_for` releases its lock while sleeping.

-----

## Configuration & Tuning

|Parameter        |CLI Position|Default|Effect                                                   |
|-----------------|------------|-------|---------------------------------------------------------|
|`n_workers`      |argv[1]     |8      |More workers → lower wait time up to queue saturation    |
|`arrival_rate_hz`|argv[2]     |20     |Higher rate → longer queue depth if workers can’t keep up|
|`duration_sec`   |argv[3]     |5      |Longer runs give more stable throughput averages         |

**Little’s Law check** — at steady state, average queue depth ≈ arrival_rate × avg_wait_time. You can verify the simulation is behaving correctly with this relationship.

**Saturation point** — the system saturates when `arrival_rate > n_workers / avg_service_time`. For default service times (~32ms avg), saturation occurs around `n_workers / 0.032 ≈ 31 tasks/s` with 1 worker. With 8 workers the saturation point is ~250 tasks/s.

-----

## Performance Notes

- **Atomic vs mutex overhead** — statistics use atomics (`fetch_add`) which are 1–2 orders of magnitude faster than a mutex for simple increments.
- **`wait_for` timeout** — the 30ms timeout in `try_pop` means workers poll at most ~33 times/second when idle. This is a tunable tradeoff between responsiveness and CPU burn.
- **Service time granularity** — `sleep_for` on most systems has ~1ms resolution. Times below ~2ms may be less accurate.
- **Log mutex contention** — the log is written on every task start and finish. At very high throughput (>1000 tasks/s) this mutex can become a bottleneck. To remove it, replace with a lock-free ring buffer or reduce log verbosity.