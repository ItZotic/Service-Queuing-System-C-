#include <iostream>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <random>
#include <chrono>
#include <functional>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <map>
// ──────────────────────────────────────────────
// Task / Job abstraction
// ──────────────────────────────────────────────
struct Task {
int id;
int priority; // 1 = high, 3 = low
double service_time_ms; // simulated work duration
std::string type; // "CPU" | "IO" | "NET"
std::chrono::steady_clock::time_point enqueue_time;
std::chrono::steady_clock::time_point start_time;
std::chrono::steady_clock::time_point finish_time;
};
// ──────────────────────────────────────────────
// Lock-free statistics collector
// ──────────────────────────────────────────────
struct Stats {
std::atomic<long long> total_tasks{0};
std::atomic<long long> completed{0};
std::atomic<long long> total_wait_us{0}; // microseconds
std::atomic<long long> total_service_us{0};
std::atomic<long long> throughput_ticks{0};
std::mutex log_mutex;
std::vector<std::string> log;
void record(const Task& t) {
auto wait_us = std::chrono::duration_cast<std::chrono::microseconds>(
t.start_time - t.enqueue_time).count();
auto svc_us = std::chrono::duration_cast<std::chrono::microseconds>(
t.finish_time - t.start_time).count();
total_wait_us += wait_us;
total_service_us += svc_us;
completed++;
}
void add_log(const std::string& msg) {
std::lock_guard<std::mutex> lk(log_mutex);
log.push_back(msg);
if (log.size() > 200) log.erase(log.begin());
}
};
// ──────────────────────────────────────────────
// Priority-aware thread-safe queue
// ──────────────────────────────────────────────
class PriorityQueue {
public:
struct Entry {
int priority;
Task task;
bool operator>(const Entry& o) const { return priority > o.priority; }
};
void push(Task t) {
{
std::lock_guard<std::mutex> lk(mtx_);
pq_.push({t.priority, std::move(t)});
size_++;
}
cv_.notify_one();
}
bool try_pop(Task& out, int timeout_ms = 50) {
std::unique_lock<std::mutex> lk(mtx_);
if (!cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms),
[&]{ return !pq_.empty() || stopped_; }))
return false;
if (pq_.empty()) return false;
out = std::move(const_cast<Task&>(pq_.top().task));
pq_.pop();
size_--;
return true;
}
void stop() { stopped_ = true; cv_.notify_all(); }
int size() const { return size_.load(); }
bool empty() const { return size_.load() == 0; }
private:
std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> pq_;
mutable std::mutex mtx_;
std::condition_variable cv_;
std::atomic<int> size_{0};
std::atomic<bool> stopped_{false};
};
// ──────────────────────────────────────────────
// Worker thread pool
// ──────────────────────────────────────────────
class WorkerPool {
public:
WorkerPool(int n_workers, PriorityQueue& q, Stats& stats)
: queue_(q), stats_(stats), running_(true)
{
for (int i = 0; i < n_workers; ++i) {
workers_.emplace_back([this, i]{ worker_loop(i); });
worker_busy_[i] = false;
}
}
~WorkerPool() {
running_ = false;
for (auto& t : workers_) if (t.joinable()) t.join();
}
std::map<int,bool> worker_status() {
std::lock_guard<std::mutex> lk(busy_mtx_);
return worker_busy_;
}
private:
void worker_loop(int id) {
Task task;
while (running_) {
if (!queue_.try_pop(task, 30)) continue;
// Mark busy
{ std::lock_guard<std::mutex> lk(busy_mtx_); worker_busy_[id] = true; }
task.start_time = std::chrono::steady_clock::now();
// Log start
{
std::ostringstream ss;
ss << "[W" << id << "] START T#" << task.id
<< " (" << task.type << ", P" << task.priority
<< ", " << std::fixed << std::setprecision(1)
<< task.service_time_ms << "ms)";
stats_.add_log(ss.str());
}
// Simulate work
std::this_thread::sleep_for(
std::chrono::microseconds(
static_cast<long long>(task.service_time_ms * 1000)));
task.finish_time = std::chrono::steady_clock::now();
stats_.record(task);
// Log finish
{
auto wait_ms = std::chrono::duration_cast<std::chrono::microseconds>(
task.start_time - task.enqueue_time).count() / 1000.0;
std::ostringstream ss;
ss << "[W" << id << "] DONE T#" << task.id
<< " wait=" << std::fixed << std::setprecision(2) << wait_ms << "ms";
stats_.add_log(ss.str());
}
// Mark idle
{ std::lock_guard<std::mutex> lk(busy_mtx_); worker_busy_[id] = false; }
stats_.throughput_ticks++;
}
}
PriorityQueue& queue_;
Stats& stats_;
std::atomic<bool> running_;
std::vector<std::thread> workers_;
std::map<int,bool> worker_busy_;
std::mutex busy_mtx_;
};
// ──────────────────────────────────────────────
// Task generator (producer)
// ──────────────────────────────────────────────
class TaskGenerator {
public:
TaskGenerator(PriorityQueue& q, Stats& stats,
double arrival_rate_hz, // tasks per second
std::atomic<bool>& stop_flag)
: queue_(q), stats_(stats),
rate_hz_(arrival_rate_hz), stop_(stop_flag),
rng_(std::random_device{}()),
type_dist_(0,2),
priority_dist_(1,3),
svc_dist_(5.0, 60.0), // ms
next_id_(1)
{}
void run() {
while (!stop_) {
Task t;
t.id = next_id_++;
t.priority = priority_dist_(rng_);
t.service_time_ms = svc_dist_(rng_);
t.enqueue_time = std::chrono::steady_clock::now();
static const char* types[] = {"CPU","IO","NET"};
t.type = types[type_dist_(rng_)];
queue_.push(t);
stats_.total_tasks++;
std::ostringstream ss;
ss << "[GEN] Enqueued T#" << t.id
<< " type=" << t.type
<< " priority=" << t.priority
<< " queue_size=" << queue_.size();
stats_.add_log(ss.str());
// Poisson inter-arrival
double interval_ms = 1000.0 / rate_hz_;
std::this_thread::sleep_for(
std::chrono::microseconds(
static_cast<long long>(interval_ms * 1000)));
}
}
private:
PriorityQueue& queue_;
Stats& stats_;
double rate_hz_;
std::atomic<bool>& stop_;
std::mt19937 rng_;
std::uniform_int_distribution<int> type_dist_, priority_dist_;
std::uniform_real_distribution<double> svc_dist_;
std::atomic<int> next_id_;
};
// ──────────────────────────────────────────────
// Pretty-print summary
// ──────────────────────────────────────────────
void print_summary(const Stats& s, double elapsed_sec, int n_workers) {
long long done = s.completed.load();
double avg_wait = done > 0 ? s.total_wait_us.load() / 1000.0 / done : 0;
double avg_svc = done > 0 ? s.total_service_us.load() / 1000.0 / done : 0;
double tput = done / elapsed_sec;
std::cout << "\n╔══════════════════════════════════════╗\n";
std::cout << "║ PARALLEL QUEUE SIMULATION REPORT ║\n";
std::cout << "╠══════════════════════════════════════╣\n";
std::cout << "║ Workers : " << std::setw(17) << n_workers << " ║\n";
std::cout << "║ Total submitted : " << std::setw(17) << s.total_tasks << " ║\n";
std::cout << "║ Completed : " << std::setw(17) << done << " ║\n";
std::cout << "║ Elapsed (s) : " << std::setw(17) << std::fixed
<< std::setprecision(2) << elapsed_sec << " ║\n";
std::cout << "║ Throughput(t/s) : " << std::setw(17) << tput << " ║\n";
std::cout << "║ Avg wait (ms) : " << std::setw(17) << avg_wait << " ║\n";
std::cout << "║ Avg svc (ms) : " << std::setw(17) << avg_svc << " ║\n";
std::cout << "╚══════════════════════════════════════╝\n";
}
// ──────────────────────────────────────────────
// main
// ──────────────────────────────────────────────
int main(int argc, char* argv[]) {
int n_workers = (argc > 1) ? std::stoi(argv[1]) : 8;
double arrival_hz = (argc > 2) ? std::stod(argv[2]) : 20.0;
int duration_sec = (argc > 3) ? std::stoi(argv[3]) : 5;
std::cout << "▶ Parallel Queue Simulation\n"
<< " Workers : " << n_workers << "\n"
<< " Arrival : " << arrival_hz << " tasks/s\n"
<< " Runtime : " << duration_sec << "s\n\n";
PriorityQueue queue;
Stats stats;
std::atomic<bool> stop_flag{false};
// Start worker pool (uses std::thread internally — OpenMP optional)
WorkerPool pool(n_workers, queue, stats);
// Start producer thread
TaskGenerator gen(queue, stats, arrival_hz, stop_flag);
std::thread producer([&]{ gen.run(); });
// Run for duration_sec
auto t0 = std::chrono::steady_clock::now();
std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
stop_flag = true;
queue.stop();
producer.join();
// Drain remaining tasks (give workers 2s to finish)
std::this_thread::sleep_for(std::chrono::seconds(2));
double elapsed = std::chrono::duration<double>(
std::chrono::steady_clock::now() - t0).count();
// Print last 30 log lines
std::cout << "\n── Recent Activity ──────────────────────\n";
{
std::lock_guard<std::mutex> lk(stats.log_mutex);
int start = std::max(0, (int)stats.log.size() - 30);
for (int i = start; i < (int)stats.log.size(); ++i)
std::cout << stats.log[i] << "\n";
}
print_summary(stats, elapsed, n_workers);
return 0;
}
