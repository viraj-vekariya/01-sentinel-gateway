// Sentinel Gateway - native rate limiting core.
//
// Why this exists in C++ at all: the gateway's hot path takes the limiter lock on
// EVERY request. In pure Python that path holds the GIL, so N worker threads
// serialise through it and the limiter becomes the throughput ceiling. This
// translation unit is compiled as a Python extension whose entry points release
// the GIL, so the lock contention becomes real parallel work instead of GIL
// contention. bench/bench_ratelimit.py measures the difference.
//
// Two algorithms are implemented, not one, because the choice between them is a
// real design decision the gateway exposes as config (see DECISIONS.md D-02).

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace sentinel {

// Monotonic seconds since process start. Deliberately NOT wall clock: NTP steps
// and DST would otherwise let a client mint free tokens by waiting for a jump.
double monotonic_now();

struct Decision {
    bool allowed;
    double tokens_remaining;   // tokens left after this call (token bucket)
    double retry_after;        // seconds until the request would be allowed; 0 if allowed
    std::uint64_t observed;    // requests seen in the current window (sliding window)
};

// ---------------------------------------------------------------------------
// Token bucket
// ---------------------------------------------------------------------------
// Lazy refill: we do not run a background timer thread topping every bucket up.
// Instead each bucket stores the timestamp of its last touch and, on access,
// credits (now - last) * rate tokens capped at `capacity`. Cost is O(1) per
// request and zero when a client is idle, which matters because the registry
// holds one bucket per client key and most of them are idle at any instant.
class TokenBucket {
public:
    TokenBucket(double capacity, double refill_per_sec, double now);

    // Attempt to spend `cost` tokens. Mutates the bucket only when allowed.
    Decision try_consume(double cost, double now);

    double capacity() const { return capacity_; }
    double refill_per_sec() const { return refill_per_sec_; }
    double peek_tokens(double now) const;
    double last_seen() const { return last_refill_; }

private:
    void refill(double now);

    double capacity_;
    double refill_per_sec_;
    double tokens_;
    double last_refill_;
};

// ---------------------------------------------------------------------------
// Sliding window counter
// ---------------------------------------------------------------------------
// The approximation used by most CDNs: keep the count for the current fixed
// window and the previous one, then interpolate the previous window's count by
// how much of it still overlaps the trailing period. Uses O(1) memory per client
// versus O(limit) for an exact log of timestamps, and never lets a client fire
// 2x the limit across a window boundary the way a naive fixed counter does.
class SlidingWindowCounter {
public:
    SlidingWindowCounter(std::uint64_t limit, double window_sec, double now);

    Decision try_consume(double now);

    std::uint64_t limit() const { return limit_; }
    double window_sec() const { return window_sec_; }
    double estimate(double now) const;
    double last_seen() const { return window_start_; }

private:
    void roll(double now);

    std::uint64_t limit_;
    double window_sec_;
    std::uint64_t current_count_;
    std::uint64_t previous_count_;
    double window_start_;
};

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------
// One limiter per client key, sharded across `stripes` independent mutexes.
// A single global mutex would make every worker thread contend on one lock and
// hand back exactly the serialisation we came here to remove; striping means
// two requests for different clients almost always take different locks.
// Stripe count is fixed at construction so a key's stripe never moves.
class Registry {
public:
    enum class Algorithm { TokenBucket, SlidingWindow };

    Registry(Algorithm algo, double capacity, double refill_per_sec,
             std::size_t stripes = 16);

    // Hot path. Called once per request, with the GIL released by the binding.
    Decision check(const std::string& key, double cost = 1.0);

    // Batch variant: one GIL release for the whole vector. Used by the benchmark
    // to isolate per-call binding overhead from the actual limiter cost.
    std::vector<Decision> check_many(const std::vector<std::string>& keys,
                                     double cost = 1.0);

    // Drop buckets untouched for longer than `max_idle_sec`. Without this the
    // registry is an unbounded map keyed by client-controlled input, which is a
    // memory-exhaustion vector, not merely a leak.
    std::size_t evict_idle(double max_idle_sec);

    std::size_t size() const;
    std::uint64_t total_allowed() const { return allowed_.load(std::memory_order_relaxed); }
    std::uint64_t total_denied() const { return denied_.load(std::memory_order_relaxed); }
    void reset_stats();

private:
    struct Stripe {
        mutable std::mutex mu;
        std::unordered_map<std::string, TokenBucket> buckets;
        std::unordered_map<std::string, SlidingWindowCounter> windows;
    };

    Stripe& stripe_for(const std::string& key);
    const Stripe& stripe_for(const std::string& key) const;

    Algorithm algo_;
    double capacity_;
    double refill_per_sec_;
    std::vector<Stripe> stripes_;
    std::hash<std::string> hasher_;
    std::atomic<std::uint64_t> allowed_{0};
    std::atomic<std::uint64_t> denied_{0};
};

}  // namespace sentinel
