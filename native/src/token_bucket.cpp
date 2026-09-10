#include "token_bucket.hpp"

#include <algorithm>
#include <stdexcept>

namespace sentinel {

namespace {
// Process-start reference point. Taken once so the doubles we hand to Python stay
// small enough that adding fractions of a second does not lose precision the way
// a raw epoch-seconds double eventually would.
const std::chrono::steady_clock::time_point kOrigin = std::chrono::steady_clock::now();
}  // namespace

double monotonic_now() {
    const auto delta = std::chrono::steady_clock::now() - kOrigin;
    return std::chrono::duration<double>(delta).count();
}

// ---------------------------------------------------------------------------
// TokenBucket
// ---------------------------------------------------------------------------

TokenBucket::TokenBucket(double capacity, double refill_per_sec, double now)
    : capacity_(capacity),
      refill_per_sec_(refill_per_sec),
      tokens_(capacity),  // start full: a fresh client should not be throttled
      last_refill_(now) {
    if (capacity <= 0.0) throw std::invalid_argument("capacity must be > 0");
    if (refill_per_sec <= 0.0) throw std::invalid_argument("refill_per_sec must be > 0");
}

void TokenBucket::refill(double now) {
    // Clamp negative deltas to zero. steady_clock should never run backwards, but
    // if it somehow did we would credit a negative number of tokens and lock the
    // client out permanently, which is a far worse failure than losing a refill.
    const double elapsed = std::max(0.0, now - last_refill_);
    if (elapsed > 0.0) {
        tokens_ = std::min(capacity_, tokens_ + elapsed * refill_per_sec_);
        last_refill_ = now;
    }
}

double TokenBucket::peek_tokens(double now) const {
    const double elapsed = std::max(0.0, now - last_refill_);
    return std::min(capacity_, tokens_ + elapsed * refill_per_sec_);
}

Decision TokenBucket::try_consume(double cost, double now) {
    refill(now);
    Decision d{};
    if (tokens_ >= cost) {
        tokens_ -= cost;
        d.allowed = true;
        d.tokens_remaining = tokens_;
        d.retry_after = 0.0;
    } else {
        // Denied requests do NOT consume tokens. Charging for a rejection would
        // let a client that is already over the limit hold itself over it, which
        // turns a burst into an outage for that client.
        d.allowed = false;
        d.tokens_remaining = tokens_;
        d.retry_after = (cost - tokens_) / refill_per_sec_;
    }
    d.observed = 0;
    return d;
}

// ---------------------------------------------------------------------------
// SlidingWindowCounter
// ---------------------------------------------------------------------------

SlidingWindowCounter::SlidingWindowCounter(std::uint64_t limit, double window_sec, double now)
    : limit_(limit),
      window_sec_(window_sec),
      current_count_(0),
      previous_count_(0),
      window_start_(now) {
    if (limit == 0) throw std::invalid_argument("limit must be > 0");
    if (window_sec <= 0.0) throw std::invalid_argument("window_sec must be > 0");
}

void SlidingWindowCounter::roll(double now) {
    const double elapsed = now - window_start_;
    if (elapsed < window_sec_) return;

    if (elapsed < 2.0 * window_sec_) {
        // Exactly one window has passed: today's count becomes yesterday's.
        previous_count_ = current_count_;
        current_count_ = 0;
        window_start_ += window_sec_;
    } else {
        // Idle for two or more windows: nothing from the past overlaps any more.
        previous_count_ = 0;
        current_count_ = 0;
        window_start_ = now;
    }
}

double SlidingWindowCounter::estimate(double now) const {
    const double elapsed = now - window_start_;
    if (elapsed >= 2.0 * window_sec_) return 0.0;

    double prev = static_cast<double>(previous_count_);
    double cur = static_cast<double>(current_count_);
    double into = elapsed;
    if (elapsed >= window_sec_) {
        // Caller has not rolled yet; project what roll() would produce.
        prev = cur;
        cur = 0.0;
        into = elapsed - window_sec_;
    }
    // Fraction of the previous window still inside the trailing period.
    const double overlap = 1.0 - (into / window_sec_);
    return cur + prev * std::max(0.0, overlap);
}

Decision SlidingWindowCounter::try_consume(double now) {
    roll(now);
    const double est = estimate(now);

    Decision d{};
    if (est + 1.0 <= static_cast<double>(limit_)) {
        current_count_ += 1;
        d.allowed = true;
        d.retry_after = 0.0;
    } else {
        d.allowed = false;
        // Time until enough of the previous window rolls out of view. Exact only
        // when the previous window carries the overage, which is the common case.
        const double excess = est + 1.0 - static_cast<double>(limit_);
        const double prev = static_cast<double>(previous_count_);
        d.retry_after = prev > 0.0 ? (excess / prev) * window_sec_
                                   : window_sec_ - (now - window_start_);
        d.retry_after = std::max(0.0, d.retry_after);
    }
    d.tokens_remaining = std::max(0.0, static_cast<double>(limit_) - est);
    d.observed = static_cast<std::uint64_t>(est + 0.5);
    return d;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

Registry::Registry(Algorithm algo, double capacity, double refill_per_sec, std::size_t stripes)
    : algo_(algo), capacity_(capacity), refill_per_sec_(refill_per_sec) {
    if (stripes == 0) throw std::invalid_argument("stripes must be > 0");
    stripes_ = std::vector<Stripe>(stripes);
}

Registry::Stripe& Registry::stripe_for(const std::string& key) {
    return stripes_[hasher_(key) % stripes_.size()];
}

const Registry::Stripe& Registry::stripe_for(const std::string& key) const {
    return stripes_[hasher_(key) % stripes_.size()];
}

Decision Registry::check(const std::string& key, double cost) {
    const double now = monotonic_now();
    Stripe& s = stripe_for(key);

    Decision d{};
    {
        std::lock_guard<std::mutex> lock(s.mu);
        if (algo_ == Algorithm::TokenBucket) {
            auto it = s.buckets.find(key);
            if (it == s.buckets.end()) {
                it = s.buckets.emplace(key, TokenBucket(capacity_, refill_per_sec_, now)).first;
            }
            d = it->second.try_consume(cost, now);
        } else {
            auto it = s.windows.find(key);
            if (it == s.windows.end()) {
                // capacity_ is requests-per-window here; refill_per_sec_ is unused.
                it = s.windows
                         .emplace(key, SlidingWindowCounter(
                                           static_cast<std::uint64_t>(capacity_), 1.0, now))
                         .first;
            }
            d = it->second.try_consume(now);
        }
    }

    // Counters live outside the stripe lock so the two never contend. Relaxed
    // ordering is deliberate: these are dashboard statistics, not control flow,
    // and paying for sequential consistency on every request to make a graph
    // marginally fresher is the wrong trade.
    if (d.allowed) allowed_.fetch_add(1, std::memory_order_relaxed);
    else denied_.fetch_add(1, std::memory_order_relaxed);
    return d;
}

std::vector<Decision> Registry::check_many(const std::vector<std::string>& keys, double cost) {
    std::vector<Decision> out;
    out.reserve(keys.size());
    for (const auto& k : keys) out.push_back(check(k, cost));
    return out;
}

std::size_t Registry::evict_idle(double max_idle_sec) {
    const double now = monotonic_now();
    const double cutoff = now - max_idle_sec;
    std::size_t removed = 0;

    // Each stripe is locked in turn rather than all at once: eviction is
    // maintenance and must never block the whole hot path simultaneously.
    for (auto& s : stripes_) {
        std::lock_guard<std::mutex> lock(s.mu);
        for (auto it = s.buckets.begin(); it != s.buckets.end();) {
            if (it->second.last_seen() < cutoff) { it = s.buckets.erase(it); ++removed; }
            else { ++it; }
        }
        for (auto it = s.windows.begin(); it != s.windows.end();) {
            if (it->second.last_seen() < cutoff) { it = s.windows.erase(it); ++removed; }
            else { ++it; }
        }
    }
    return removed;
}

std::size_t Registry::size() const {
    std::size_t n = 0;
    for (const auto& s : stripes_) {
        std::lock_guard<std::mutex> lock(s.mu);
        n += s.buckets.size() + s.windows.size();
    }
    return n;
}

void Registry::reset_stats() {
    allowed_.store(0, std::memory_order_relaxed);
    denied_.store(0, std::memory_order_relaxed);
}

}  // namespace sentinel
