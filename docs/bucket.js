/* Token bucket, reimplemented in JavaScript so the demo is genuinely interactive.
 *
 * Mirrors native/src/token_bucket.cpp: lazy refill, denied requests do not consume tokens,
 * and retry_after is the exact time until the deficit is repaid.
 * tools/check_js_matches_native.py asserts it agrees with the compiled C++ extension.
 */
function makeBucket(capacity, refillPerSec){
  return {capacity, refillPerSec, tokens: capacity, last: 0};   // fresh clients start full
}
function tryConsume(b, cost, now){
  // Lazy refill: credit (now - last) * rate, capped at capacity. No background timer, and
  // an idle client costs nothing.
  const elapsed = Math.max(0, now - b.last);
  if(elapsed > 0){ b.tokens = Math.min(b.capacity, b.tokens + elapsed * b.refillPerSec); b.last = now; }
  if(b.tokens >= cost){
    b.tokens -= cost;
    return {allowed: true, remaining: b.tokens, retryAfter: 0};
  }
  // A denied request does NOT consume tokens. Charging for a rejection would let a client
  // already over its limit hold itself over it - turning a burst into an outage.
  return {allowed: false, remaining: b.tokens, retryAfter: (cost - b.tokens) / b.refillPerSec};
}
if(typeof module !== "undefined") module.exports = {makeBucket, tryConsume};
