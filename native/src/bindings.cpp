// pybind11 bindings for the Sentinel native rate limiter.
//
// The single most important line in this file is `py::call_guard<py::gil_scoped_release>()`
// on Registry::check. Without it the C++ code would run holding the GIL and this whole
// extension would be pointless: we would have moved the work to C++ but kept the exact
// serialisation that made pure Python slow. With it, N gateway worker threads enter the
// striped-lock registry concurrently and the limiter stops being the throughput ceiling.
//
// Everything exposed here is deliberately small. The extension owns the limiter and
// nothing else; policy, config and routing all stay in Python where they are easier to
// change. See DECISIONS.md D-01.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "token_bucket.hpp"

namespace py = pybind11;
using namespace sentinel;

PYBIND11_MODULE(sentinel_native, m) {
    m.doc() = "Sentinel Gateway native rate-limiting core (token bucket + sliding window)";

    m.def("monotonic_now", &monotonic_now,
          "Seconds since module load, from steady_clock. Never moves backwards.");

    py::class_<Decision>(m, "Decision")
        .def_readonly("allowed", &Decision::allowed)
        .def_readonly("tokens_remaining", &Decision::tokens_remaining)
        .def_readonly("retry_after", &Decision::retry_after)
        .def_readonly("observed", &Decision::observed)
        .def("__repr__", [](const Decision& d) {
            return "<Decision allowed=" + std::string(d.allowed ? "True" : "False") +
                   " remaining=" + std::to_string(d.tokens_remaining) +
                   " retry_after=" + std::to_string(d.retry_after) + ">";
        });

    py::class_<TokenBucket>(m, "TokenBucket")
        .def(py::init([](double capacity, double refill_per_sec) {
                 return TokenBucket(capacity, refill_per_sec, monotonic_now());
             }),
             py::arg("capacity"), py::arg("refill_per_sec"))
        .def("try_consume",
             [](TokenBucket& b, double cost) { return b.try_consume(cost, monotonic_now()); },
             py::arg("cost") = 1.0)
        .def("peek_tokens", [](const TokenBucket& b) { return b.peek_tokens(monotonic_now()); })
        .def_property_readonly("capacity", &TokenBucket::capacity)
        .def_property_readonly("refill_per_sec", &TokenBucket::refill_per_sec);

    py::class_<SlidingWindowCounter>(m, "SlidingWindowCounter")
        .def(py::init([](std::uint64_t limit, double window_sec) {
                 return SlidingWindowCounter(limit, window_sec, monotonic_now());
             }),
             py::arg("limit"), py::arg("window_sec") = 1.0)
        .def("try_consume",
             [](SlidingWindowCounter& w) { return w.try_consume(monotonic_now()); })
        .def("estimate", [](const SlidingWindowCounter& w) { return w.estimate(monotonic_now()); })
        .def_property_readonly("limit", &SlidingWindowCounter::limit)
        .def_property_readonly("window_sec", &SlidingWindowCounter::window_sec);

    py::enum_<Registry::Algorithm>(m, "Algorithm")
        .value("TOKEN_BUCKET", Registry::Algorithm::TokenBucket)
        .value("SLIDING_WINDOW", Registry::Algorithm::SlidingWindow);

    py::class_<Registry>(m, "Registry")
        .def(py::init<Registry::Algorithm, double, double, std::size_t>(),
             py::arg("algorithm"), py::arg("capacity"), py::arg("refill_per_sec"),
             py::arg("stripes") = 16)

        // THE hot path. call_guard releases the GIL for the duration of the C++ call
        // and reacquires it on return, so concurrent callers actually run in parallel.
        .def("check", &Registry::check, py::arg("key"), py::arg("cost") = 1.0,
             py::call_guard<py::gil_scoped_release>(),
             "Rate-limit one request. Releases the GIL.")

        // Deliberate experimental control: the SAME C++ code path, but WITHOUT
        // releasing the GIL. Having both lets the benchmark decompose the effect of
        // "compiled instead of interpreted" from the effect of "released the GIL",
        // which turn out to point in opposite directions under contention.
        // See DECISIONS.md D-03 and outputs/bench_ratelimit.json.
        .def("check_holding_gil", &Registry::check, py::arg("key"), py::arg("cost") = 1.0,
             "Rate-limit one request WITHOUT releasing the GIL (benchmark control arm).")

        // One GIL release amortised over a whole batch. Used by the benchmark to
        // separate per-call binding overhead from the limiter's own cost.
        .def("check_many", &Registry::check_many, py::arg("keys"), py::arg("cost") = 1.0,
             py::call_guard<py::gil_scoped_release>(),
             "Rate-limit a batch of requests under a single GIL release.")

        .def("evict_idle", &Registry::evict_idle, py::arg("max_idle_sec"),
             py::call_guard<py::gil_scoped_release>(),
             "Drop buckets untouched for longer than max_idle_sec. Returns count removed.")

        .def("size", &Registry::size, py::call_guard<py::gil_scoped_release>())
        .def("reset_stats", &Registry::reset_stats)
        .def_property_readonly("total_allowed", &Registry::total_allowed)
        .def_property_readonly("total_denied", &Registry::total_denied);

#ifdef SENTINEL_VERSION
    m.attr("__version__") = SENTINEL_VERSION;
#else
    m.attr("__version__") = "0.1.0";
#endif
}
