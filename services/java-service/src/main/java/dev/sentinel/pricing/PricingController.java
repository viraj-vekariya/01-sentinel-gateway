package dev.sentinel.pricing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP surface for the pricing service.
 *
 * <p>Carries the same fault-injection and latency knobs as the Python upstream, for
 * the same reason: the gateway's circuit breaker and retry paths cannot be
 * demonstrated against a backend that never fails. The two services expose the knobs
 * identically so a demo can fail either one and the gateway behaves the same.
 */
@RestController
@RequestMapping("/api/pricing")
public class PricingController {

    private final PricingEngine engine;

    @Value("${sentinel.fail-rate:0.0}")
    private double failRate;

    @Value("${sentinel.latency-ms:0}")
    private long latencyMs;

    public PricingController(PricingEngine engine) {
        this.engine = engine;
    }

    /** Applies the configured latency and failure injection. */
    private void maybeFault() {
        if (latencyMs > 0) {
            try {
                Thread.sleep(latencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();   // never swallow an interrupt
            }
        }
        if (failRate > 0 && ThreadLocalRandom.current().nextDouble() < failRate) {
            throw new ResponseStatusFault("injected fault");
        }
    }

    @GetMapping("/quote")
    public ResponseEntity<Quote> quote(
            @RequestParam long itemId,
            @RequestParam(defaultValue = "1") int quantity,
            @RequestHeader(value = "x-tier", required = false) String tier) {
        maybeFault();
        return ResponseEntity.ok(engine.quote(itemId, quantity, tier));
    }

    /**
     * Batch quoting. Exists because it is the natural place for the gateway to show a
     * route whose upstream cost scales with the request body, rather than being
     * constant per call.
     */
    @PostMapping("/quote/batch")
    public ResponseEntity<Map<String, Object>> batch(
            @RequestBody List<Map<String, Object>> lines,
            @RequestHeader(value = "x-tier", required = false) String tier) {
        maybeFault();
        if (lines.size() > 200) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                    .body(Map.of("error", "at most 200 lines per batch"));
        }
        List<Quote> quotes = new ArrayList<>(lines.size());
        long total = 0;
        for (Map<String, Object> line : lines) {
            long itemId = ((Number) line.getOrDefault("itemId", 1)).longValue();
            int qty = ((Number) line.getOrDefault("quantity", 1)).intValue();
            Quote q = engine.quote(itemId, qty, tier);
            quotes.add(q);
            total += q.totalCents();
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("lines", quotes.size());
        body.put("totalCents", total);
        body.put("quotes", quotes);
        return ResponseEntity.ok(body);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("service", "pricing-java");
        body.put("javaVersion", System.getProperty("java.version"));
        body.put("quotesIssued", engine.quotesIssued());
        body.put("unitsQuoted", engine.unitsQuoted());
        body.put("failRate", failRate);
        body.put("latencyMs", latencyMs);
        return body;
    }

    /** Thrown to produce a 503 the gateway's breaker will count as a failure. */
    static class ResponseStatusFault extends RuntimeException {
        ResponseStatusFault(String message) {
            super(message);
        }
    }
}
