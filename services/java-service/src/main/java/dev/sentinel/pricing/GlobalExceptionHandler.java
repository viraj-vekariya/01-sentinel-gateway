package dev.sentinel.pricing;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps exceptions to the status codes the gateway's circuit breaker expects.
 *
 * <p>This matters more than it looks. The breaker counts 5xx as upstream failure and
 * 4xx as the caller's problem. If a bad {@code quantity} leaked out as a 500, enough
 * malformed requests would trip the breaker and take a healthy service out of
 * rotation - a client-side bug causing a server-side outage.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", "bad_request", "detail", ex.getMessage()));
    }

    @ExceptionHandler(PricingController.ResponseStatusFault.class)
    public ResponseEntity<Map<String, Object>> injected(
            PricingController.ResponseStatusFault ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "service_unavailable", "detail", ex.getMessage()));
    }
}
