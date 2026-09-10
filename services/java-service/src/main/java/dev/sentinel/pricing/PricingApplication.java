package dev.sentinel.pricing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Pricing service - the Java half of Sentinel's polyglot backend.
 *
 * <p>Sentinel forwards {@code /api/pricing/**} here over plain HTTP. Nothing in the
 * gateway knows this service is Java, which is the property being demonstrated: a
 * gateway whose routing depended on the backend's language would not be a gateway.
 */
@SpringBootApplication
public class PricingApplication {
    public static void main(String[] args) {
        SpringApplication.run(PricingApplication.class, args);
    }
}
