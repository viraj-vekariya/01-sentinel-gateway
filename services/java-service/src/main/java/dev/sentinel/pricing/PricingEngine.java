package dev.sentinel.pricing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Service;

/**
 * The pricing rules.
 *
 * <p>Rule order is the whole design here, and it is deliberately fixed rather than
 * configurable: discounts that compose in a different order produce different totals,
 * and "why was I charged this?" must have exactly one answer. Every quote therefore
 * carries the ordered list of rules that fired, so a disputed price can be replayed
 * rather than argued about.
 *
 * <p>All arithmetic is in integer cents. Floating-point money is the classic way to
 * end up a fraction of a cent short on every transaction and unable to reconcile.
 */
@Service
public class PricingEngine {

    /** Volume breakpoints, checked highest-first so the best applicable tier wins. */
    private static final int[] VOLUME_THRESHOLDS = {100, 50, 20, 10};
    private static final int[] VOLUME_DISCOUNT_BPS = {1500, 1000, 600, 300};

    private static final long TAX_BPS = 1800;          // 18%
    private static final long BULK_FREIGHT_WAIVER = 25_000;

    private final AtomicLong quotesIssued = new AtomicLong();
    private final AtomicLong unitsQuoted = new AtomicLong();

    public Quote quote(long itemId, int quantity, String tier) {
        long started = System.nanoTime();
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive, got " + quantity);
        }

        List<String> rules = new ArrayList<>();

        // Base price is derived from the item id rather than looked up in a database.
        // This service is an upstream for a gateway demonstration; a real price store
        // would add a dependency without adding anything to what is being shown.
        long unitPrice = 4_900 + (itemId * 733) % 345_000;
        rules.add("base-price");

        long subtotal = unitPrice * quantity;
        long discount = 0;

        for (int i = 0; i < VOLUME_THRESHOLDS.length; i++) {
            if (quantity >= VOLUME_THRESHOLDS[i]) {
                discount += subtotal * VOLUME_DISCOUNT_BPS[i] / 10_000;
                rules.add("volume-" + VOLUME_THRESHOLDS[i] + "@" + VOLUME_DISCOUNT_BPS[i] + "bps");
                break;                              // one volume tier only, best match
            }
        }

        // Tier discount stacks on top of volume, applied to the ALREADY discounted
        // subtotal rather than the original. Applying both to the original would let
        // a large enough order price below cost.
        long tierBps = switch (tier == null ? "free" : tier.toLowerCase()) {
            case "internal" -> 2500;
            case "paid" -> 800;
            default -> 0;
        };
        if (tierBps > 0) {
            discount += (subtotal - discount) * tierBps / 10_000;
            rules.add("tier-" + tier + "@" + tierBps + "bps");
        }

        long discounted = subtotal - discount;
        if (discounted >= BULK_FREIGHT_WAIVER) {
            rules.add("freight-waived");
        }

        long tax = discounted * TAX_BPS / 10_000;
        rules.add("tax@" + TAX_BPS + "bps");

        quotesIssued.incrementAndGet();
        unitsQuoted.addAndGet(quantity);

        long micros = (System.nanoTime() - started) / 1_000;
        return new Quote(itemId, quantity, unitPrice, discount, tax, discounted + tax,
                List.copyOf(rules), micros);
    }

    public long quotesIssued() {
        return quotesIssued.get();
    }

    public long unitsQuoted() {
        return unitsQuoted.get();
    }
}
