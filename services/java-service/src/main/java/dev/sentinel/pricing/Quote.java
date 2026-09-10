package dev.sentinel.pricing;

import java.util.List;

/**
 * A price quote. A record rather than a class: it is an immutable data carrier, and
 * records give equals/hashCode/toString without the boilerplate that would otherwise
 * be the bulk of this file.
 *
 * @param itemId          the catalogue item priced
 * @param quantity        units requested
 * @param unitPriceCents  list price per unit before discounts
 * @param discountCents   total discount applied across all units
 * @param taxCents        tax on the discounted subtotal
 * @param totalCents      final payable amount
 * @param appliedRules    which pricing rules fired, in order - the audit trail
 * @param computeMicros   how long the calculation took, for the gateway's histograms
 */
public record Quote(
        long itemId,
        int quantity,
        long unitPriceCents,
        long discountCents,
        long taxCents,
        long totalCents,
        List<String> appliedRules,
        long computeMicros) {
}
