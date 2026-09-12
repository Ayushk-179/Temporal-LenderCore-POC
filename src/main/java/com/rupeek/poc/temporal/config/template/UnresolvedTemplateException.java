package com.rupeek.poc.temporal.config.template;

/**
 * Thrown in strict mode when a "${ctx.x}" has no value and no default.
 *
 * The old renderer did ctx.getOrDefault(key, "") — an absent key silently became
 * an empty string in the request body. On a disbursal call that is not a
 * rendering detail, it is a defect that reaches the lender. Failing loudly, and
 * non-retryably, is the whole point of this class.
 */
public class UnresolvedTemplateException extends RuntimeException {
    public UnresolvedTemplateException(String message) {
        super(message);
    }
}
