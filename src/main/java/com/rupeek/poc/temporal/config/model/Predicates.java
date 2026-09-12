package com.rupeek.poc.temporal.config.model;

import java.util.List;

/**
 * Shared by the workflow (ctx conditions) and the activity (response
 * predicates) so condition semantics cannot drift between the two planes.
 */
public final class Predicates {

    public static boolean evaluate(Predicate p, String actual) {
        String op = p.op == null ? "EQUALS" : p.op.toUpperCase();
        List<String> vals = p.values;
        switch (op) {
            case "EQUALS":
                return actual != null && actual.equals(p.value);
            case "NOT_EQUALS":
                return actual == null || !actual.equals(p.value);
            case "IN":
                return actual != null && vals.contains(actual);
            case "NOT_IN":
                return actual == null || !vals.contains(actual);
            case "BLANK":
                return actual == null || actual.isBlank();
            case "NOT_BLANK":
                return actual != null && !actual.isBlank();
            default:
                throw new IllegalArgumentException("Unknown predicate op: " + op);
        }
    }

    private Predicates() {
    }
}
