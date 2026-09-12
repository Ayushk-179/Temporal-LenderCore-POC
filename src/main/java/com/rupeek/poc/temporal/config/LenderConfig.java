package com.rupeek.poc.temporal.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The entire lender contract surface, as data.
 *
 * Everything in here is what we want to move OUT of Java and INTO config.
 * Anything that cannot be expressed by these classes is, by definition, a
 * Tier-2 handler and needs code — that boundary is the POC's actual finding.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LenderConfig {

    public Map<String, Lender> lenders = new LinkedHashMap<>();

    public Lender lender(String slug) {
        Lender l = lenders.get(slug);
        if (l == null) {
            throw new IllegalArgumentException("No config for lenderSlug=" + slug
                    + ". Known: " + lenders.keySet());
        }
        return l;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Lender {
        public String displayName;
        public Transport transport = new Transport();
        public Retry defaultRetry = new Retry();
        public List<Stage> stages = new ArrayList<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Transport {
        public String baseUrl;
        public int defaultTimeoutSeconds = 10;
        public Map<String, String> staticHeaders = new LinkedHashMap<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Retry {
        public int initialIntervalSeconds = 2;
        public double backoffCoefficient = 2.0;
        public int maximumAttempts = 3;
        public List<String> nonRetryableErrorTypes = new ArrayList<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Stage {
        public String name;
        public List<Step> steps = new ArrayList<>();
    }

    /** SYNC | PARALLEL | POLL | AWAIT_SIGNAL */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Step {
        public String name;
        public String mode;
        public Predicate condition;

        // SYNC / POLL
        public Call call;

        // PARALLEL
        public List<Call> calls = new ArrayList<>();
        /**
         * What to do when the fan-out gate is not met (a mandatory branch failed,
         * or minSuccess was not reached).
         *   FAIL     — stop the flow (default; a lender rejection always stops it)
         *   CONTINUE — go forward degraded; downstream steps branch on
         *              ${step}.decision / ${step}.${call}.outcome via `condition`
         */
        public String onBranchFailure = "FAIL";
        /** Quorum: minimum successful branches required to go forward. 0 = no quorum rule. */
        public int minSuccess = 0;

        // POLL
        public int pollIntervalSeconds = 30;
        public int maxAttempts = 10;
        /** FAIL | AWAIT_SIGNAL */
        public String onExhausted = "FAIL";
        public String exhaustedSignalName;

        // AWAIT_SIGNAL
        public String signalName;
        public int timeoutHours = 72;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Call {
        /** Only set for members of a PARALLEL group; otherwise the Step name is used. */
        public String name;
        public Predicate condition;

        /**
         * PARALLEL only. false = a technical failure on this branch (after its
         * retries are exhausted) does not by itself stop the flow. A
         * businessRejectWhen match still stops it — a lender "no" is an answer,
         * not a fault.
         */
        public boolean mandatory = true;

        /** Per-call retry override. Falls back to lender.defaultRetry when absent. */
        public Retry retry;

        public String method = "POST";
        public String path;
        public Map<String, String> queryParams = new LinkedHashMap<>();
        public Map<String, String> request = new LinkedHashMap<>();
        public Map<String, String> headers = new LinkedHashMap<>();

        /** contextKey -> dot path into the response body, e.g. "$.data.customer_id" */
        public Map<String, String> extract = new LinkedHashMap<>();

        public Predicate successWhen;
        public Predicate terminalWhen;
        public Predicate businessRejectWhen;

        public Integer timeoutSeconds;
    }

    /**
     * One predicate shape, evaluated against either a response body ({@code path})
     * or the workflow context ({@code ctx}).
     *
     * ops: EQUALS | NOT_EQUALS | IN | NOT_IN | BLANK | NOT_BLANK
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Predicate {
        public String path;
        public String ctx;
        public String op = "EQUALS";
        public String value;
        public List<String> values = new ArrayList<>();
    }
}
