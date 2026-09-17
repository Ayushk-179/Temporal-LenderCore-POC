package com.rupeek.poc.temporal.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * THE PINNED PLANE — everything the workflow needs to decide control flow, and
 * nothing else.
 *
 * This is loaded ONCE at workflow start through an activity, so it lands in
 * event history and is stable for the life of the execution. It has to be:
 * every field in here influences the NUMBER, ORDER or IDENTITY of the commands
 * the workflow emits, which is exactly what Temporal compares on replay.
 *
 * Notice what is NOT here: no URL, no method, no payload template, no extract
 * paths, no response predicates. Those live in {@link CallBinding}, are resolved
 * inside the activity on every attempt, and are therefore free to change under a
 * running workflow without any replay risk at all.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FlowSkeleton {

    public String lenderSlug;
    public String displayName;
    /** Version of the skeleton document this execution is pinned to, e.g. "v1". */
    public String version;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Stage> stages = new ArrayList<>();

    @JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Stage {
        public String name;
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<Step> steps = new ArrayList<>();
    }

    /** SYNC | PARALLEL | POLL | AWAIT_SIGNAL */
    @JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Step {
        public String name;
        public String mode;
        public Predicate condition;

        // ---- PARALLEL: branch identity + gate. The HTTP of each branch is a binding.
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<Branch> branches = new ArrayList<>();
        /** FAIL | CONTINUE — what to do when the fan-out gate is not met. */
        public String onBranchFailure = "FAIL";
        /** Quorum: minimum successful branches required to go forward. 0 = no rule. */
        public int minSuccess = 0;

        // ---- POLL
        public int pollIntervalSeconds = 30;
        public int maxAttempts = 10;
        /** FAIL | AWAIT_SIGNAL */
        public String onExhausted = "FAIL";
        public String exhaustedSignalName;

        // ---- AWAIT_SIGNAL
        public String signalName;
        public int timeoutHours = 72;

        /**
         * FAIL | PARK. PARK is the whole point of the config-driven recovery story:
         * instead of failing the execution when retries are exhausted, the workflow
         * parks on the `retryStep` signal. An operator fixes the binding in the
         * store and signals; the step re-runs and resolves the CORRECTED binding.
         * No redeploy, no Temporal Reset.
         *
         * Never applies to a BUSINESS_REJECT — a lender decline is an answer, not
         * a fault, and no config edit can change it.
         */
        public String onFailure = "FAIL";

        /**
         * How long a PARKed step waits for retryStep before giving up, in hours.
         * Defaults to 72 — the same three days as the e-sign wait, because both
         * are "a human has to do something" waits and there is no reason for an
         * operator to get less time than a customer.
         */
        public int parkTimeoutHours = 72;
    }

    /** One member of a PARALLEL fan-out: identity and gate role only. */
    @JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Branch {
        public String name;
        public Predicate condition;
        /**
         * false = a technical failure on this branch (after its retries) does not
         * by itself stop the flow. A businessRejectWhen match still stops it.
         */
        public boolean mandatory = true;
    }
}
