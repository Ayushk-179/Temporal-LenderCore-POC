package com.rupeek.poc.temporal.activity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything non-deterministic lives here: HTTP, config reads, JSON parsing.
 * The workflow never touches a socket or the config store. That separation is
 * the whole reason replay works.
 *
 * Three methods, three config resolution points:
 *
 *   loadFlowSkeleton  once per execution  -> PINNED  (shape cannot shift under a loan)
 *   loadStageKnobs    once per stage      -> per-stage (retry/timeout tuning reaches
 *                                            every step not yet scheduled)
 *   execute           every attempt       -> HOT     (binding resolved INSIDE the
 *                                            activity, invisible to replay)
 */
@ActivityInterface
public interface StageActivity {

    /**
     * The pinned plane. The result is recorded in event history, so the flow
     * shape a loan follows is fixed at start and auditable forever.
     */
    @ActivityMethod
    FlowSkeleton loadFlowSkeleton(String lenderSlug);

    /**
     * The per-stage operational slice. Called at each stage boundary because
     * retry policy and activity timeouts are ActivityOptions — the workflow sets
     * them when scheduling, so they cannot be resolved activity-side.
     */
    @ActivityMethod
    StageKnobs loadStageKnobs(String lenderSlug, String stageName);

    /**
     * Executes one configured call. The CallRequest carries a callKey, NOT the
     * call config: the binding is resolved from the live store on every attempt.
     * That is what lets a payload fix reach an activity already in retry backoff.
     */
    @ActivityMethod
    CallResult execute(CallRequest request);

    // ---------------------------------------------------------------- DTOs

    /**
     * Tolerate unknown fields: these DTOs are recorded in event history, and a
     * replay months from now deserializes OLD json into the CURRENT class. Without
     * this, the first field removed here stalls every in-flight loan on
     * UnrecognizedPropertyException — the SDK's Jackson converter leaves
     * FAIL_ON_UNKNOWN_PROPERTIES at Jackson's default (enabled).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    class CallRequest {
        public String lenderSlug;
        public String stageName;
        public String stepName;
        public String callName;
        /** "<STAGE>.<step>" or "<STAGE>.<step>.<branch>" — the binding lookup key. */
        public String callKey;
        /** Immutable snapshot of workflow context used to resolve ${ctx.*} templates. */
        public Map<String, String> context = new LinkedHashMap<>();

        public CallRequest() {
        }

        public CallRequest(String lenderSlug, String stageName, String stepName, String callName,
                           String callKey, Map<String, String> context) {
            this.lenderSlug = lenderSlug;
            this.stageName = stageName;
            this.stepName = stepName;
            this.callName = callName;
            this.callKey = callKey;
            this.context = new LinkedHashMap<>(context);
        }
    }

    /** Same reason as CallRequest — and this one is deserialized on every replay. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    class CallResult {
        public String callName;
        public int httpStatus;
        /** Extracted context keys -> values, merged back into workflow state. */
        public Map<String, String> extracted = new LinkedHashMap<>();
        /** True when terminalWhen matched (POLL mode only). */
        public boolean terminal;
        public String rawResponseSnippet;

        /**
         * Provenance. Which config versions this ATTEMPT actually used — recorded
         * in event history, so "what contract did this call go out with, at 02:14"
         * is answerable months later without a config archaeology exercise.
         */
        public String bindingVersion;
        public String transportVersion;

        public CallResult() {
        }
    }
}
