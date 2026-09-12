package com.rupeek.poc.temporal.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * ONE document per stage: everything the workflow needs to schedule the
 * activities of that stage, and nothing from any other stage.
 *
 * This is the "config per stage" the design asks for, and it maps 1:1 onto a
 * resolution point — loadStageKnobs is a single document read at the stage
 * boundary, not a lookup into a lender-wide blob.
 *
 * Retry policy and activity timeouts are ActivityOptions: the WORKFLOW sets them
 * when it schedules, so unlike a CallBinding they cannot be resolved inside the
 * activity. Re-reading them per stage is the compromise — a knob change reaches
 * every step not yet scheduled. A step already retrying keeps the policy it was
 * scheduled with; the escape hatch for that is PARK + retryStep.
 *
 * Values are MATERIALISED, not inherited: the seeder folds the lender default
 * into each stage document. Editing FUND_TRANSFER's retry is one document and
 * means exactly what it says, with no hunt for an inherited default. The cost is
 * that changing the default for every stage is N edits — the right trade for
 * something humans read and maintain.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StageKnobs {

    public String lenderSlug;
    /** Which transport version this slice came from — surfaces in FlowStatus. */
    public String transportVersion;
    public String stage;

    public Retry retry = new Retry();
    public int timeoutSeconds = 10;

    /**
     * Per-call exceptions, as a LIST rather than a map keyed by callKey.
     * callKeys contain dots ("PRECHECK.parallelChecks.mobileAadhaarLink"); as map
     * keys those become BSON field names, which is the thing that forced the
     * un-normalised store to keep everything as opaque JSON strings.
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<CallOverride> callOverrides = new ArrayList<>();

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CallOverride {
        public String callKey;
        public Retry retry;
        public Integer timeoutSeconds;
    }

    private CallOverride override(String callKey) {
        for (CallOverride o : callOverrides) {
            if (o.callKey != null && o.callKey.equals(callKey)) {
                return o;
            }
        }
        return null;
    }

    public Retry retryFor(String callKey) {
        CallOverride o = override(callKey);
        return o != null && o.retry != null ? o.retry : retry;
    }

    public int timeoutFor(String callKey) {
        CallOverride o = override(callKey);
        return o != null && o.timeoutSeconds != null ? o.timeoutSeconds : timeoutSeconds;
    }
}
