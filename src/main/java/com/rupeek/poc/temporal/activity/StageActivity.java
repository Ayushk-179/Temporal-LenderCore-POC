package com.rupeek.poc.temporal.activity;

import com.rupeek.poc.temporal.config.LenderConfig;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything non-deterministic lives here: HTTP, config loading, JSON parsing.
 *
 * The workflow never touches a socket. That separation is the whole reason
 * replay works.
 */
@ActivityInterface
public interface StageActivity {

    /**
     * Loaded exactly once per workflow execution and recorded in event history,
     * so a replay months later uses the SAME config the flow started with.
     * That is our contextSnapshot, for free.
     */
    @ActivityMethod
    LenderConfig loadConfig();

    /** Executes one configured HTTP call and returns the extracted context delta. */
    @ActivityMethod
    CallResult execute(CallRequest request);

    // ---------------------------------------------------------------- DTOs

    class CallRequest {
        public String lenderSlug;
        public String stageName;
        public String stepName;
        public String callName;
        public LenderConfig.Call call;
        public LenderConfig.Transport transport;
        /** Immutable snapshot of workflow context used to resolve ${ctx.*} templates. */
        public Map<String, String> context = new LinkedHashMap<>();

        public CallRequest() {
        }

        public CallRequest(String lenderSlug, String stageName, String stepName, String callName,
                           LenderConfig.Call call, LenderConfig.Transport transport,
                           Map<String, String> context) {
            this.lenderSlug = lenderSlug;
            this.stageName = stageName;
            this.stepName = stepName;
            this.callName = callName;
            this.call = call;
            this.transport = transport;
            this.context = new LinkedHashMap<>(context);
        }
    }

    class CallResult {
        public String callName;
        public int httpStatus;
        /** Extracted context keys -> values, merged back into workflow state. */
        public Map<String, String> extracted = new LinkedHashMap<>();
        /** True when terminalWhen matched (POLL mode only). */
        public boolean terminal;
        public String rawResponseSnippet;

        public CallResult() {
        }
    }
}
