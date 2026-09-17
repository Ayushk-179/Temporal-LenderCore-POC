package com.rupeek.poc.temporal.workflow;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@WorkflowInterface
public interface LenderFlowWorkflow {

    @WorkflowMethod
    FlowResult run(FlowInput input);

    /** Replaces the e-sign correlation store: PUT /v1/disbursement signals this. */
    @SignalMethod
    void pledgeCardReady(Map<String, String> payload);

    /** Replaces GET /workflow/publish-event?eventName=retryLoanStatus */
    @SignalMethod
    void retryLoanStatus();

    /** Replaces GET /workflow/publish-event?eventName=retryFundTransferStatus */
    @SignalMethod
    void retryFundTransferStatus();

    /**
     * Release a step parked by onFailure=PARK.
     *
     * The recovery gesture this whole design exists for: a step has exhausted its
     * retries, an operator has fixed the binding document in the store, and this
     * signal re-runs the step. The re-run resolves config afresh, so it picks up
     * the fix. No worker redeploy, no Temporal Reset, no manual compensation.
     */
    @SignalMethod
    void retryStep();

    /**
     * Operator escape hatch: write values straight into the workflow context of a
     * RUNNING loan.
     *
     * For the case config cannot reach — a value nobody captured, that the lender
     * now requires. Park the step, patch the missing key in, signal retryStep, and
     * the re-run renders it.
     *
     * This is the most dangerous thing in the API: it edits live loan state with
     * no schema and no validation, and the change is permanent in history. It is
     * recorded in the step history and logged loudly for exactly that reason, and
     * a small set of identity keys is refused outright.
     */
    @SignalMethod
    void patchContext(Map<String, String> patch);

    /**
     * Live progress without Operate and without a DB read. This is what an ops
     * screen or a support engineer calls to answer "where is this loan stuck, and
     * which config version is it running".
     */
    @QueryMethod
    FlowStatus status();

    // ---------------------------------------------------------------- DTOs

    class FlowInput {
        public String lenderSlug;
        public String loanRequestId;
        /** Seed context: pan, mobile, customerName, dob, aadhaarRef, ... */
        public Map<String, String> seed = new LinkedHashMap<>();

        public FlowInput() {
        }

        public FlowInput(String lenderSlug, String loanRequestId, Map<String, String> seed) {
            this.lenderSlug = lenderSlug;
            this.loanRequestId = loanRequestId;
            this.seed = new LinkedHashMap<>(seed);
        }
    }

    class FlowResult {
        public String outcome;
        public String failedAt;
        public String reason;
        public Map<String, String> finalContext = new LinkedHashMap<>();

        public FlowResult() {
        }
    }

    class StepRecord {
        public String stage;
        public String step;
        public String status;
        public int attempts;
        public String detail;
        /** Which binding version this step's call actually went out with. */
        public String bindingVersion;

        public StepRecord() {
        }

        public StepRecord(String stage, String step, String status, int attempts, String detail) {
            this(stage, step, status, attempts, detail, null);
        }

        public StepRecord(String stage, String step, String status, int attempts, String detail,
                          String bindingVersion) {
            this.stage = stage;
            this.step = step;
            this.status = status;
            this.attempts = attempts;
            this.detail = detail;
            this.bindingVersion = bindingVersion;
        }
    }

    class FlowStatus {
        public String lenderSlug;
        public String loanRequestId;
        public String currentStage;
        public String currentStep;
        public String waitingOnSignal;
        /** "<STAGE>/<step>" when a step is parked awaiting retryStep, else null. */
        public String parkedAt;

        /** Config provenance, live. The pinned shape and the versions last used. */
        public String skeletonVersion;
        public String transportVersion;
        public String lastBindingVersion;

        public List<StepRecord> history;
        public Map<String, String> context;

        public FlowStatus() {
        }
    }
}
