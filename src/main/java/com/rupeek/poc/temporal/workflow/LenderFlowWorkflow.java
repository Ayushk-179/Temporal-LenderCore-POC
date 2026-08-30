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
     * Live progress without Operate and without a DB read. This is what an ops
     * screen or a support engineer calls to answer "where is this loan stuck".
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

        public StepRecord() {
        }

        public StepRecord(String stage, String step, String status, int attempts, String detail) {
            this.stage = stage;
            this.step = step;
            this.status = status;
            this.attempts = attempts;
            this.detail = detail;
        }
    }

    class FlowStatus {
        public String lenderSlug;
        public String loanRequestId;
        public String currentStage;
        public String currentStep;
        public String waitingOnSignal;
        public List<StepRecord> history;
        public Map<String, String> context;

        public FlowStatus() {
        }
    }
}
