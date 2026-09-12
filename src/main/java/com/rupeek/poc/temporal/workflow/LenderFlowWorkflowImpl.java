package com.rupeek.poc.temporal.workflow;

import com.rupeek.poc.temporal.activity.StageActivity;
import com.rupeek.poc.temporal.activity.StageActivityImpl;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.Predicate;
import com.rupeek.poc.temporal.config.model.Predicates;
import com.rupeek.poc.temporal.config.model.Retry;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ONE workflow class for every lender. Adding a lender means adding config
 * documents to the store — not a BPMN file, a dispatcher, or a branch in here.
 *
 * DETERMINISM RULES OBSERVED HERE — the part to review carefully:
 *   - no System.currentTimeMillis()  -> Workflow.currentTimeMillis()
 *   - no UUID.randomUUID()          -> Workflow.randomUUID()
 *   - no Thread.sleep()             -> Workflow.sleep()
 *   - no HTTP, no config store read -> activities only
 *
 * CONFIG AND REPLAY — the design claim, stated precisely:
 *
 *   Replay safety does not come from PINNING config. It comes from config being
 *   read inside an ACTIVITY. Activity results are recorded in history and never
 *   re-executed on replay, so whatever the store says later is irrelevant to a
 *   replay of what already happened.
 *
 *   What pinning buys is something else: SEMANTIC COHERENCE. One loan follows one
 *   flow shape start to finish. So the split is by what a field can influence:
 *
 *     loadFlowSkeleton  once   the number, order and identity of commands
 *                              -> must be stable, so it is pinned in history
 *     loadStageKnobs    /stage ActivityOptions (retry, timeouts) chosen by the
 *                              workflow at schedule time -> re-read per stage
 *     the CallBinding   never  resolved inside the activity, so a payload, URL,
 *                              header or extract path can change under a running
 *                              loan with no replay consequence whatsoever
 */
public class LenderFlowWorkflowImpl implements LenderFlowWorkflow {

    private final Map<String, String> ctx = new LinkedHashMap<>();
    private final List<StepRecord> history = new ArrayList<>();

    private String lenderSlug;
    private String loanRequestId;
    private String currentStage = "NOT_STARTED";
    private String currentStep = "NOT_STARTED";
    private String waitingOnSignal;
    private String parkedAt;

    private String skeletonVersion;
    private String transportVersion;
    private String lastBindingVersion;

    private boolean pledgeCardSignal;
    private boolean retryLoanStatusSignal;
    private boolean retryFundTransferSignal;
    private boolean retryStepSignal;

    private StageActivity configActivities;

    @Override
    public FlowResult run(FlowInput input) {
        this.lenderSlug = input.lenderSlug;
        this.loanRequestId = input.loanRequestId;
        ctx.putAll(input.seed);
        ctx.put("loanRequestId", input.loanRequestId);
        ctx.put("lenderSlug", input.lenderSlug);

        // Short-timeout stub for the two config-reading activities. Config reads
        // must retry — the store is a dependency now, and an outage at a stage
        // boundary should stall, not fail, the loan.
        configActivities = Workflow.newActivityStub(
                StageActivity.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(20))
                        .setRetryOptions(RetryOptions.newBuilder()
                                .setInitialInterval(Duration.ofSeconds(2))
                                .setBackoffCoefficient(2.0)
                                .setMaximumAttempts(5)
                                .build())
                        .build());

        // ---- resolution point 1: ONCE. Pinned in history for the life of the loan.
        FlowSkeleton skeleton = configActivities.loadFlowSkeleton(lenderSlug);
        skeletonVersion = skeleton.version;

        FlowResult result = new FlowResult();

        try {
            for (FlowSkeleton.Stage stage : skeleton.stages) {
                currentStage = stage.name;

                // ---- resolution point 2: PER STAGE. A retry/timeout tune published
                // while this loan was in PRECHECK reaches its FUND_TRANSFER steps.
                StageKnobs knobs = configActivities.loadStageKnobs(lenderSlug, stage.name);
                transportVersion = knobs.transportVersion;

                for (FlowSkeleton.Step step : stage.steps) {
                    currentStep = step.name;

                    if (!conditionHolds(step.condition)) {
                        history.add(new StepRecord(stage.name, step.name, "SKIPPED", 0,
                                "condition not met"));
                        continue;
                    }
                    runStep(stage, step, knobs);
                }
            }
            result.outcome = "COMPLETED";
        } catch (ActivityFailure af) {
            result.outcome = isBusinessReject(af) ? "REJECTED" : "FAILED";
            result.failedAt = currentStage + "/" + currentStep;
            result.reason = rootMessage(af);
            history.add(new StepRecord(currentStage, currentStep, result.outcome, 0, result.reason));
        } catch (ApplicationFailure af) {
            // Thrown by the workflow itself — today only the PARALLEL gate, which
            // has already decided REJECTED vs FAILED. Every other type
            // (POLL_EXHAUSTED, SIGNAL_TIMEOUT, CONFIG_ERROR) still fails loudly.
            if (!StageActivityImpl.BUSINESS_REJECT.equals(af.getType())
                    && !StageActivityImpl.TECHNICAL_FAILURE.equals(af.getType())) {
                throw af;
            }
            result.outcome = StageActivityImpl.BUSINESS_REJECT.equals(af.getType())
                    ? "REJECTED" : "FAILED";
            result.failedAt = currentStage + "/" + currentStep;
            result.reason = af.getOriginalMessage();
            history.add(new StepRecord(currentStage, currentStep, result.outcome, 0, result.reason));
        }

        result.finalContext = new LinkedHashMap<>(ctx);
        return result;
    }

    /**
     * Runs one step, and — when config says so — parks instead of failing.
     *
     * PARK is the recovery path the config store exists to enable. The activity
     * has already exhausted its retry ladder (during which a binding fix would
     * have been picked up automatically, since bindings resolve per attempt). If
     * it is still broken, failing the execution would mean a Temporal Reset and a
     * manual remediation. Parking instead leaves the loan alive and waiting: an
     * operator fixes the document, signals retryStep, and the step re-runs against
     * freshly resolved config.
     *
     * A BUSINESS_REJECT never parks — a lender decline is an answer, and no config
     * edit can turn it into a yes.
     *
     * Determinism note: the park count is driven by recorded activity failures and
     * recorded signals, so the sequence of commands is fully reproducible on replay.
     */
    private void runStep(FlowSkeleton.Stage stage, FlowSkeleton.Step step, StageKnobs knobs) {
        StageKnobs effective = knobs;
        while (true) {
            try {
                switch (step.mode.toUpperCase()) {
                    case "SYNC":
                        runSync(stage, step, effective);
                        return;
                    case "PARALLEL":
                        runParallel(stage, step, effective);
                        return;
                    case "POLL":
                        runPoll(stage, step, effective);
                        return;
                    case "AWAIT_SIGNAL":
                        runAwaitSignal(stage, step);
                        return;
                    default:
                        throw ApplicationFailure.newNonRetryableFailure(
                                "Unknown step mode: " + step.mode, "CONFIG_ERROR");
                }
            } catch (ActivityFailure af) {
                if (!"PARK".equalsIgnoreCase(step.onFailure) || isBusinessReject(af)) {
                    throw af;
                }
                parkedAt = stage.name + "/" + step.name;
                history.add(new StepRecord(stage.name, step.name, "AWAITING_INTERVENTION", 0,
                        "retries exhausted: " + rootMessage(af)
                                + " — fix the binding in the config store, then signal retryStep"));

                boolean released = awaitRetryStep(Duration.ofDays(7));
                parkedAt = null;
                if (!released) {
                    throw af;
                }

                // A fix may be a knob change (more attempts, longer timeout), not
                // only a binding change, so re-read this stage's slice too.
                effective = configActivities.loadStageKnobs(lenderSlug, stage.name);
                transportVersion = effective.transportVersion;
                history.add(new StepRecord(stage.name, step.name, "RETRYING", 0,
                        "released by retryStep; config re-resolved (transport "
                                + effective.transportVersion + ")"));
            }
        }
    }

    // ------------------------------------------------------------- step modes

    private void runSync(FlowSkeleton.Stage stage, FlowSkeleton.Step step, StageKnobs knobs) {
        String callKey = callKey(stage, step, null);
        StageActivity.CallResult res = stub(knobs, callKey).execute(
                activityName(stage, step.name), StageActivity.CallResult.class,
                request(stage, step, step.name, callKey));
        ctx.putAll(res.extracted);
        lastBindingVersion = res.bindingVersion;
        history.add(new StepRecord(stage.name, step.name, "SUCCESS", 1,
                "http " + res.httpStatus, res.bindingVersion));
    }

    /**
     * The parallel fan-out. Branch identity and the gate come from the skeleton;
     * each branch's wire contract is its own binding, resolved independently.
     *
     * executeAsync blocks the workflow, not a thread. The join is SETTLE-ALL, not
     * fail-fast:
     *   1. every launched branch is awaited, including branches still running when
     *      a sibling has already failed. Promise.get() parks the workflow
     *      coroutine, the branches keep running concurrently, so the wall clock is
     *      still the slowest branch — not the sum;
     *   2. each branch has exhausted its OWN retry policy before its promise
     *      settles (per-call override in the knobs, else the stage default);
     *   3. every SUCCESSFUL branch's extract is merged into ctx in declared order,
     *      so a partial fan-out is not discarded;
     *   4. the go/no-go decision is taken once, after the join.
     */
    private void runParallel(FlowSkeleton.Stage stage, FlowSkeleton.Step step, StageKnobs knobs) {
        List<FlowSkeleton.Branch> launched = new ArrayList<>();
        List<Promise<StageActivity.CallResult>> promises = new ArrayList<>();

        for (FlowSkeleton.Branch branch : step.branches) {
            if (!conditionHolds(branch.condition)) {
                ctx.put(branchKey(step, branch.name), "SKIPPED");
                history.add(new StepRecord(stage.name, step.name + "/" + branch.name,
                        "SKIPPED", 0, "condition not met"));
                continue;
            }
            String callKey = callKey(stage, step, branch.name);
            promises.add(stub(knobs, callKey).executeAsync(
                    activityName(stage, branch.name), StageActivity.CallResult.class,
                    request(stage, step, branch.name, callKey)));
            launched.add(branch);
        }

        if (promises.isEmpty()) {
            ctx.put(decisionKey(step), "PROCEED");
            history.add(new StepRecord(stage.name, step.name, "SKIPPED", 0, "no branches enabled"));
            return;
        }

        // ---------------------------------------------------------- settle all
        List<String> succeeded = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        List<String> rejected = new ArrayList<>();
        List<String> blocking = new ArrayList<>();

        for (int i = 0; i < promises.size(); i++) {
            FlowSkeleton.Branch branch = launched.get(i);
            String status;
            String detail;
            String bindingVersion = null;
            try {
                StageActivity.CallResult r = promises.get(i).get();
                // Declared order, not completion order: the merge stays deterministic.
                ctx.putAll(r.extracted);
                lastBindingVersion = r.bindingVersion;
                bindingVersion = r.bindingVersion;
                status = "SUCCESS";
                detail = "http " + r.httpStatus;
                succeeded.add(branch.name);
            } catch (ActivityFailure af) {
                boolean reject = isBusinessReject(af);
                status = reject ? "REJECTED" : "FAILED";
                detail = rootMessage(af);
                (reject ? rejected : failed).add(branch.name);
                if (reject || branch.mandatory) {
                    blocking.add(branch.name + "(" + status + ")");
                }
            }
            ctx.put(branchKey(step, branch.name), status);
            history.add(new StepRecord(stage.name, step.name + "/" + branch.name, status, 1,
                    detail, bindingVersion));
        }

        // ------------------------------------------------------- decide, once
        ctx.put(step.name + ".successCount", String.valueOf(succeeded.size()));
        ctx.put(step.name + ".branchCount", String.valueOf(promises.size()));
        ctx.put(step.name + ".failedCalls", String.join(",", failed));
        ctx.put(step.name + ".rejectedCalls", String.join(",", rejected));

        boolean quorumMet = step.minSuccess <= 0 || succeeded.size() >= step.minSuccess;
        String summary = succeeded.size() + "/" + promises.size() + " ok"
                + (blocking.isEmpty() ? "" : "; blocking: " + String.join(", ", blocking))
                + (quorumMet ? "" : "; minSuccess=" + step.minSuccess + " not met");

        if (!rejected.isEmpty()) {
            ctx.put(decisionKey(step), "STOP");
            throw ApplicationFailure.newNonRetryableFailure(
                    step.name + " rejected by lender: " + String.join(", ", rejected)
                            + " [" + summary + "]",
                    StageActivityImpl.BUSINESS_REJECT);
        }

        if (blocking.isEmpty() && quorumMet) {
            ctx.put(decisionKey(step), failed.isEmpty() ? "PROCEED" : "PROCEED_DEGRADED");
            history.add(new StepRecord(stage.name, step.name,
                    failed.isEmpty() ? "SUCCESS" : "PARTIAL_SUCCESS", promises.size(),
                    "parallel: " + summary
                            + (failed.isEmpty() ? "" : "; tolerated: " + String.join(", ", failed))));
            return;
        }

        if ("CONTINUE".equalsIgnoreCase(step.onBranchFailure)) {
            ctx.put(decisionKey(step), "PROCEED_DEGRADED");
            history.add(new StepRecord(stage.name, step.name, "PARTIAL_SUCCESS", promises.size(),
                    "parallel: " + summary + "; onBranchFailure=CONTINUE"));
            return;
        }

        ctx.put(decisionKey(step), "STOP");
        throw ApplicationFailure.newNonRetryableFailure(
                step.name + " gate not met: " + summary, StageActivityImpl.TECHNICAL_FAILURE);
    }

    /**
     * Durable polling. No PendingCorrelation document, no poll_lock, no
     * ASYNC_RESUME re-enqueue. Workflow.sleep survives pod death and deploys —
     * and each poll iteration resolves the binding again, so a lender changing
     * its status endpoint mid-poll is a config edit, not an incident.
     */
    private void runPoll(FlowSkeleton.Stage stage, FlowSkeleton.Step step, StageKnobs knobs) {
        String callKey = callKey(stage, step, null);
        int attempt = 0;
        while (true) {
            attempt++;
            StageActivity.CallResult res = stub(knobs, callKey).execute(
                    activityName(stage, step.name), StageActivity.CallResult.class,
                    request(stage, step, step.name, callKey));
            ctx.putAll(res.extracted);
            lastBindingVersion = res.bindingVersion;

            if (res.terminal) {
                history.add(new StepRecord(stage.name, step.name, "SUCCESS", attempt,
                        "terminal after " + attempt + " poll(s)", res.bindingVersion));
                return;
            }

            if (attempt >= step.maxAttempts) {
                if (!"AWAIT_SIGNAL".equalsIgnoreCase(step.onExhausted)) {
                    throw ApplicationFailure.newNonRetryableFailure(
                            step.name + " exhausted " + step.maxAttempts + " polls",
                            "POLL_EXHAUSTED");
                }
                // Park for a human. Mirrors the BPMN receiveTask, minus the BPMN.
                history.add(new StepRecord(stage.name, step.name, "AWAITING_MANUAL_RETRY",
                        attempt, "signal: " + step.exhaustedSignalName));
                awaitNamedSignal(step.exhaustedSignalName, Duration.ofDays(7));
                attempt = 0;   // matches prod: counter resets on manual resume
                continue;
            }

            Workflow.sleep(Duration.ofSeconds(step.pollIntervalSeconds));
        }
    }

    /** E-sign wait. Days long, zero resource cost, survives everything. */
    private void runAwaitSignal(FlowSkeleton.Stage stage, FlowSkeleton.Step step) {
        history.add(new StepRecord(stage.name, step.name, "WAITING", 0,
                "signal: " + step.signalName));
        boolean got = awaitNamedSignal(step.signalName, Duration.ofHours(step.timeoutHours));
        if (!got) {
            throw ApplicationFailure.newNonRetryableFailure(
                    step.name + " timed out after " + step.timeoutHours + "h waiting for "
                            + step.signalName, "SIGNAL_TIMEOUT");
        }
        history.add(new StepRecord(stage.name, step.name, "SUCCESS", 1, "signal received"));
    }

    private boolean awaitNamedSignal(String signalName, Duration timeout) {
        waitingOnSignal = signalName;
        try {
            switch (signalName) {
                case "pledgeCardReady":
                    return Workflow.await(timeout, () -> pledgeCardSignal);
                case "retryLoanStatus": {
                    boolean r = Workflow.await(timeout, () -> retryLoanStatusSignal);
                    retryLoanStatusSignal = false;
                    return r;
                }
                case "retryFundTransferStatus": {
                    boolean r = Workflow.await(timeout, () -> retryFundTransferSignal);
                    retryFundTransferSignal = false;
                    return r;
                }
                default:
                    throw ApplicationFailure.newNonRetryableFailure(
                            "Unmapped signal name in config: " + signalName, "CONFIG_ERROR");
            }
        } finally {
            waitingOnSignal = null;
        }
    }

    private boolean awaitRetryStep(Duration timeout) {
        waitingOnSignal = "retryStep";
        try {
            boolean r = Workflow.await(timeout, () -> retryStepSignal);
            retryStepSignal = false;
            return r;
        } finally {
            waitingOnSignal = null;
        }
    }

    // ---------------------------------------------------------------- signals

    @Override
    public void pledgeCardReady(Map<String, String> payload) {
        if (payload != null) {
            ctx.putAll(payload);
        }
        pledgeCardSignal = true;
    }

    @Override
    public void retryLoanStatus() {
        retryLoanStatusSignal = true;
    }

    @Override
    public void retryFundTransferStatus() {
        retryFundTransferSignal = true;
    }

    @Override
    public void retryStep() {
        retryStepSignal = true;
    }

    @Override
    public FlowStatus status() {
        FlowStatus s = new FlowStatus();
        s.lenderSlug = lenderSlug;
        s.loanRequestId = loanRequestId;
        s.currentStage = currentStage;
        s.currentStep = currentStep;
        s.waitingOnSignal = waitingOnSignal;
        s.parkedAt = parkedAt;
        s.skeletonVersion = skeletonVersion;
        s.transportVersion = transportVersion;
        s.lastBindingVersion = lastBindingVersion;
        s.history = new ArrayList<>(history);
        s.context = new LinkedHashMap<>(ctx);
        return s;
    }

    // ---------------------------------------------------------------- helpers

    private StageActivity.CallRequest request(FlowSkeleton.Stage stage, FlowSkeleton.Step step,
                                              String callName, String callKey) {
        Map<String, String> snapshot = new LinkedHashMap<>(ctx);
        // Deterministic per-call request id. Replay reproduces the same value.
        snapshot.put("_requestId", "RPK-" + loanRequestId + "-"
                + Workflow.randomUUID().toString().substring(0, 8));
        return new StageActivity.CallRequest(lenderSlug, stage.name, step.name, callName,
                callKey, snapshot);
    }

    /** Binding lookup key: "<STAGE>.<step>" or "<STAGE>.<step>.<branch>". */
    private String callKey(FlowSkeleton.Stage stage, FlowSkeleton.Step step, String branchName) {
        String base = stage.name + "." + step.name;
        return branchName == null ? base : base + "." + branchName;
    }

    /**
     * Activity type name shown in the Temporal UI timeline, e.g.
     * "PRECHECK.panValidation". Config-derived but with NO per-instance data, so
     * the type set stays bounded. Handled on the worker by DynamicStageActivityImpl.
     */
    private String activityName(FlowSkeleton.Stage stage, String callName) {
        return stage.name + "." + callName;
    }

    /**
     * ActivityOptions come from the stage's knobs slice, with an optional
     * per-call override. These are the one piece of config the workflow must
     * choose, which is why they are re-read per stage rather than per attempt.
     */
    private ActivityStub stub(StageKnobs knobs, String callKey) {
        Retry r = knobs.retryFor(callKey);
        long timeoutSeconds = knobs.timeoutFor(callKey);
        return Workflow.newUntypedActivityStub(ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(timeoutSeconds + 10L))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(r.initialIntervalSeconds))
                        // > 1.0 matters: constant-interval retries during a bank
                        // outage burn Temporal Actions with nothing to show for it.
                        .setBackoffCoefficient(r.backoffCoefficient)
                        .setMaximumAttempts(r.maximumAttempts)
                        .setDoNotRetry(doNotRetry(knobs, r))
                        .build())
                .build());
    }

    /**
     * A per-call retry override must not silently drop the stage-level
     * non-retryable list, and BUSINESS_REJECT is never retryable whatever config
     * says. CONFIG_ERROR is deliberately absent: it stays retryable so that a
     * config fix published mid-backoff is picked up by the next attempt.
     */
    private String[] doNotRetry(StageKnobs knobs, Retry effective) {
        Set<String> types = new LinkedHashSet<>(knobs.retry.nonRetryableErrorTypes);
        types.addAll(effective.nonRetryableErrorTypes);
        types.add(StageActivityImpl.BUSINESS_REJECT);
        return types.toArray(new String[0]);
    }

    /** ctx key carrying one fan-out branch verdict, e.g. "parallelChecks.panValidation.outcome". */
    private String branchKey(FlowSkeleton.Step step, String branchName) {
        return step.name + "." + branchName + ".outcome";
    }

    /** ctx key carrying the gate verdict: PROCEED | PROCEED_DEGRADED | STOP. */
    private String decisionKey(FlowSkeleton.Step step) {
        return step.name + ".decision";
    }

    private boolean conditionHolds(Predicate p) {
        if (p == null) {
            return true;
        }
        return Predicates.evaluate(p, ctx.get(p.ctx));
    }

    private boolean isBusinessReject(ActivityFailure af) {
        Throwable c = af.getCause();
        return c instanceof ApplicationFailure
                && StageActivityImpl.BUSINESS_REJECT.equals(((ApplicationFailure) c).getType());
    }

    private String rootMessage(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null) {
            cur = cur.getCause();
        }
        return cur.getMessage();
    }
}
