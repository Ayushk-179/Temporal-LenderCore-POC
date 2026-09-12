package com.rupeek.poc.temporal.workflow;

import com.rupeek.poc.temporal.activity.StageActivity;
import com.rupeek.poc.temporal.activity.StageActivityImpl;
import com.rupeek.poc.temporal.config.LenderConfig;
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
 * ONE workflow class for every lender.
 *
 * Adding a lender means adding a config document. It does not mean adding a
 * BPMN file, a dispatcher, a set of adapters, or a branch in this class.
 *
 * DETERMINISM RULES OBSERVED HERE — this is the part to review carefully:
 *   - no System.currentTimeMillis()  -> Workflow.currentTimeMillis()
 *   - no UUID.randomUUID()          -> Workflow.randomUUID()
 *   - no Thread.sleep()             -> Workflow.sleep()
 *   - no HTTP, no Mongo             -> activities only
 *   - config read ONCE via activity -> recorded in history, stable on replay
 */
public class LenderFlowWorkflowImpl implements LenderFlowWorkflow {

    private final Map<String, String> ctx = new LinkedHashMap<>();
    private final List<StepRecord> history = new ArrayList<>();

    private String lenderSlug;
    private String loanRequestId;
    private String currentStage = "NOT_STARTED";
    private String currentStep = "NOT_STARTED";
    private String waitingOnSignal;

    private boolean pledgeCardSignal;
    private boolean retryLoanStatusSignal;
    private boolean retryFundTransferSignal;

    private StageActivity bootstrapActivities;

    @Override
    public FlowResult run(FlowInput input) {
        this.lenderSlug = input.lenderSlug;
        this.loanRequestId = input.loanRequestId;
        ctx.putAll(input.seed);
        ctx.put("loanRequestId", input.loanRequestId);
        ctx.put("lenderSlug", input.lenderSlug);

        // Short-timeout stub purely for config load.
        bootstrapActivities = Workflow.newActivityStub(
                StageActivity.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(20))
                        .build());

        LenderConfig config = bootstrapActivities.loadConfig();
        LenderConfig.Lender lender = config.lender(lenderSlug);

        // Untyped stub: the activity TYPE name is supplied per call from config,
        // so the UI timeline names each lender call instead of showing N x "Execute".
        ActivityStub activities = Workflow.newUntypedActivityStub(activityOptions(lender));

        FlowResult result = new FlowResult();

        try {
            for (LenderConfig.Stage stage : lender.stages) {
                currentStage = stage.name;
                for (LenderConfig.Step step : stage.steps) {
                    currentStep = step.name;

                    if (!conditionHolds(step.condition)) {
                        history.add(new StepRecord(stage.name, step.name, "SKIPPED", 0,
                                "condition not met"));
                        continue;
                    }

                    switch (step.mode.toUpperCase()) {
                        case "SYNC":
                            runSync(activities, lender, stage, step);
                            break;
                        case "PARALLEL":
                            runParallel(activities, lender, stage, step);
                            break;
                        case "POLL":
                            runPoll(activities, lender, stage, step);
                            break;
                        case "AWAIT_SIGNAL":
                            runAwaitSignal(stage, step);
                            break;
                        default:
                            throw ApplicationFailure.newNonRetryableFailure(
                                    "Unknown step mode: " + step.mode, "CONFIG_ERROR");
                    }
                }
            }
            result.outcome = "COMPLETED";
        } catch (ActivityFailure af) {
            result.outcome = isBusinessReject(af) ? "REJECTED" : "FAILED";
            result.failedAt = currentStage + "/" + currentStep;
            result.reason = rootMessage(af);
            history.add(new StepRecord(currentStage, currentStep, result.outcome, 0, result.reason));
        } catch (ApplicationFailure af) {
            // Thrown by the workflow itself — today only the PARALLEL gate, which has
            // already decided REJECTED vs FAILED. Every other type (CONFIG_ERROR,
            // POLL_EXHAUSTED, SIGNAL_TIMEOUT) still fails the execution loudly.
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

    // ------------------------------------------------------------- step modes

    private void runSync(ActivityStub acts, LenderConfig.Lender lender,
                         LenderConfig.Stage stage, LenderConfig.Step step) {
        StageActivity.CallRequest req = request(lender, stage, step, step.call, step.name);
        StageActivity.CallResult res = acts.execute(
                activityName(stage, step.name), StageActivity.CallResult.class, req);
        ctx.putAll(res.extracted);
        history.add(new StepRecord(stage.name, step.name, "SUCCESS", 1,
                "http " + res.httpStatus));
    }

    /**
     * The 4 parallel precheck calls. Two of them carry a condition, so the fan-out
     * width is decided by config at runtime, not by code.
     *
     * executeAsync blocks the workflow, not a thread. The worker pod is free while
     * these are in flight — contrast CompletableFuture.allOf holding a pod thread
     * for the slowest bank endpoint.
     *
     * The join is SETTLE-ALL, not fail-fast:
     *   1. every launched branch is awaited, including branches still running when
     *      a sibling has already failed. Promise.get() parks the workflow
     *      coroutine, the branches keep running concurrently, so the wall clock is
     *      still the slowest branch — not the sum;
     *   2. each branch has exhausted its OWN retry policy before its promise
     *      settles (per-call `retry` block, else lender.defaultRetry). A retryable
     *      500 is retried with backoff inside the branch, invisible here;
     *   3. every SUCCESSFUL branch's extract is merged into ctx in declared order,
     *      so a partial fan-out is no longer discarded;
     *   4. the go/no-go decision is taken once, after the join, from the collected
     *      outcomes plus the config gate (`mandatory`, `minSuccess`,
     *      `onBranchFailure`).
     */
    private void runParallel(ActivityStub defaultActs, LenderConfig.Lender lender,
                             LenderConfig.Stage stage, LenderConfig.Step step) {
        List<LenderConfig.Call> launched = new ArrayList<>();
        List<Promise<StageActivity.CallResult>> promises = new ArrayList<>();

        for (LenderConfig.Call call : step.calls) {
            if (!conditionHolds(call.condition)) {
                ctx.put(branchKey(step, call.name), "SKIPPED");
                history.add(new StepRecord(stage.name, step.name + "/" + call.name,
                        "SKIPPED", 0, "condition not met"));
                continue;
            }
            // A branch with its own retry/timeout config gets its own stub; the
            // rest share the lender-default stub built once by the caller.
            ActivityStub acts = (call.retry == null && call.timeoutSeconds == null)
                    ? defaultActs
                    : Workflow.newUntypedActivityStub(activityOptions(lender, call));

            promises.add(acts.executeAsync(activityName(stage, call.name),
                    StageActivity.CallResult.class,
                    request(lender, stage, step, call, call.name)));
            launched.add(call);
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
            LenderConfig.Call call = launched.get(i);
            String status;
            String detail;
            try {
                StageActivity.CallResult r = promises.get(i).get();
                // Declared order, not completion order: the merge stays deterministic.
                ctx.putAll(r.extracted);
                status = "SUCCESS";
                detail = "http " + r.httpStatus;
                succeeded.add(call.name);
            } catch (ActivityFailure af) {
                boolean reject = isBusinessReject(af);
                status = reject ? "REJECTED" : "FAILED";
                detail = rootMessage(af);
                (reject ? rejected : failed).add(call.name);
                if (reject || call.mandatory) {
                    blocking.add(call.name + "(" + status + ")");
                }
            }
            ctx.put(branchKey(step, call.name), status);
            history.add(new StepRecord(stage.name, step.name + "/" + call.name, status, 1, detail));
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
            // A decline is an answer, not a fault. No retry and no `mandatory:false`
            // can change it, so it stops the flow whatever the gate says.
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
     * Durable polling. This loop is the reason lender-async-service is unnecessary:
     * no PendingCorrelation document, no poll_lock, no ASYNC_RESUME re-enqueue.
     * Workflow.sleep survives pod death, deploys and node drains.
     */
    private void runPoll(ActivityStub acts, LenderConfig.Lender lender,
                         LenderConfig.Stage stage, LenderConfig.Step step) {
        int attempt = 0;
        while (true) {
            attempt++;
            StageActivity.CallRequest req = request(lender, stage, step, step.call, step.name);
            StageActivity.CallResult res = acts.execute(
                    activityName(stage, step.name), StageActivity.CallResult.class, req);
            ctx.putAll(res.extracted);

            if (res.terminal) {
                history.add(new StepRecord(stage.name, step.name, "SUCCESS", attempt,
                        "terminal after " + attempt + " poll(s)"));
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
    private void runAwaitSignal(LenderConfig.Stage stage, LenderConfig.Step step) {
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
    public FlowStatus status() {
        FlowStatus s = new FlowStatus();
        s.lenderSlug = lenderSlug;
        s.loanRequestId = loanRequestId;
        s.currentStage = currentStage;
        s.currentStep = currentStep;
        s.waitingOnSignal = waitingOnSignal;
        s.history = new ArrayList<>(history);
        s.context = new LinkedHashMap<>(ctx);
        return s;
    }

    // ---------------------------------------------------------------- helpers

    private StageActivity.CallRequest request(LenderConfig.Lender lender,
                                              LenderConfig.Stage stage,
                                              LenderConfig.Step step,
                                              LenderConfig.Call call,
                                              String callName) {
        Map<String, String> snapshot = new LinkedHashMap<>(ctx);
        // Deterministic per-call request id. Replay reproduces the same value.
        snapshot.put("_requestId", "RPK-" + loanRequestId + "-"
                + Workflow.randomUUID().toString().substring(0, 8));
        return new StageActivity.CallRequest(lenderSlug, stage.name, step.name, callName,
                call, lender.transport, snapshot);
    }

    /**
     * Activity type name shown in the Temporal UI timeline, e.g. "PRECHECK.panValidation".
     * Stage prefix makes stage boundaries readable in the graph without opening events.
     * Handled on the worker by DynamicStageActivityImpl.
     */
    private String activityName(LenderConfig.Stage stage, String callName) {
        return stage.name + "." + callName;
    }

    private ActivityOptions activityOptions(LenderConfig.Lender lender) {
        return activityOptions(lender, null);
    }

    /**
     * Per-call override: a single fan-out branch can carry its own retry policy and
     * timeout, e.g. a flaky NSDL endpoint retried 5 times while its siblings keep
     * the lender default of 3.
     */
    private ActivityOptions activityOptions(LenderConfig.Lender lender, LenderConfig.Call call) {
        LenderConfig.Retry r = (call != null && call.retry != null)
                ? call.retry : lender.defaultRetry;
        long timeoutSeconds = (call != null && call.timeoutSeconds != null)
                ? call.timeoutSeconds : lender.transport.defaultTimeoutSeconds;
        return ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(timeoutSeconds + 10L))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(r.initialIntervalSeconds))
                        // > 1.0 matters: constant-interval retries during a bank
                        // outage burn Temporal Actions with nothing to show for it.
                        .setBackoffCoefficient(r.backoffCoefficient)
                        .setMaximumAttempts(r.maximumAttempts)
                        .setDoNotRetry(doNotRetry(lender, r))
                        .build())
                .build();
    }

    /**
     * A per-call retry block must not silently drop the lender-level non-retryable
     * list, and BUSINESS_REJECT is never retryable whatever config says.
     */
    private String[] doNotRetry(LenderConfig.Lender lender, LenderConfig.Retry effective) {
        Set<String> types = new LinkedHashSet<>(lender.defaultRetry.nonRetryableErrorTypes);
        types.addAll(effective.nonRetryableErrorTypes);
        types.add(StageActivityImpl.BUSINESS_REJECT);
        return types.toArray(new String[0]);
    }

    /** ctx key carrying one fan-out branch verdict, e.g. "parallelChecks.panValidation.outcome". */
    private String branchKey(LenderConfig.Step step, String callName) {
        return step.name + "." + callName + ".outcome";
    }

    /** ctx key carrying the gate verdict: PROCEED | PROCEED_DEGRADED | STOP. */
    private String decisionKey(LenderConfig.Step step) {
        return step.name + ".decision";
    }

    private boolean conditionHolds(LenderConfig.Predicate p) {
        if (p == null) {
            return true;
        }
        return StageActivityImpl.evaluate(p, ctx.get(p.ctx));
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
