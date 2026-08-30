package com.rupeek.poc.temporal.workflow;

import com.rupeek.poc.temporal.activity.StageActivity;
import com.rupeek.poc.temporal.activity.StageActivityImpl;
import com.rupeek.poc.temporal.config.LenderConfig;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

        StageActivity activities = Workflow.newActivityStub(
                StageActivity.class, activityOptions(lender));

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
        }

        result.finalContext = new LinkedHashMap<>(ctx);
        return result;
    }

    // ------------------------------------------------------------- step modes

    private void runSync(StageActivity acts, LenderConfig.Lender lender,
                         LenderConfig.Stage stage, LenderConfig.Step step) {
        StageActivity.CallRequest req = request(lender, stage, step, step.call, step.name);
        StageActivity.CallResult res = acts.execute(req);
        ctx.putAll(res.extracted);
        history.add(new StepRecord(stage.name, step.name, "SUCCESS", 1,
                "http " + res.httpStatus));
    }

    /**
     * The 4 parallel precheck calls. Two of them carry a condition, so the fan-out
     * width is decided by config at runtime, not by code.
     *
     * Promise.allOf blocks the workflow, not a thread. The worker pod is free
     * while these are in flight — contrast CompletableFuture.allOf holding a
     * pod thread for the slowest bank endpoint.
     */
    private void runParallel(StageActivity acts, LenderConfig.Lender lender,
                             LenderConfig.Stage stage, LenderConfig.Step step) {
        List<Promise<StageActivity.CallResult>> promises = new ArrayList<>();
        List<String> launched = new ArrayList<>();

        for (LenderConfig.Call call : step.calls) {
            if (!conditionHolds(call.condition)) {
                history.add(new StepRecord(stage.name, step.name + "/" + call.name,
                        "SKIPPED", 0, "condition not met"));
                continue;
            }
            StageActivity.CallRequest req = request(lender, stage, step, call, call.name);
            promises.add(Async.function(acts::execute, req));
            launched.add(call.name);
        }

        if (promises.isEmpty()) {
            history.add(new StepRecord(stage.name, step.name, "SKIPPED", 0, "no branches enabled"));
            return;
        }

        // Join. Any branch failure surfaces here after its own retry policy is exhausted.
        Promise.allOf(promises).get();

        for (Promise<StageActivity.CallResult> p : promises) {
            StageActivity.CallResult r = p.get();
            ctx.putAll(r.extracted);
        }
        history.add(new StepRecord(stage.name, step.name, "SUCCESS", promises.size(),
                "parallel: " + String.join(", ", launched)));
    }

    /**
     * Durable polling. This loop is the reason lender-async-service is unnecessary:
     * no PendingCorrelation document, no poll_lock, no ASYNC_RESUME re-enqueue.
     * Workflow.sleep survives pod death, deploys and node drains.
     */
    private void runPoll(StageActivity acts, LenderConfig.Lender lender,
                         LenderConfig.Stage stage, LenderConfig.Step step) {
        int attempt = 0;
        while (true) {
            attempt++;
            StageActivity.CallRequest req = request(lender, stage, step, step.call, step.name);
            StageActivity.CallResult res = acts.execute(req);
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

    private ActivityOptions activityOptions(LenderConfig.Lender lender) {
        LenderConfig.Retry r = lender.defaultRetry;
        return ActivityOptions.newBuilder()
                .setStartToCloseTimeout(
                        Duration.ofSeconds(lender.transport.defaultTimeoutSeconds + 10L))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(r.initialIntervalSeconds))
                        // > 1.0 matters: constant-interval retries during a bank
                        // outage burn Temporal Actions with nothing to show for it.
                        .setBackoffCoefficient(r.backoffCoefficient)
                        .setMaximumAttempts(r.maximumAttempts)
                        .setDoNotRetry(r.nonRetryableErrorTypes.toArray(new String[0]))
                        .build())
                .build();
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
