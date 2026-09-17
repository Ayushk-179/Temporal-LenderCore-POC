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
import org.slf4j.Logger;

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

    /**
     * Workflow.getLogger, NOT LoggerFactory.getLogger.
     *
     * A workflow method re-executes from the top on every replay. A plain slf4j
     * logger would re-emit every line each time a worker picks the workflow up —
     * so a loan that parks for a day would print its PRECHECK log dozens of times
     * and the log would read like the flow ran dozens of times. This logger is
     * replay-aware and suppresses output while replaying.
     */
    private static final Logger log = Workflow.getLogger(LenderFlowWorkflowImpl.class);

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

        log.info("=== FLOW START lender={} loanRequestId={} workflowId={} seedKeys={}",
                lenderSlug, loanRequestId, Workflow.getInfo().getWorkflowId(), input.seed.keySet());

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
        log.info("CONFIG pinned skeleton={} ({} stages: {}) — this shape is now fixed "
                        + "for the life of this loan",
                skeleton.version, skeleton.stages.size(), stageNames(skeleton));

        FlowResult result = new FlowResult();

        try {
            for (FlowSkeleton.Stage stage : skeleton.stages) {
                currentStage = stage.name;

                // ---- resolution point 2: PER STAGE. A retry/timeout tune published
                // while this loan was in PRECHECK reaches its FUND_TRANSFER steps.
                StageKnobs knobs = configActivities.loadStageKnobs(lenderSlug, stage.name);
                transportVersion = knobs.transportVersion;
                log.info("--- STAGE {} [{} steps] knobs re-read from transport={} "
                                + "(retry {}x initial {}s x{} backoff, timeout {}s)",
                        stage.name, stage.steps.size(), knobs.transportVersion,
                        knobs.retry.maximumAttempts, knobs.retry.initialIntervalSeconds,
                        knobs.retry.backoffCoefficient, knobs.timeoutSeconds);

                for (FlowSkeleton.Step step : stage.steps) {
                    currentStep = step.name;
                    String where = stage.name + "/" + step.name;

                    if (!conditionHolds(step.condition, where)) {
                        log.info("STEP {} SKIPPED — {}", where, describe(step.condition));
                        history.add(new StepRecord(stage.name, step.name, "SKIPPED", 0,
                                "condition not met: " + describe(step.condition)));
                        continue;
                    }
                    log.info("STEP {} mode={} onFailure={}", where, step.mode, step.onFailure);
                    runStep(stage, step, knobs);
                }
            }
            result.outcome = "COMPLETED";
            log.info("=== FLOW COMPLETED lender={} loanRequestId={} ctxKeys={}",
                    lenderSlug, loanRequestId, ctx.keySet());
        } catch (ActivityFailure af) {
            result.outcome = isBusinessReject(af) ? "REJECTED" : "FAILED";
            result.failedAt = currentStage + "/" + currentStep;
            result.reason = rootMessage(af);
            log.error("=== FLOW {} at {} — {}", result.outcome, result.failedAt, result.reason);
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
            log.error("=== FLOW {} at {} — {}", result.outcome, result.failedAt, result.reason);
            history.add(new StepRecord(currentStage, currentStep, result.outcome, 0, result.reason));
        }

        result.finalContext = new LinkedHashMap<>(ctx);

        // A loan that did not complete FAILS THE EXECUTION, so it shows red in the
        // Temporal UI instead of green "Completed". Returning a FlowResult with
        // outcome=FAILED made a broken loan indistinguishable from a disbursed one
        // in the workflow list — which is where anyone looks first.
        //
        // The failure TYPE keeps the two apart for metrics and for anyone filtering:
        //   BUSINESS_REJECT    the lender said no. Normal, expected, high volume.
        //   TECHNICAL_FAILURE  something broke, or a parked step was never rescued.
        //
        // Worth knowing: with both shown as Failed, "failed workflow count" alone is
        // not an alerting signal — it has to be filtered by type, or a busy day of
        // declines looks like an outage.
        //
        // The whole FlowResult rides along as failure details, so the final context
        // and step history stay visible on the UI's failure panel.
        if (!"COMPLETED".equals(result.outcome)) {
            throw ApplicationFailure.newNonRetryableFailure(
                    result.outcome + " at " + result.failedAt + " — " + result.reason,
                    "REJECTED".equals(result.outcome)
                            ? StageActivityImpl.BUSINESS_REJECT
                            : StageActivityImpl.TECHNICAL_FAILURE,
                    result);
        }
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
        if (step.mode == null) {
            // A config error must fail the execution loudly. A raw NPE here would
            // fail the WORKFLOW TASK instead, and the Java SDK retries those
            // forever — the loan would hang with no outcome and no error.
            throw ApplicationFailure.newNonRetryableFailure(
                    "Step " + stage.name + "/" + step.name + " has no mode", "CONFIG_ERROR");
        }
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
                    log.error("STEP {}/{} FAILED (onFailure={}, businessReject={}) — {}",
                            stage.name, step.name, step.onFailure, isBusinessReject(af),
                            rootMessage(af));
                    throw af;
                }
                parkedAt = stage.name + "/" + step.name;
                log.warn("STEP {} PARKED — retries exhausted: {}", parkedAt, rootMessage(af));
                log.warn("STEP {} waiting up to {}h for retryStep. Fix the binding document "
                                + "in the config store, then: Runner signal {} retryStep",
                        parkedAt, step.parkTimeoutHours, loanRequestId);
                history.add(new StepRecord(stage.name, step.name, "AWAITING_INTERVENTION", 0,
                        "retries exhausted: " + rootMessage(af)
                                + " — fix the binding in the config store, then signal retryStep"));

                boolean released = awaitRetryStep(Duration.ofHours(step.parkTimeoutHours));
                parkedAt = null;
                if (!released) {
                    log.error("STEP {}/{} park timed out after {}h without retryStep",
                            stage.name, step.name, step.parkTimeoutHours);
                    throw af;
                }

                // A fix may be a knob change (more attempts, longer timeout), not
                // only a binding change, so re-read this stage's slice too.
                effective = configActivities.loadStageKnobs(lenderSlug, stage.name);
                transportVersion = effective.transportVersion;
                log.info("STEP {}/{} RELEASED by retryStep — re-resolving config "
                                + "(transport={}, retry {}x) and re-running the step",
                        stage.name, step.name, effective.transportVersion,
                        effective.retry.maximumAttempts);
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
        log.info("STEP {}/{} SUCCESS http={} binding={} extracted={}",
                stage.name, step.name, res.httpStatus, res.bindingVersion, res.extracted);
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
    private void runParallel(FlowSkeleton.Stage stage, FlowSkeleton.Step step,
                             StageKnobs knobs) {
        StageKnobs effective = knobs;

        // Branch conditions are evaluated ONCE, before the first round. Re-checking
        // them after a park could change the fan-out width halfway through a step.
        List<FlowSkeleton.Branch> enabled = new ArrayList<>();
        for (FlowSkeleton.Branch branch : step.branches) {
            String where = stage.name + "/" + step.name + "/" + branch.name;
            if (!conditionHolds(branch.condition, where)) {
                log.info("BRANCH {} SKIPPED — {}", where, describe(branch.condition));
                ctx.put(branchKey(step, branch.name), "SKIPPED");
                history.add(new StepRecord(stage.name, step.name + "/" + branch.name,
                        "SKIPPED", 0, "condition not met: " + describe(branch.condition)));
                continue;
            }
            enabled.add(branch);
        }

        if (enabled.isEmpty()) {
            ctx.put(decisionKey(step), "PROCEED");
            log.info("FANOUT {}/{} no branches enabled by config -> PROCEED",
                    stage.name, step.name);
            history.add(new StepRecord(stage.name, step.name, "SKIPPED", 0, "no branches enabled"));
            return;
        }

        // Cumulative across rounds: a branch that succeeded in round 1 is never
        // re-fired, however many times the step parks.
        Map<String, String> outcome = new LinkedHashMap<>();
        int round = 0;

        while (true) {
            round++;
            List<FlowSkeleton.Branch> toRun = new ArrayList<>();
            for (FlowSkeleton.Branch b : enabled) {
                if (!"SUCCESS".equals(outcome.get(b.name))) {
                    toRun.add(b);
                }
            }

            List<Promise<StageActivity.CallResult>> promises = new ArrayList<>();
            for (FlowSkeleton.Branch branch : toRun) {
                String callKey = callKey(stage, step, branch.name);
                log.info("BRANCH {}/{}/{} LAUNCH (round {}) mandatory={} retry={}x timeout={}s",
                        stage.name, step.name, branch.name, round, branch.mandatory,
                        effective.retryFor(callKey).maximumAttempts,
                        effective.timeoutFor(callKey));
                promises.add(stub(effective, callKey).executeAsync(
                        activityName(stage, branch.name), StageActivity.CallResult.class,
                        request(stage, step, branch.name, callKey)));
            }
            log.info("FANOUT {}/{} round {} awaiting {} branch(es) — settle-all, not fail-fast",
                    stage.name, step.name, round, promises.size());

            // ------------------------------------------------------ settle all
            List<String> rejected = new ArrayList<>();
            for (int i = 0; i < promises.size(); i++) {
                FlowSkeleton.Branch branch = toRun.get(i);
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
                    log.info("BRANCH {}/{}/{} SUCCESS http={} binding={} extracted={}",
                            stage.name, step.name, branch.name, r.httpStatus,
                            r.bindingVersion, r.extracted);
                } catch (ActivityFailure af) {
                    boolean reject = isBusinessReject(af);
                    status = reject ? "REJECTED" : "FAILED";
                    detail = rootMessage(af);
                    if (reject) {
                        rejected.add(branch.name);
                    }
                    log.warn("BRANCH {}/{}/{} {} (mandatory={}) — {}",
                            stage.name, step.name, branch.name, status, branch.mandatory, detail);
                }
                outcome.put(branch.name, status);
                ctx.put(branchKey(step, branch.name), status);
                history.add(new StepRecord(stage.name, step.name + "/" + branch.name, status,
                        round, detail, bindingVersion));
            }

            // ---------------------------- tally the full picture, across all rounds
            List<String> succeeded = new ArrayList<>();
            List<String> failed = new ArrayList<>();
            List<String> blocking = new ArrayList<>();
            for (FlowSkeleton.Branch b : enabled) {
                String o = outcome.get(b.name);
                if ("SUCCESS".equals(o)) {
                    succeeded.add(b.name);
                } else {
                    // REJECTED counts as failed for the gate; it is handled below.
                    failed.add(b.name);
                    if (b.mandatory || "REJECTED".equals(o)) {
                        blocking.add(b.name);
                    }
                }
            }

            // Set BEFORE the rejection check: a stopped fan-out must still leave a
            // readable tally in context for the status query and for downstream
            // conditions.
            ctx.put(step.name + ".successCount", String.valueOf(succeeded.size()));
            ctx.put(step.name + ".branchCount", String.valueOf(enabled.size()));
            ctx.put(step.name + ".rejectedCalls", String.join(",", rejected));
            List<String> technicallyFailed = new ArrayList<>(failed);
            technicallyFailed.removeAll(rejected);
            ctx.put(step.name + ".failedCalls", String.join(",", technicallyFailed));

            // A decline stops the flow whatever the gate or onFailure says. Retrying
            // cannot change it and no config edit can turn it into a yes, so it must
            // never park.
            if (!rejected.isEmpty()) {
                ctx.put(decisionKey(step), "STOP");
                log.error("GATE {}/{} STOP — lender declined: {}", stage.name, step.name, rejected);
                throw ApplicationFailure.newNonRetryableFailure(
                        step.name + " rejected by lender: " + String.join(", ", rejected),
                        StageActivityImpl.BUSINESS_REJECT);
            }

            boolean quorumMet = step.minSuccess <= 0 || succeeded.size() >= step.minSuccess;
            String summary = succeeded.size() + "/" + enabled.size() + " ok"
                    + (blocking.isEmpty() ? "" : "; blocking: " + String.join(", ", blocking))
                    + (quorumMet ? "" : "; minSuccess=" + step.minSuccess + " not met");
            log.info("GATE {}/{} {} | quorumMet={} (minSuccess={}) onBranchFailure={}",
                    stage.name, step.name, summary, quorumMet, step.minSuccess,
                    step.onBranchFailure);

            if (blocking.isEmpty() && quorumMet) {
                ctx.put(decisionKey(step), failed.isEmpty() ? "PROCEED" : "PROCEED_DEGRADED");
                log.info("GATE {}/{} -> {}", stage.name, step.name, ctx.get(decisionKey(step)));
                history.add(new StepRecord(stage.name, step.name,
                        failed.isEmpty() ? "SUCCESS" : "PARTIAL_SUCCESS", enabled.size(),
                        "parallel: " + summary
                                + (failed.isEmpty() ? "" : "; tolerated: " + String.join(", ", failed))));
                return;
            }

            if ("CONTINUE".equalsIgnoreCase(step.onBranchFailure)) {
                ctx.put(decisionKey(step), "PROCEED_DEGRADED");
                log.warn("GATE {}/{} -> PROCEED_DEGRADED (onBranchFailure=CONTINUE tolerated {})",
                        stage.name, step.name, blocking);
                history.add(new StepRecord(stage.name, step.name, "PARTIAL_SUCCESS", enabled.size(),
                        "parallel: " + summary + "; onBranchFailure=CONTINUE"));
                return;
            }

            // The gate is not met and the failures are TECHNICAL. Park instead of
            // killing the loan, and on release re-run ONLY the failed branches —
            // re-firing a branch that already succeeded could double-charge a
            // lender-side side effect.
            if (!"PARK".equalsIgnoreCase(step.onFailure)) {
                ctx.put(decisionKey(step), "STOP");
                log.error("GATE {}/{} STOP — {}", stage.name, step.name, summary);
                throw ApplicationFailure.newNonRetryableFailure(
                        step.name + " gate not met: " + summary,
                        StageActivityImpl.TECHNICAL_FAILURE);
            }

            parkedAt = stage.name + "/" + step.name;
            log.warn("FANOUT {} PARKED — {} ({} branch(es) still failing: {})",
                    parkedAt, summary, failed.size(), failed);
            log.warn("FANOUT {} waiting up to {}h for retryStep. Fix the binding(s), then: "
                            + "Runner signal {} retryStep — only {} will re-run",
                    parkedAt, step.parkTimeoutHours, loanRequestId, failed);
            history.add(new StepRecord(stage.name, step.name, "AWAITING_INTERVENTION",
                    enabled.size(), "gate not met: " + summary
                            + " — fix the binding(s) in the config store, then signal retryStep; "
                            + "only " + String.join(", ", failed) + " will re-run"));

            boolean released = awaitRetryStep(Duration.ofHours(step.parkTimeoutHours));
            parkedAt = null;
            if (!released) {
                ctx.put(decisionKey(step), "STOP");
                log.error("FANOUT {}/{} park timed out after {}h without retryStep",
                        stage.name, step.name, step.parkTimeoutHours);
                throw ApplicationFailure.newNonRetryableFailure(
                        step.name + " parked " + step.parkTimeoutHours
                                + "h without retryStep: " + summary,
                        StageActivityImpl.TECHNICAL_FAILURE);
            }

            effective = configActivities.loadStageKnobs(lenderSlug, stage.name);
            transportVersion = effective.transportVersion;
            log.info("FANOUT {}/{} RELEASED by retryStep — re-resolving config (transport={}) "
                            + "and re-running {} only",
                    stage.name, step.name, effective.transportVersion, failed);
            history.add(new StepRecord(stage.name, step.name, "RETRYING", enabled.size(),
                    "released by retryStep; re-running " + String.join(", ", failed)));
        }
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
            log.info("POLL {}/{} attempt {}/{} http={} terminal={} binding={} extracted={}",
                    stage.name, step.name, attempt, step.maxAttempts, res.httpStatus,
                    res.terminal, res.bindingVersion, res.extracted);

            if (res.terminal) {
                history.add(new StepRecord(stage.name, step.name, "SUCCESS", attempt,
                        "terminal after " + attempt + " poll(s)", res.bindingVersion));
                return;
            }

            if (attempt >= step.maxAttempts) {
                if (!"AWAIT_SIGNAL".equalsIgnoreCase(step.onExhausted)) {
                    log.error("POLL {}/{} exhausted {} attempts, onExhausted=FAIL",
                            stage.name, step.name, step.maxAttempts);
                    throw ApplicationFailure.newNonRetryableFailure(
                            step.name + " exhausted " + step.maxAttempts + " polls",
                            "POLL_EXHAUSTED");
                }
                log.warn("POLL {}/{} exhausted {} attempts -> parking on signal '{}'",
                        stage.name, step.name, step.maxAttempts, step.exhaustedSignalName);
                // Park for a human. Mirrors the BPMN receiveTask, minus the BPMN.
                history.add(new StepRecord(stage.name, step.name, "AWAITING_MANUAL_RETRY",
                        attempt, "signal: " + step.exhaustedSignalName));
                awaitNamedSignal(step.exhaustedSignalName, Duration.ofHours(step.parkTimeoutHours));
                attempt = 0;   // matches prod: counter resets on manual resume
                continue;
            }

            log.debug("POLL {}/{} sleeping {}s (durable — survives worker restart)",
                    stage.name, step.name, step.pollIntervalSeconds);
            Workflow.sleep(Duration.ofSeconds(step.pollIntervalSeconds));
        }
    }

    /** E-sign wait. Days long, zero resource cost, survives everything. */
    private void runAwaitSignal(FlowSkeleton.Stage stage, FlowSkeleton.Step step) {
        log.info("WAIT {}/{} awaiting signal '{}' for up to {}h (zero resource cost)",
                stage.name, step.name, step.signalName, step.timeoutHours);
        history.add(new StepRecord(stage.name, step.name, "WAITING", 0,
                "signal: " + step.signalName));
        boolean got = awaitNamedSignal(step.signalName, Duration.ofHours(step.timeoutHours));
        if (!got) {
            log.error("WAIT {}/{} timed out after {}h waiting for '{}'",
                    stage.name, step.name, step.timeoutHours, step.signalName);
            throw ApplicationFailure.newNonRetryableFailure(
                    step.name + " timed out after " + step.timeoutHours + "h waiting for "
                            + step.signalName, "SIGNAL_TIMEOUT");
        }
        log.info("WAIT {}/{} signal '{}' received", stage.name, step.name, step.signalName);
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

    /**
     * Note the deliberate asymmetry: the flag is NOT cleared on entry, so a
     * retryStep sent while the step was still on its last retry is still honoured
     * when it parks a moment later — which is the realistic ops sequence ("I fixed
     * it, release it"). The cost is that a stray retryStep sent when nothing is
     * parked will short-circuit the NEXT park. That is self-correcting (the step
     * simply re-runs and re-parks) but it is confusing in a log, so it is called
     * out explicitly.
     */
    private boolean awaitRetryStep(Duration timeout) {
        waitingOnSignal = "retryStep";
        if (retryStepSignal) {
            log.warn("retryStep was already pending when this step parked — releasing "
                    + "immediately without waiting. If nobody just fixed config, this step "
                    + "will fail again and park again.");
        }
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
        log.info("SIGNAL payload merged into ctx: {}",
                payload == null ? "(none)" : payload.keySet());
        log.info("SIGNAL received: pledgeCardReady"); 
        pledgeCardSignal = true;
    }

    @Override
    public void retryLoanStatus() {
        log.info("SIGNAL received: retryLoanStatus"); 
        retryLoanStatusSignal = true;
    }

    @Override
    public void retryFundTransferStatus() {
        log.info("SIGNAL received: retryFundTransferStatus"); 
        retryFundTransferSignal = true;
    }

    @Override
    public void retryStep() {
        log.info("SIGNAL received: retryStep"); 
        retryStepSignal = true;
    }

    /**
     * Keys that identify the loan or are derived per call. Letting an operator
     * rewrite loanRequestId would re-point every downstream idempotency key at a
     * different loan; _requestId is regenerated per attempt anyway, so patching it
     * only creates confusion.
     */
    private static final Set<String> PROTECTED_CTX_KEYS = new LinkedHashSet<>(
            java.util.Arrays.asList("loanRequestId", "lenderSlug", "_requestId"));

    @Override
    public void patchContext(Map<String, String> patch) {
        if (patch == null || patch.isEmpty()) {
            log.warn("SIGNAL patchContext ignored — empty patch");
            return;
        }
        Map<String, String> applied = new LinkedHashMap<>();
        List<String> refused = new ArrayList<>();
        for (Map.Entry<String, String> e : patch.entrySet()) {
            if (PROTECTED_CTX_KEYS.contains(e.getKey())) {
                refused.add(e.getKey());
                continue;
            }
            applied.put(e.getKey(), e.getValue());
        }

        if (!refused.isEmpty()) {
            log.error("SIGNAL patchContext REFUSED protected key(s) {} — these identify the "
                    + "loan and rewriting them would re-point its idempotency keys", refused);
        }
        if (applied.isEmpty()) {
            history.add(new StepRecord(currentStage, currentStep, "CONTEXT_PATCH_REFUSED", 0,
                    "all keys protected: " + refused));
            return;
        }

        Map<String, String> before = new LinkedHashMap<>();
        applied.keySet().forEach(k -> before.put(k, ctx.get(k)));
        ctx.putAll(applied);

        // Loud on purpose: this edits live loan state by hand.
        log.warn("SIGNAL patchContext APPLIED {} (was {}){}",
                applied, before, refused.isEmpty() ? "" : "; refused " + refused);
        history.add(new StepRecord(currentStage, currentStep, "CONTEXT_PATCHED", 0,
                "operator set " + applied.keySet() + " (was " + before + ")"
                        + (refused.isEmpty() ? "" : "; refused " + refused)));
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

    /**
     * Workflow-side conditions read the workflow CONTEXT only. A predicate with a
     * response {@code path} belongs on the binding (successWhen / terminalWhen /
     * businessRejectWhen) and is evaluated inside the activity.
     *
     * Both failure modes here are config errors, and both are converted to a
     * NON-RETRYABLE ApplicationFailure on purpose. A raw IllegalArgumentException
     * or NPE thrown from workflow code fails the WORKFLOW TASK, and the Java SDK
     * retries workflow tasks forever — the loan would hang silently with no
     * outcome. Converting makes a typo in config fail the loan loudly instead.
     */
    private boolean conditionHolds(Predicate p, String where) {
        if (p == null) {
            return true;
        }
        if (p.ctx == null) {
            throw ApplicationFailure.newNonRetryableFailure(
                    "Condition at " + where + " has no 'ctx' field. Workflow-side conditions "
                            + "read workflow context only; a response 'path' predicate belongs "
                            + "on the binding as successWhen/terminalWhen/businessRejectWhen.",
                    "CONFIG_ERROR");
        }
        boolean holds;
        try {
            holds = Predicates.evaluate(p, ctx.get(p.ctx));
        } catch (IllegalArgumentException e) {
            throw ApplicationFailure.newNonRetryableFailure(
                    "Condition at " + where + ": " + e.getMessage(), "CONFIG_ERROR");
        }
        log.info("CONDITION {} : {} -> {}", where, describe(p), holds);
        return holds;
    }

    /** Renders a predicate and the value it saw, so a skipped step explains itself. */
    private String describe(Predicate p) {
        if (p == null) {
            return "(no condition)";
        }
        String op = p.op == null ? "EQUALS" : p.op.toUpperCase();
        StringBuilder sb = new StringBuilder("ctx.").append(p.ctx).append(' ').append(op);
        if (!"BLANK".equals(op) && !"NOT_BLANK".equals(op)) {
            sb.append(' ').append(p.values != null && !p.values.isEmpty() ? p.values : p.value);
        }
        return sb.append(" [actual=").append(ctx.get(p.ctx)).append(']').toString();
    }

    private String stageNames(FlowSkeleton skeleton) {
        List<String> names = new ArrayList<>();
        skeleton.stages.forEach(st -> names.add(st.name));
        return String.join(" -> ", names);
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
