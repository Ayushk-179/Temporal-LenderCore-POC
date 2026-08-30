package com.rupeek.poc.temporal;

import com.rupeek.poc.temporal.activity.StageActivity;
import com.rupeek.poc.temporal.activity.StageActivityImpl;
import com.rupeek.poc.temporal.config.LenderConfig;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflow;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These run against TestWorkflowEnvironment, which SKIPS TIME.
 *
 * The 30-poll loan-status loop at 120s intervals is 60 minutes of wall clock.
 * These tests execute it in milliseconds. That is the single most persuasive
 * developer-experience argument in the whole POC — demo it live.
 */
class LenderFlowWorkflowTest {

    private static final String TASK_QUEUE = "lih-federal-work";

    private TestWorkflowEnvironment env;
    private Worker worker;
    private WorkflowClient client;

    @BeforeEach
    void setUp() {
        env = TestWorkflowEnvironment.newInstance();
        worker = env.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    // ------------------------------------------------------------ fake activity

    /**
     * Scripted stand-in for Beeceptor. Keeps tests hermetic; the live demo uses
     * the real StageActivityImpl against Beeceptor.
     */
    static class FakeStageActivity implements StageActivity {
        final Map<String, String> scripted = new LinkedHashMap<>();
        final AtomicInteger loanStatusCalls = new AtomicInteger();
        final AtomicInteger sanctionCalls = new AtomicInteger();
        int loanStatusTerminalAfter = 3;
        int sanctionTerminalAfter = 3;
        String panStatusCode = "E";

        @Override
        public LenderConfig loadConfig() {
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("lender-config.json")) {
                return new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(in, LenderConfig.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CallResult execute(CallRequest req) {
            String name = req.callName == null ? req.stepName : req.callName;
            CallResult r = new CallResult();
            r.callName = name;
            r.httpStatus = 200;

            switch (name) {
                case "panValidation":
                    if (!"E".equals(panStatusCode)) {
                        throw ApplicationFailure.newNonRetryableFailure(
                                "pan rejected", StageActivityImpl.BUSINESS_REJECT);
                    }
                    r.extracted.put("panStatus", "SUCCESS");
                    break;
                case "dedupeCheck":
                    r.extracted.put("precheckDedupeFlag", "N");
                    r.extracted.put("aadhaarChecksNeeded",
                            scripted.getOrDefault("aadhaarChecksNeeded", "true"));
                    break;
                case "dedupe":
                    r.extracted.put("dedupeFlag", scripted.getOrDefault("dedupeFlag", "N"));
                    r.extracted.put("partialKyc", "N");
                    break;
                case "customerCreation":
                    r.extracted.put("customerId", "CIF-1001");
                    break;
                case "accountInfo":
                    r.extracted.put("loanAccountNumber", "LA-77001");
                    break;
                case "loanStatus": {
                    int n = loanStatusCalls.incrementAndGet();
                    boolean done = n >= loanStatusTerminalAfter;
                    r.extracted.put("fedLoanStatus", done ? "SUCCESS" : "PENDING");
                    r.terminal = done;
                    break;
                }
                case "sanction": {
                    int n = sanctionCalls.incrementAndGet();
                    boolean done = n >= sanctionTerminalAfter;
                    r.extracted.put("sanctionStatus", done ? "APPROVED" : "PENDING");
                    r.terminal = done;
                    break;
                }
                case "fundTransferStatus":
                    r.extracted.put("fundTransferStatus", "SUCCESS");
                    r.extracted.put("utr", "UTR123456789");
                    r.terminal = true;
                    break;
                case "dedupeRetry":
                    r.extracted.put("customerId", "CIF-1001");
                    r.terminal = true;
                    break;
                default:
                    r.extracted.put(name + "Status", "SUCCESS");
            }
            return r;
        }
    }

    private LenderFlowWorkflow stub(String loanRequestId) {
        return client.newWorkflowStub(LenderFlowWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(TASK_QUEUE)
                        .setWorkflowId("lender-flow-" + loanRequestId)
                        .build());
    }

    private static Map<String, String> seed() {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("customerName", "TEST CUSTOMER");
        s.put("pan", "AAAPZ1234C");
        s.put("mobile", "9000000000");
        s.put("dob", "1990-01-15");
        s.put("aadhaarRef", "XXXX-XXXX-1234");
        return s;
    }

    // ------------------------------------------------------------------ tests

    @Test
    void happyPath_allThreeStages_withTimeSkippedPollsAndEsignSignal() {
        FakeStageActivity acts = new FakeStageActivity();
        worker.registerActivitiesImplementations(acts);
        env.start();

        LenderFlowWorkflow wf = stub("LR-HAPPY-1");
        WorkflowClient.start(wf::run,
                new LenderFlowWorkflow.FlowInput("federal", "LR-HAPPY-1", seed()));

        // The e-sign wait would block for up to 72h of real time. Time skipping
        // means we can just signal it and move on.
        wf.pledgeCardReady(Map.of("esignCompletedAt", "test"));

        LenderFlowWorkflow.FlowResult result = wf.run(
                new LenderFlowWorkflow.FlowInput("federal", "LR-HAPPY-1", seed()));

        assertEquals("COMPLETED", result.outcome);
        assertEquals("UTR123456789", result.finalContext.get("utr"));
        assertEquals("LA-77001", result.finalContext.get("loanAccountNumber"));
    }

    @Test
    void businessRejection_isNotRetried_andSurfacesAsRejected() {
        FakeStageActivity acts = new FakeStageActivity();
        acts.panStatusCode = "F";           // fake PAN -> lender says no
        worker.registerActivitiesImplementations(acts);
        env.start();

        LenderFlowWorkflow wf = stub("LR-REJECT-1");
        LenderFlowWorkflow.FlowResult result = wf.run(
                new LenderFlowWorkflow.FlowInput("federal", "LR-REJECT-1", seed()));

        assertEquals("REJECTED", result.outcome);
        assertTrue(result.failedAt.startsWith("PRECHECK/parallelChecks"));
    }

    @Test
    void customerCreation_isSkipped_whenDedupeFlagIsY() {
        FakeStageActivity acts = new FakeStageActivity();
        acts.scripted.put("dedupeFlag", "Y");
        worker.registerActivitiesImplementations(acts);
        env.start();

        LenderFlowWorkflow wf = stub("LR-SKIP-1");
        WorkflowClient.start(wf::run,
                new LenderFlowWorkflow.FlowInput("federal", "LR-SKIP-1", seed()));
        wf.pledgeCardReady(Map.of());
        wf.run(new LenderFlowWorkflow.FlowInput("federal", "LR-SKIP-1", seed()));

        LenderFlowWorkflow.FlowStatus s = wf.status();
        assertTrue(s.history.stream().anyMatch(
                h -> "customerCreation".equals(h.step) && "SKIPPED".equals(h.status)));
    }

    @Test
    void aadhaarChecks_areSkipped_whenDedupeSaysNotNeeded() {
        FakeStageActivity acts = new FakeStageActivity();
        acts.scripted.put("aadhaarChecksNeeded", "false");
        worker.registerActivitiesImplementations(acts);
        env.start();

        LenderFlowWorkflow wf = stub("LR-AADHAAR-1");
        WorkflowClient.start(wf::run,
                new LenderFlowWorkflow.FlowInput("federal", "LR-AADHAAR-1", seed()));
        wf.pledgeCardReady(Map.of());
        wf.run(new LenderFlowWorkflow.FlowInput("federal", "LR-AADHAAR-1", seed()));

        LenderFlowWorkflow.FlowStatus s = wf.status();
        assertTrue(s.history.stream().anyMatch(
                h -> h.step.endsWith("panAadhaarLink") && "SKIPPED".equals(h.status)));
    }

    @Test
    void loanStatusPoll_exhausts_thenParksForManualRetrySignal() {
        FakeStageActivity acts = new FakeStageActivity();
        acts.loanStatusTerminalAfter = 45;   // never terminal within maxAttempts=30
        worker.registerActivitiesImplementations(acts);
        env.start();

        LenderFlowWorkflow wf = stub("LR-EXHAUST-1");
        WorkflowClient.start(wf::run,
                new LenderFlowWorkflow.FlowInput("federal", "LR-EXHAUST-1", seed()));

        // 30 polls x 120s = 1 hour, skipped instantly.
        env.sleep(java.time.Duration.ofHours(2));

        LenderFlowWorkflow.FlowStatus parked = wf.status();
        assertEquals("retryLoanStatus", parked.waitingOnSignal);

        wf.retryLoanStatus();                // ops clicks retry
        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(
                new LenderFlowWorkflow.FlowInput("federal", "LR-EXHAUST-1", seed()));

        assertEquals("COMPLETED", result.outcome);
    }

    @Test
    void secondLender_runsWithZeroCodeChange() {
        FakeStageActivity acts = new FakeStageActivity();
        Worker demoWorker = env.newWorker("lih-demobank-work");
        demoWorker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
        demoWorker.registerActivitiesImplementations(acts);
        worker.registerActivitiesImplementations(acts);
        env.start();

        LenderFlowWorkflow wf = client.newWorkflowStub(LenderFlowWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue("lih-demobank-work")
                        .setWorkflowId("lender-flow-LR-DEMO-1")
                        .build());

        LenderFlowWorkflow.FlowResult result = wf.run(
                new LenderFlowWorkflow.FlowInput("demobank", "LR-DEMO-1", seed()));

        assertEquals("COMPLETED", result.outcome);
    }
}
