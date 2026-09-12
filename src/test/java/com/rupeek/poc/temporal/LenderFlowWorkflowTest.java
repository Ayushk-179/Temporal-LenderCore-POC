package com.rupeek.poc.temporal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rupeek.poc.temporal.activity.DynamicStageActivityImpl;
import com.rupeek.poc.temporal.activity.StageActivity;
import com.rupeek.poc.temporal.activity.StageActivityImpl;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.ResolvedBinding;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import com.rupeek.poc.temporal.config.seed.ConfigSeeder;
import com.rupeek.poc.temporal.config.store.ConfigResolver;
import com.rupeek.poc.temporal.config.store.ConfigStore;
import com.rupeek.poc.temporal.config.store.ConfigStores;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflow;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.WorkflowReplayer;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These run against TestWorkflowEnvironment, which SKIPS TIME.
 *
 * The 30-poll loan-status loop at 120s intervals is 60 minutes of wall clock.
 * These tests execute it in milliseconds.
 *
 * The config store here is a REAL FileConfigStore in a temp directory, seeded by
 * the REAL ConfigSeeder and read through the REAL ConfigResolver. Only the HTTP
 * call is faked. So the seeder, the two-plane split and the per-attempt
 * resolution are all under test, not stubbed around.
 */
class LenderFlowWorkflowTest {

    private static final String TASK_QUEUE = "lih-federal-work";
    private static final String SLUG = "federal";

    @TempDir
    Path storeDir;

    private TestWorkflowEnvironment env;
    private Worker worker;
    private WorkflowClient client;
    private ConfigStore store;
    private ConfigResolver resolver;

    @BeforeEach
    void setUp() {
        store = ConfigStores.file(storeDir);
        ConfigSeeder.seedAll(store);
        resolver = new ConfigResolver(store);

        env = TestWorkflowEnvironment.newInstance();
        worker = env.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    // --------------------------------------------------------- store utilities

    /**
     * Stands in for an operator editing the config store while loans are live.
     * With the store normalised this touches ONE document — which is the
     * maintenance argument for normalising, not just the browsing one.
     */
    private void editBindingPath(String slug, String callKey, String newPath) {
        String id = ConfigStore.id(slug, ConfigSeeder.BINDING_VERSION, callKey);
        ObjectNode doc = (ObjectNode) store.read(ConfigStore.BINDINGS, id);
        doc.put("path", newPath);
        store.write(ConfigStore.BINDINGS, id, doc);
    }

    /** Publishes a second binding version (copy every call document) and flips the pointer. */
    private void publishBindingVersion(String slug, String version) {
        for (JsonNode doc : store.readAll(ConfigStore.BINDINGS,
                ConfigStore.id(slug, ConfigSeeder.BINDING_VERSION))) {
            ObjectNode copy = ((ObjectNode) doc).deepCopy();
            copy.put("bindingVersion", version);
            store.write(ConfigStore.BINDINGS,
                    ConfigStore.id(slug, version, copy.get("callKey").asText()), copy);
        }
        ObjectNode pointer = (ObjectNode) store.read(ConfigStore.POINTERS, slug);
        pointer.put("bindingVersion", version);
        store.write(ConfigStore.POINTERS, slug, pointer);
    }

    // ------------------------------------------------------------ fake activity

    /**
     * Scripted stand-in for the lender's HTTP endpoint. Config resolution is NOT
     * faked: loadFlowSkeleton, loadStageKnobs and the per-attempt binding lookup
     * all go through the real store, which is what makes the config tests mean
     * something.
     */
    class FakeStageActivity implements StageActivity {
        final Map<String, String> scripted = new LinkedHashMap<>();
        final AtomicInteger loanStatusCalls = new AtomicInteger();
        final AtomicInteger sanctionCalls = new AtomicInteger();
        int loanStatusTerminalAfter = 3;
        int sanctionTerminalAfter = 3;
        String panStatusCode = "E";

        /** callName -> how many attempts fail with a RETRYABLE technical failure. */
        final Map<String, Integer> failAttempts = new LinkedHashMap<>();
        /** callName -> attempts actually served, so tests can assert retry counts. */
        final Map<String, AtomicInteger> attempts = new LinkedHashMap<>();
        /** Lets a test flip SHAPE (mandatory / onBranchFailure / minSuccess). */
        Consumer<FlowSkeleton> configTweak = s -> { };
        /** (callName, attemptNumber) hook — a test uses it to edit the store mid-retry. */
        BiConsumer<String, Integer> onAttempt = (n, a) -> { };

        @Override
        public FlowSkeleton loadFlowSkeleton(String lenderSlug) {
            FlowSkeleton s = resolver.skeleton(lenderSlug);
            configTweak.accept(s);
            return s;
        }

        @Override
        public StageKnobs loadStageKnobs(String lenderSlug, String stageName) {
            return resolver.knobs(lenderSlug, stageName);
        }

        int attemptsFor(String callName) {
            AtomicInteger a = attempts.get(callName);
            return a == null ? 0 : a.get();
        }

        @Override
        public CallResult execute(CallRequest req) {
            String name = req.callName == null ? req.stepName : req.callName;
            int attempt = attempts.computeIfAbsent(name, k -> new AtomicInteger())
                    .incrementAndGet();
            onAttempt.accept(name, attempt);

            // THE POINT: resolved per attempt, from the store as it is right now.
            ResolvedBinding rb = resolver.binding(req.lenderSlug, req.callKey);

            CallResult r = new CallResult();
            r.callName = name;
            r.httpStatus = 200;
            r.bindingVersion = rb.bindingVersion;
            r.transportVersion = rb.transportVersion;

            // A binding pointing at a path the lender does not serve. Retryable,
            // exactly like a real 404/500 would be.
            if (rb.binding.path != null && rb.binding.path.startsWith("/broken")) {
                throw ApplicationFailure.newFailure(
                        name + " HTTP 404 for " + rb.binding.path,
                        StageActivityImpl.TECHNICAL_FAILURE);
            }

            Integer failFirst = failAttempts.get(name);
            if (failFirst != null && (failFirst < 0 || attempt <= failFirst)) {
                throw ApplicationFailure.newFailure(name + " upstream 500",
                        StageActivityImpl.TECHNICAL_FAILURE);
            }

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

    private FakeStageActivity register() {
        FakeStageActivity acts = new FakeStageActivity();
        worker.registerActivitiesImplementations(acts, new DynamicStageActivityImpl(acts));
        return acts;
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

    private static LenderFlowWorkflow.FlowInput input(String loanRequestId) {
        return new LenderFlowWorkflow.FlowInput("federal", loanRequestId, seed());
    }

    // ------------------------------------------------------- baseline behaviour

    @Test
    void happyPath_allThreeStages_withTimeSkippedPollsAndEsignSignal() {
        FakeStageActivity acts = register();
        env.start();

        LenderFlowWorkflow wf = stub("LR-HAPPY-1");
        WorkflowClient.start(wf::run, input("LR-HAPPY-1"));
        wf.pledgeCardReady(Map.of("esignCompletedAt", "test"));

        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-HAPPY-1"));

        assertEquals("COMPLETED", result.outcome);
        assertEquals("UTR123456789", result.finalContext.get("utr"));
        assertEquals("LA-77001", result.finalContext.get("loanAccountNumber"));
        assertTrue(acts.attemptsFor("nameDobCheck") >= 1);
    }

    @Test
    void businessRejection_isNotRetried_andSurfacesAsRejected() {
        FakeStageActivity acts = register();
        acts.panStatusCode = "F";           // fake PAN -> lender says no
        env.start();

        LenderFlowWorkflow wf = stub("LR-REJECT-1");
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-REJECT-1"));

        assertEquals("REJECTED", result.outcome);
        assertTrue(result.failedAt.startsWith("PRECHECK/parallelChecks"));
        assertEquals("SUCCESS", result.finalContext.get("parallelChecks.unNameCheck.outcome"));
        assertEquals("STOP", result.finalContext.get("parallelChecks.decision"));
        assertEquals("panValidation", result.finalContext.get("parallelChecks.rejectedCalls"));
    }

    // -------------------------------------------------- parallel join semantics

    @Test
    void parallelBranchFailure_keepsSuccessfulSiblingResults_thenStopsAtTheGate() {
        FakeStageActivity acts = register();
        acts.failAttempts.put("panValidation", -1);   // never recovers
        env.start();

        LenderFlowWorkflow wf = stub("LR-PARTIAL-1");
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-PARTIAL-1"));

        assertEquals("FAILED", result.outcome);
        assertEquals("PRECHECK/parallelChecks", result.failedAt);

        assertEquals("SUCCESS", result.finalContext.get("unNameCheckStatus"));
        assertEquals("SUCCESS", result.finalContext.get("panAadhaarLinkStatus"));
        assertEquals("FAILED", result.finalContext.get("parallelChecks.panValidation.outcome"));
        assertEquals("3", result.finalContext.get("parallelChecks.successCount"));
        assertEquals("STOP", result.finalContext.get("parallelChecks.decision"));

        assertTrue(acts.attemptsFor("mobileAadhaarLink") >= 1);
        // Stage-default retry, now sourced from the transport document's knobs.
        assertEquals(3, acts.attemptsFor("panValidation"));
    }

    @Test
    void retryableBranchFailure_isRetriedPerConfig_thenFlowGoesForward() {
        FakeStageActivity acts = register();
        acts.failAttempts.put("unNameCheck", 2);     // 2 x 500, succeeds on attempt 3
        env.start();

        LenderFlowWorkflow wf = stub("LR-RETRY-1");
        WorkflowClient.start(wf::run, input("LR-RETRY-1"));
        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-RETRY-1"));

        assertEquals("COMPLETED", result.outcome);
        assertEquals(3, acts.attemptsFor("unNameCheck"));
        assertEquals("PROCEED", result.finalContext.get("parallelChecks.decision"));
    }

    @Test
    void optionalBranchFailure_isTolerated_andFlowProceedsDegraded() {
        FakeStageActivity acts = register();
        acts.failAttempts.put("mobileAadhaarLink", -1);
        env.start();

        LenderFlowWorkflow wf = stub("LR-DEGRADED-1");
        WorkflowClient.start(wf::run, input("LR-DEGRADED-1"));
        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-DEGRADED-1"));

        assertEquals("COMPLETED", result.outcome);
        assertEquals("PROCEED_DEGRADED", result.finalContext.get("parallelChecks.decision"));
        assertEquals("mobileAadhaarLink", result.finalContext.get("parallelChecks.failedCalls"));
        // Per-call retry override survived the migration into transport.callOverrides.
        assertEquals(4, acts.attemptsFor("mobileAadhaarLink"));
    }

    @Test
    void mandatoryBranchFailure_isTolerated_whenGateSaysCONTINUE() {
        FakeStageActivity acts = register();
        acts.failAttempts.put("panAadhaarLink", -1);
        acts.configTweak = s -> s.stages.get(0).steps.stream()
                .filter(st -> "parallelChecks".equals(st.name))
                .forEach(st -> st.onBranchFailure = "CONTINUE");
        env.start();

        LenderFlowWorkflow wf = stub("LR-CONTINUE-1");
        WorkflowClient.start(wf::run, input("LR-CONTINUE-1"));
        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-CONTINUE-1"));

        assertEquals("COMPLETED", result.outcome);
        assertEquals("PROCEED_DEGRADED", result.finalContext.get("parallelChecks.decision"));
    }

    @Test
    void quorum_stopsTheFlow_whenTooFewBranchesSucceed() {
        FakeStageActivity acts = register();
        acts.failAttempts.put("mobileAadhaarLink", -1);
        acts.configTweak = s -> s.stages.get(0).steps.stream()
                .filter(st -> "parallelChecks".equals(st.name))
                .forEach(st -> st.minSuccess = 4);
        env.start();

        LenderFlowWorkflow wf = stub("LR-QUORUM-1");
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-QUORUM-1"));

        assertEquals("FAILED", result.outcome);
        assertEquals("STOP", result.finalContext.get("parallelChecks.decision"));
        assertTrue(result.reason.contains("minSuccess=4"));
    }

    // ------------------------------------------------------- conditional steps

    @Test
    void customerCreation_isSkipped_whenDedupeFlagIsY() {
        FakeStageActivity acts = register();
        acts.scripted.put("dedupeFlag", "Y");
        env.start();

        LenderFlowWorkflow wf = stub("LR-SKIP-1");
        WorkflowClient.start(wf::run, input("LR-SKIP-1"));
        wf.pledgeCardReady(Map.of());
        wf.run(input("LR-SKIP-1"));

        LenderFlowWorkflow.FlowStatus s = wf.status();
        assertTrue(s.history.stream().anyMatch(
                h -> "customerCreation".equals(h.step) && "SKIPPED".equals(h.status)));
    }

    @Test
    void aadhaarChecks_areSkipped_whenDedupeSaysNotNeeded() {
        FakeStageActivity acts = register();
        acts.scripted.put("aadhaarChecksNeeded", "false");
        env.start();

        LenderFlowWorkflow wf = stub("LR-AADHAAR-1");
        WorkflowClient.start(wf::run, input("LR-AADHAAR-1"));
        wf.pledgeCardReady(Map.of());
        wf.run(input("LR-AADHAAR-1"));

        LenderFlowWorkflow.FlowStatus s = wf.status();
        assertTrue(s.history.stream().anyMatch(
                h -> h.step.endsWith("panAadhaarLink") && "SKIPPED".equals(h.status)));
    }

    @Test
    void loanStatusPoll_exhausts_thenParksForManualRetrySignal() {
        FakeStageActivity acts = register();
        acts.loanStatusTerminalAfter = 45;   // never terminal within maxAttempts=30
        env.start();

        LenderFlowWorkflow wf = stub("LR-EXHAUST-1");
        WorkflowClient.start(wf::run, input("LR-EXHAUST-1"));

        env.sleep(Duration.ofHours(2));

        LenderFlowWorkflow.FlowStatus parked = wf.status();
        assertEquals("retryLoanStatus", parked.waitingOnSignal);

        wf.retryLoanStatus();                // ops clicks retry
        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-EXHAUST-1"));

        assertEquals("COMPLETED", result.outcome);
    }

    @Test
    void secondLender_runsWithZeroCodeChange() {
        FakeStageActivity acts = new FakeStageActivity();
        Worker demoWorker = env.newWorker("lih-demobank-work");
        demoWorker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
        demoWorker.registerActivitiesImplementations(acts, new DynamicStageActivityImpl(acts));
        worker.registerActivitiesImplementations(acts, new DynamicStageActivityImpl(acts));
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

    // ==================================================================
    //  Hot config — the behaviour this redesign exists to make possible
    // ==================================================================

    /**
     * THE HEADLINE. A binding points at a path the lender does not serve. The
     * activity fails and enters its retry backoff. Config is corrected WHILE the
     * loan is in that backoff, and the next attempt picks it up and succeeds.
     *
     * No worker restart, no redeploy, no signal, no Temporal Reset. The only
     * thing that changed is a document in the store.
     *
     * (The edit is triggered from the attempt hook purely so the test can
     * interleave deterministically; in production the writer is an operator.)
     */
    @Test
    void bindingEditedMidRetry_healsTheLoan_withNoRedeploy() {
        FakeStageActivity acts = register();
        editBindingPath(SLUG, "STP.customerInfo", "/broken/customer-info");
        acts.onAttempt = (name, attempt) -> {
            if ("customerInfo".equals(name) && attempt == 2) {
                editBindingPath(SLUG, "STP.customerInfo", "/stp/customer-info");
            }
        };
        env.start();

        LenderFlowWorkflow wf = stub("LR-HEAL-1");
        WorkflowClient.start(wf::run, input("LR-HEAL-1"));
        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-HEAL-1"));

        assertEquals("COMPLETED", result.outcome);
        // Attempt 1 failed on the broken path, attempt 2 resolved the fixed one.
        assertEquals(2, acts.attemptsFor("customerInfo"));
    }

    /**
     * When the retry ladder runs out before anyone fixes anything, the step PARKS
     * instead of failing the loan. The operator then has unlimited time: fix the
     * binding, signal retryStep, the step re-runs against fresh config.
     */
    @Test
    void stepParks_whenRetriesExhaust_thenRetryStepResumesWithFixedConfig() {
        FakeStageActivity acts = register();
        editBindingPath(SLUG, "STP.customerInfo", "/broken/customer-info");
        env.start();

        LenderFlowWorkflow wf = stub("LR-PARK-1");
        WorkflowClient.start(wf::run, input("LR-PARK-1"));

        // Stage-default retry is 3 attempts with 2s/4s backoff — skipped instantly.
        env.sleep(Duration.ofMinutes(5));

        LenderFlowWorkflow.FlowStatus parked = wf.status();
        assertEquals("STP/customerInfo", parked.parkedAt);
        assertEquals("retryStep", parked.waitingOnSignal);
        assertTrue(parked.history.stream().anyMatch(
                h -> "AWAITING_INTERVENTION".equals(h.status)));

        // The operator fixes the document and releases the step.
        editBindingPath(SLUG, "STP.customerInfo", "/stp/customer-info");
        wf.retryStep();
        wf.pledgeCardReady(Map.of());

        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-PARK-1"));
        assertEquals("COMPLETED", result.outcome);
        assertEquals(4, acts.attemptsFor("customerInfo"));   // 3 failed + 1 after the fix
    }

    /** A lender decline is an answer, not a fault. It must never park. */
    @Test
    void businessReject_neverParks() {
        FakeStageActivity acts = register();
        acts.panStatusCode = "F";
        env.start();

        LenderFlowWorkflow wf = stub("LR-NOPARK-1");
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-NOPARK-1"));

        assertEquals("REJECTED", result.outcome);
        assertNull(wf.status().parkedAt);
    }

    /**
     * REPLAY SAFETY — the claim, tested rather than asserted.
     *
     * The binding is changed mid-flight (a different version is published while
     * the loan is parked on the e-sign wait). The execution then completes, and
     * its recorded history is replayed against the same workflow class. A clean
     * replay proves the config change never entered the workflow's determinism
     * surface — which is exactly why resolving bindings inside the activity,
     * rather than handing them in from the workflow, is the safe design.
     */
    @Test
    void replayIsClean_afterTheBindingChangedMidFlight() throws Exception {
        register();
        env.start();

        LenderFlowWorkflow wf = stub("LR-REPLAY-1");
        WorkflowClient.start(wf::run, input("LR-REPLAY-1"));

        // Let the loan reach the e-sign wait, then publish new config under it.
        env.sleep(Duration.ofMinutes(5));
        publishBindingVersion(SLUG, "b2");

        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-REPLAY-1"));
        assertEquals("COMPLETED", result.outcome);

        // Calls made after the flip recorded the new version; the ones before did not.
        assertEquals("b2", wf.status().lastBindingVersion);

        WorkflowExecutionHistory history = client.fetchHistory("lender-flow-LR-REPLAY-1");
        WorkflowReplayer.replayWorkflowExecution(history, LenderFlowWorkflowImpl.class);
    }

    /**
     * The other half of "future, not past": a SHAPE change is pinned. Publishing
     * a new skeleton while a loan is in flight must not add, remove or reorder
     * that loan's steps — while a loan started afterwards does pick it up.
     */
    @Test
    void skeletonChange_doesNotLeakIntoARunningExecution() {
        FakeStageActivity acts = register();
        env.start();

        LenderFlowWorkflow wf = stub("LR-SHAPE-1");
        WorkflowClient.start(wf::run, input("LR-SHAPE-1"));
        env.sleep(Duration.ofMinutes(5));      // parked on eSignWait, past PRECHECK

        // Publish a v2 flow: copy every stage document, add one step to PRECHECK.
        ObjectNode flow = (ObjectNode) store.read(ConfigStore.FLOWS,
                ConfigStore.id(SLUG, ConfigSeeder.SKELETON_VERSION));
        flow.put("version", "v2");
        store.write(ConfigStore.FLOWS, ConfigStore.id(SLUG, "v2"), flow);

        for (JsonNode doc : store.readAll(ConfigStore.STAGES,
                ConfigStore.id(SLUG, ConfigSeeder.SKELETON_VERSION))) {
            ObjectNode stage = ((ObjectNode) doc).deepCopy();
            stage.put("flowVersion", "v2");
            if ("PRECHECK".equals(stage.get("name").asText())) {
                ObjectNode extra = stage.objectNode();
                extra.put("name", "extraCheck");
                extra.put("mode", "SYNC");
                extra.put("onFailure", "PARK");
                ((ArrayNode) stage.get("steps")).add(extra);
            }
            store.write(ConfigStore.STAGES,
                    ConfigStore.id(SLUG, "v2", stage.get("name").asText()), stage);
        }

        // The new step needs a binding — one more document, nothing else changes.
        ObjectNode binding = ((ObjectNode) store.read(ConfigStore.BINDINGS,
                ConfigStore.id(SLUG, ConfigSeeder.BINDING_VERSION, "PRECHECK.nameDobCheck")))
                .deepCopy();
        binding.put("step", "extraCheck");
        binding.put("callKey", "PRECHECK.extraCheck");
        store.write(ConfigStore.BINDINGS,
                ConfigStore.id(SLUG, ConfigSeeder.BINDING_VERSION, "PRECHECK.extraCheck"), binding);

        ObjectNode pointer = (ObjectNode) store.read(ConfigStore.POINTERS, SLUG);
        pointer.put("skeletonVersion", "v2");
        store.write(ConfigStore.POINTERS, SLUG, pointer);

        wf.pledgeCardReady(Map.of());
        LenderFlowWorkflow.FlowResult result = wf.run(input("LR-SHAPE-1"));

        assertEquals("COMPLETED", result.outcome);
        // The running loan kept the shape it started with.
        assertEquals(ConfigSeeder.SKELETON_VERSION, wf.status().skeletonVersion);
        assertEquals(0, acts.attemptsFor("extraCheck"));

        // A loan started now gets the new shape — same worker, same binary.
        LenderFlowWorkflow next = stub("LR-SHAPE-2");
        WorkflowClient.start(next::run, input("LR-SHAPE-2"));
        next.pledgeCardReady(Map.of());
        assertEquals("COMPLETED", next.run(input("LR-SHAPE-2")).outcome);

        assertEquals("v2", next.status().skeletonVersion);
        assertNotEquals(0, acts.attemptsFor("extraCheck"));
    }

    // ==================================================================
    //  History fixtures for ReplayCompatibilityTest
    //
    //  Disabled by default. Regenerate ONLY when a history-shape change is
    //  intended and versioned:
    //    mvn test -Dregen.histories=true -Dtest=LenderFlowWorkflowTest#captureHistoryFixture
    //
    //  Six scenarios chosen because each produces a STRUCTURALLY different
    //  command sequence — between them they cover all four step modes, both
    //  park paths, conditional skips, and a second lender's skeleton.
    // ==================================================================

    @ParameterizedTest(name = "capture {0}")
    @ValueSource(strings = {"happy", "business-reject", "conditional-skips",
            "poll-exhaustion-park", "step-park-retrystep", "second-lender"})
    @EnabledIfSystemProperty(named = "regen.histories", matches = "true")
    void captureHistoryFixture(String scenario) {
        String loanId = "LR-FIX-" + scenario;
        String workflowId = "lender-flow-" + loanId;

        switch (scenario) {
            // SYNC + PARALLEL + POLL + AWAIT_SIGNAL, every step taken.
            case "happy": {
                register();
                env.start();
                LenderFlowWorkflow wf = stub(loanId);
                WorkflowClient.start(wf::run, input(loanId));
                wf.pledgeCardReady(Map.of("esignCompletedAt", "fixture"));
                wf.run(input(loanId));
                break;
            }
            // Non-retryable reject inside the parallel gate — the short history.
            case "business-reject": {
                FakeStageActivity acts = register();
                acts.panStatusCode = "F";
                env.start();
                stub(loanId).run(input(loanId));
                break;
            }
            // Conditions false -> fewer scheduled activities, which shifts every
            // downstream positional activityId. The shape most likely to drift.
            case "conditional-skips": {
                FakeStageActivity acts = register();
                acts.scripted.put("dedupeFlag", "Y");
                acts.scripted.put("aadhaarChecksNeeded", "false");
                env.start();
                LenderFlowWorkflow wf = stub(loanId);
                WorkflowClient.start(wf::run, input(loanId));
                wf.pledgeCardReady(Map.of());
                wf.run(input(loanId));
                break;
            }
            // 30 poll cycles (timer + activity each), then park on a signal.
            case "poll-exhaustion-park": {
                FakeStageActivity acts = register();
                acts.loanStatusTerminalAfter = 45;
                env.start();
                LenderFlowWorkflow wf = stub(loanId);
                WorkflowClient.start(wf::run, input(loanId));
                env.sleep(Duration.ofHours(2));
                wf.retryLoanStatus();
                wf.pledgeCardReady(Map.of());
                wf.run(input(loanId));
                break;
            }
            // Retries exhaust -> PARK -> retryStep -> knobs re-read mid-step.
            case "step-park-retrystep": {
                register();
                editBindingPath(SLUG, "STP.customerInfo", "/broken/customer-info");
                env.start();
                LenderFlowWorkflow wf = stub(loanId);
                WorkflowClient.start(wf::run, input(loanId));
                env.sleep(Duration.ofMinutes(5));
                editBindingPath(SLUG, "STP.customerInfo", "/stp/customer-info");
                wf.retryStep();
                wf.pledgeCardReady(Map.of());
                wf.run(input(loanId));
                break;
            }
            // A different skeleton entirely, same workflow class.
            case "second-lender": {
                FakeStageActivity acts = new FakeStageActivity();
                Worker demoWorker = env.newWorker("lih-demobank-work");
                demoWorker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
                demoWorker.registerActivitiesImplementations(
                        acts, new DynamicStageActivityImpl(acts));
                env.start();
                client.newWorkflowStub(LenderFlowWorkflow.class,
                                WorkflowOptions.newBuilder()
                                        .setTaskQueue("lih-demobank-work")
                                        .setWorkflowId(workflowId)
                                        .build())
                        .run(new LenderFlowWorkflow.FlowInput("demobank", loanId, seed()));
                break;
            }
            default:
                throw new IllegalArgumentException("unmapped scenario: " + scenario);
        }

        HistoryFixtures.capture(client, workflowId, scenario);
    }
}
