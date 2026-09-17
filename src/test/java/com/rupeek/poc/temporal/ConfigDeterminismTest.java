package com.rupeek.poc.temporal;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rupeek.poc.temporal.activity.DynamicStageActivityImpl;
import com.rupeek.poc.temporal.activity.StageActivity;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import com.rupeek.poc.temporal.config.seed.ConfigSeeder;
import com.rupeek.poc.temporal.config.store.ConfigResolver;
import com.rupeek.poc.temporal.config.store.ConfigStore;
import com.rupeek.poc.temporal.config.store.ConfigStores;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflow;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflowImpl;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.WorkflowReplayer;
import io.temporal.worker.NonDeterministicException;
import io.temporal.worker.Worker;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WHY CONFIG IS READ INSIDE AN ACTIVITY — demonstrated, not asserted.
 *
 * Both workflows in this file are driven by the same mutable pointer, and the
 * same config change is made under both. One breaks on replay and one does not,
 * and the single difference is WHERE the store is read:
 *
 *   DriftingWorkflowImpl    reads the store from WORKFLOW code
 *                           -> replay re-executes the read, sees the NEW config,
 *                              emits a different command sequence, blows up
 *
 *   LenderFlowWorkflowImpl  reads the store inside an ACTIVITY
 *                           -> replay never re-executes it; the recorded result
 *                              comes back out of history and the sequence holds
 *
 * That is the whole argument for the pointer design. A mutable pointer is safe
 * precisely because the workflow never dereferences it — the activity does, and
 * activity results are immutable history.
 */
class ConfigDeterminismTest {

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
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        DriftingWorkflowImpl.RESOLVER = null;
        env.close();
    }

    // ------------------------------------------------------------ the bad shape

    @WorkflowInterface
    public interface DriftingWorkflow {
        /** Returns how many steps it walked, so the drift is visible in the result. */
        @WorkflowMethod
        int run(String lenderSlug);
    }

    /**
     * DO NOT COPY THIS. It is the mistake the real design avoids.
     *
     * Reading the config store from workflow code looks harmless — it is "just a
     * lookup" — but a workflow method re-executes from the top on every replay,
     * so the lookup re-executes too. The moment the pointer moves, replay walks a
     * different skeleton and schedules a different number of activities.
     */
    public static class DriftingWorkflowImpl implements DriftingWorkflow {

        /** Static, so workflow code can reach the store. That is the smell. */
        static ConfigResolver RESOLVER;

        @Override
        public int run(String lenderSlug) {
            // ILLEGAL: I/O from workflow code. Re-executed on every replay.
            FlowSkeleton skeleton = RESOLVER.skeleton(lenderSlug);

            StageActivity activities = Workflow.newActivityStub(
                    StageActivity.class,
                    ActivityOptions.newBuilder()
                            .setStartToCloseTimeout(Duration.ofSeconds(20))
                            .build());

            int steps = 0;
            for (FlowSkeleton.Stage stage : skeleton.stages) {
                for (FlowSkeleton.Step ignored : stage.steps) {
                    // One activity per step: the command count IS the flow shape.
                    activities.loadStageKnobs(lenderSlug, stage.name);
                    steps++;
                }
            }
            return steps;
        }
    }

    /** Only the two config activities are needed; nothing here makes HTTP calls. */
    static class ConfigOnlyActivity implements StageActivity {
        private final ConfigResolver config;

        ConfigOnlyActivity(ConfigResolver config) {
            this.config = config;
        }

        @Override
        public FlowSkeleton loadFlowSkeleton(String lenderSlug) {
            return config.skeleton(lenderSlug);
        }

        @Override
        public StageKnobs loadStageKnobs(String lenderSlug, String stageName) {
            return config.knobs(lenderSlug, stageName);
        }

        @Override
        public CallResult execute(CallRequest request) {
            throw new UnsupportedOperationException("not used by these tests");
        }
    }

    /** Drops the last step of PRECHECK, in place, under the live pointer. */
    private int shrinkPrecheckInPlace() {
        String id = ConfigStore.id(SLUG, ConfigSeeder.SKELETON_VERSION, "PRECHECK");
        ObjectNode stage = (ObjectNode) store.read(ConfigStore.STAGES, id);
        ArrayNode steps = (ArrayNode) stage.get("steps");
        int before = steps.size();
        steps.remove(before - 1);
        store.write(ConfigStore.STAGES, id, stage);
        return before;
    }

    // ------------------------------------------------------------------- tests

    /**
     * The failure mode this design exists to prevent.
     *
     * A worker replaying this history after the pointer moved would not "use the
     * new config" — it would be unable to make progress at all. In production this
     * is the worst kind of outage: the workflow does not fail, it STALLS, retrying
     * the same workflow task forever while ops sees a loan that simply stops.
     */
    @Test
    void readingConfigFromWorkflowCode_breaksReplayWhenTheConfigChanges() throws Exception {
        DriftingWorkflowImpl.RESOLVER = resolver;
        worker.registerWorkflowImplementationTypes(DriftingWorkflowImpl.class);
        worker.registerActivitiesImplementations(new ConfigOnlyActivity(resolver));
        env.start();

        DriftingWorkflow wf = client.newWorkflowStub(DriftingWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(TASK_QUEUE)
                        .setWorkflowId("drift-1")
                        .build());
        int stepsWhenItRan = wf.run(SLUG);
        assertTrue(stepsWhenItRan > 0);

        WorkflowExecutionHistory history = client.fetchHistory("drift-1");

        // The operator edits config. Nothing about this is unusual or reckless.
        int before = shrinkPrecheckInPlace();
        assertEquals(stepsWhenItRan - 1, countSteps(), "the edit must actually change the shape");
        assertNotEquals(0, before);

        // Replay now walks a shorter skeleton and schedules one activity fewer.
        // The replayer surfaces this wrapped, so assert on the whole cause chain
        // rather than the outer type.
        Exception e = assertThrows(Exception.class,
                () -> WorkflowReplayer.replayWorkflowExecution(
                        history, DriftingWorkflowImpl.class));

        String chain = causeChain(e);
        assertTrue(chain.contains(NonDeterministicException.class.getSimpleName())
                        || chain.contains("does not match command type"),
                "expected a command-sequence mismatch, got: " + chain);
        // The real message names the exact divergence, e.g.
        //   Event 71 of type EVENT_TYPE_ACTIVITY_TASK_SCHEDULED does not match
        //   command type COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
        assertTrue(chain.contains("ACTIVITY_TASK_SCHEDULED"),
                "the mismatch should be about a scheduled activity, got: " + chain);
    }

    /**
     * The same edit, under the real workflow: replay is clean.
     *
     * Not because the skeleton is "pinned" in any special way — nothing is frozen
     * on disk — but because the workflow asked an ACTIVITY for it. Replay serves
     * that answer from history and never looks at the store again.
     */
    @Test
    void readingConfigThroughAnActivity_replaysCleanlyUnderTheSameChange() throws Exception {
        LenderFlowWorkflowTest.FakeStageActivity acts =
                new LenderFlowWorkflowTest.FakeStageActivity(resolver);
        worker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
        worker.registerActivitiesImplementations(acts, new DynamicStageActivityImpl(acts));
        env.start();

        LenderFlowWorkflow wf = client.newWorkflowStub(LenderFlowWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(TASK_QUEUE)
                        .setWorkflowId("lender-flow-LR-DRIFT-1")
                        .build());
        WorkflowClient.start(wf::run, new LenderFlowWorkflow.FlowInput(SLUG, "LR-DRIFT-1", seed()));
        wf.pledgeCardReady(Map.of());
        assertEquals("COMPLETED", wf.run(
                new LenderFlowWorkflow.FlowInput(SLUG, "LR-DRIFT-1", seed())).outcome);

        WorkflowExecutionHistory history = client.fetchHistory("lender-flow-LR-DRIFT-1");

        // Exactly the same edit that broke the drifting workflow.
        shrinkPrecheckInPlace();

        // No exception. This is the property the whole config design rests on.
        WorkflowReplayer.replayWorkflowExecution(history, LenderFlowWorkflowImpl.class);
    }

    /** Replay failures arrive wrapped; flatten the chain so assertions can read it. */
    private static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(cur).append(" | ");
            if (cur.getCause() == cur) {
                break;
            }
        }
        return sb.toString();
    }

    private int countSteps() {
        int n = 0;
        for (FlowSkeleton.Stage s : resolver.skeleton(SLUG).stages) {
            n += s.steps.size();
        }
        return n;
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
}
