package com.rupeek.poc.temporal;

import com.rupeek.poc.temporal.workflow.LenderFlowWorkflowImpl;
import io.temporal.testing.WorkflowReplayer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The determinism regression net. Replays committed histories (see
 * {@link HistoryFixtures}) against the CURRENT LenderFlowWorkflowImpl.
 *
 * No activities are registered on purpose: replay never executes them, it reads
 * their recorded results back out of history. That is the same mechanism that
 * makes hot config changes safe, so this test exercises it directly.
 *
 * A failure here is not a flaky test. It means a code change moved the command
 * sequence, and deploying it would stall every in-flight loan that replays on
 * the new worker — a hang with repeating WorkflowTaskFailed, not a clean error.
 * Fix the code, or gate the change behind Workflow.getVersion and recapture.
 */
class ReplayCompatibilityTest {

    static List<Path> fixtures() {
        return HistoryFixtures.committed();
    }

    @Test
    void fixturesAreCommitted() {
        assertFalse(HistoryFixtures.committed().isEmpty(),
                "No histories in " + HistoryFixtures.DIR + " — this test is the only thing "
                        + "standing between a control-flow edit and a fleet of stalled loans. "
                        + "Regenerate with -Dregen.histories=true");
    }

    @ParameterizedTest(name = "replays cleanly: {0}")
    @MethodSource("fixtures")
    void replaysCleanly(Path fixture) throws Exception {
        WorkflowReplayer.replayWorkflowExecution(
                HistoryFixtures.load(fixture), LenderFlowWorkflowImpl.class);
    }
}
