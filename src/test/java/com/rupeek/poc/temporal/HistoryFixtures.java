package com.rupeek.poc.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.common.WorkflowExecutionHistory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Committed event histories, used as a regression net for DETERMINISM.
 *
 * WHY THESE ARE FILES AND NOT GENERATED PER RUN — the whole point. A history
 * captured by the current code and replayed by the current code always passes;
 * it proves nothing about a change. These files are a frozen record of the
 * command sequence the workflow emitted on the day they were captured, so a
 * future edit that adds, removes, reorders or renames a command fails the build
 * instead of stalling live loans on NonDeterministicException.
 *
 * WHAT REPLAY COMPARES (verified against SDK 1.25.2 WorkflowStateMachines):
 *   every command   command type <-> event type
 *   activities      activityId + activityType. When you do not set an activityId
 *                   the SDK derives a DETERMINISTIC uuid per command, so it also
 *                   shifts the moment the command sequence changes.
 *   timers          timerId
 *   child workflows workflowId + workflowType
 * NOT compared: activity inputs, retry policy, timeouts, task queue, timer
 * durations, user metadata. So retry/timeout knob changes are free; control flow
 * is not.
 *
 * REGENERATE deliberately, never to "make the build green":
 *
 *   mvn test -Dregen.histories=true -Dtest=LenderFlowWorkflowTest#captureHistoryFixture
 *
 * A diff in these files during review means the workflow's history shape moved.
 * That is either a bug, or a change that needs Workflow.getVersion (or a drain)
 * before it reaches a worker serving in-flight loans.
 */
final class HistoryFixtures {

    /** Source tree, not target/ — these are committed and reviewed like code. */
    static final Path DIR = Path.of("src", "test", "resources", "histories");

    private HistoryFixtures() {
    }

    static void capture(WorkflowClient client, String workflowId, String fixture) {
        try {
            Files.createDirectories(DIR);
            Files.writeString(DIR.resolve(fixture + ".json"),
                    client.fetchHistory(workflowId).toJson(true), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<Path> committed() {
        if (!Files.isDirectory(DIR)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(DIR)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static WorkflowExecutionHistory load(Path fixture) {
        try {
            return WorkflowExecutionHistory.fromJson(
                    Files.readString(fixture, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
