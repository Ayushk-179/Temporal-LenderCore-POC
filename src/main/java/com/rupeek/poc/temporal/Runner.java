package com.rupeek.poc.temporal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rupeek.poc.temporal.activity.DynamicStageActivityImpl;
import com.rupeek.poc.temporal.activity.StageActivityImpl;
import com.rupeek.poc.temporal.config.seed.ConfigSeeder;
import com.rupeek.poc.temporal.config.store.ConfigResolver;
import com.rupeek.poc.temporal.config.store.ConfigStore;
import com.rupeek.poc.temporal.config.store.ConfigStores;
import com.rupeek.poc.temporal.config.model.CallBinding;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.Pointer;
import com.rupeek.poc.temporal.config.model.ResolvedBinding;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflow;
import com.rupeek.poc.temporal.workflow.LenderFlowWorkflowImpl;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.api.workflowservice.v1.DeleteWorkflowExecutionRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Demo driver.
 *
 *   seed                               split lender-config.json into the config store
 *   show    <slug>                     print what the store currently serves
 *   worker  <slug>                     start a worker on the lender's own task queue
 *   start   <slug> <loanRequestId>     start a flow
 *   signal  <loanRequestId> <name>     pledgeCardReady | retryLoanStatus
 *                                      | retryFundTransferStatus | retryStep
 *   status  <loanRequestId>            query live progress + config versions in use
 *   patch   <loanRequestId> k=v ...    write values into a RUNNING loan's context
 *   reset   [loanRequestId]            put everything back to scenario zero
 *
 * Store selection is environmental, so the seeder and the worker can never
 * disagree:  CONFIG_STORE=file|mongo   CONFIG_DIR=./config-store
 *            MONGO_URI=mongodb://localhost:27017   MONGO_DB=lender_config
 */
public final class Runner {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Task queue PER LENDER — this is the per-lender isolation, with no extra infra. */
    static String taskQueue(String slug) {
        return "lih-" + slug + "-work";
    }

    static String workflowId(String loanRequestId) {
        return "lender-flow-" + loanRequestId;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: seed|show|worker|start|signal|status ...");
            System.exit(2);
        }

        // seed and show do not need a Temporal server at all.
        switch (args[0]) {
            case "seed":
                seed();
                return;
            case "show":
                show(args[1]);
                return;
            default:
                break;
        }

        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        switch (args[0]) {
            case "worker":
                worker(client, args[1]);
                break;
            case "start":
                start(client, args[1], args[2]);
                break;
            case "signal":
                signal(client, args[1], args[2]);
                break;
            case "status":
                status(client, args[1]);
                break;
            case "patch":
                patch(client, args[1], java.util.Arrays.copyOfRange(args, 2, args.length));
                break;
            case "reset":
                reset(client, args.length > 1 ? args[1] : null);
                break;
            default:
                System.err.println("unknown command: " + args[0]);
                System.exit(2);
        }

        // The worker is the only command that should keep running: factory.start()
        // returns immediately and the worker lives on its own threads.
        //
        // Every other command is one-shot, and without this it appears to HANG.
        // WorkflowServiceStubs leaves non-daemon gRPC threads behind, and
        // exec:java runs in Maven's JVM and waits for them — so `reset` would
        // print its output and then sit there until the terminal was killed.
        if (!"worker".equals(args[0])) {
            System.exit(0);
        }
    }

    /**
     * One-time migration from the classpath blob to the external store. After
     * this, lender-config.json is only a seed source — the running system reads
     * the store, which is why config edits no longer need a build.
     */
    private static void seed() {
        ConfigStore store = ConfigStores.fromEnv();
        ConfigSeeder.Report r = ConfigSeeder.seedAll(store);
        System.out.println("seeded into " + store.describe());
        System.out.println("  lenders      " + r.lenders);
        System.out.println("  stages       " + r.stages + " documents (one per stage)");
        System.out.println("  bindings     " + r.bindings + " documents (one per call)");
        System.out.println("  stage_knobs  " + r.stageKnobs + " documents (one per stage)");
        System.out.println("  + flows, transports, pointers, lenders");
    }

    /**
     * Prints what the store serves RIGHT NOW, stage by stage — the ground truth a
     * demo needs, and the reason the data is normalised per stage rather than
     * held as one blob.
     */
    private static void show(String slug) {
        ConfigStore store = ConfigStores.fromEnv();
        ConfigResolver resolver = new ConfigResolver(store);

        Pointer p = resolver.pointer(slug);
        FlowSkeleton flow = resolver.skeleton(slug);

        System.out.println("store    : " + store.describe());
        System.out.println("lender   : " + slug + "  —  " + flow.displayName);
        System.out.println("pointer  : skeleton=" + p.skeletonVersion
                + "  bindings=" + p.bindingVersion + "  transport=" + p.transportVersion);

        for (FlowSkeleton.Stage stage : flow.stages) {
            StageKnobs k = resolver.knobs(slug, stage.name);
            System.out.println();
            System.out.printf("%s   [knobs: %d attempts, %ds initial, x%.1f backoff, %ds timeout]%n",
                    stage.name, k.retry.maximumAttempts, k.retry.initialIntervalSeconds,
                    k.retry.backoffCoefficient, k.timeoutSeconds);

            for (FlowSkeleton.Step step : stage.steps) {
                String gate = "PARALLEL".equalsIgnoreCase(step.mode)
                        ? "onBranchFailure=" + step.onBranchFailure + " minSuccess=" + step.minSuccess
                        : "POLL".equalsIgnoreCase(step.mode)
                            ? "every " + step.pollIntervalSeconds + "s x" + step.maxAttempts
                                + " then " + step.onExhausted
                            : "AWAIT_SIGNAL".equalsIgnoreCase(step.mode)
                                ? "signal " + step.signalName + " (" + step.timeoutHours + "h)"
                                : "onFailure=" + step.onFailure;
                System.out.printf("  %-22s %-13s %s%n", step.name, step.mode, gate);

                if ("AWAIT_SIGNAL".equalsIgnoreCase(step.mode)) {
                    continue;   // a human wait, not an HTTP call — no binding by design
                }
                if (step.branches.isEmpty()) {
                    printCall(resolver, slug, stage.name + "." + step.name, "    ");
                } else {
                    for (FlowSkeleton.Branch b : step.branches) {
                        System.out.printf("    %-20s %s%n", b.name,
                                b.mandatory ? "mandatory" : "optional");
                        printCall(resolver, slug,
                                stage.name + "." + step.name + "." + b.name, "      ");
                    }
                }
            }
        }
        System.out.println();
        System.out.println("binding documents: " + resolver.callKeys(slug, p.bindingVersion).size()
                + " — edit any one of them while a worker runs; no restart, no redeploy");
    }

    private static void printCall(ConfigResolver resolver, String slug, String callKey,
                                  String indent) {
        try {
            ResolvedBinding rb = resolver.binding(slug, callKey);
            CallBinding c = rb.binding;
            System.out.println(indent + c.method + " " + rb.transport.baseUrl + c.path
                    + "   -> extract " + c.extract.keySet());
        } catch (ConfigResolver.ConfigMissingException e) {
            System.out.println(indent + "(no binding document for " + callKey + ")");
        }
    }

    private static void worker(WorkflowClient client, String slug) {
        ConfigStore store = ConfigStores.fromEnv();
        ConfigResolver resolver = new ConfigResolver(store);
        StageActivityImpl activities = new StageActivityImpl(resolver);

        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker(taskQueue(slug));
        worker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
        // Typed impl serves the two config activities; the dynamic dispatcher
        // serves every config-named call type (PRECHECK.nameDobCheck, ...).
        worker.registerActivitiesImplementations(
                activities, new DynamicStageActivityImpl(activities));
        System.out.println("worker up on " + taskQueue(slug)
                + " — config from " + store.describe());
        System.out.println("config is read per stage and per activity attempt; "
                + "edit it while this runs, no restart needed");
        factory.start();
    }

    private static void start(WorkflowClient client, String slug, String loanRequestId) {
        Map<String, String> seed = new LinkedHashMap<>();
        seed.put("customerName", "TEST CUSTOMER");
        seed.put("pan", "AAAPZ1234C");
        seed.put("mobile", "9000000000");
        seed.put("dob", "1990-01-15");
        seed.put("aadhaarRef", "XXXX-XXXX-1234");

        LenderFlowWorkflow wf = client.newWorkflowStub(
                LenderFlowWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(taskQueue(slug))
                        // Idempotency. A duplicate start from Rupeekweb is rejected
                        // here — this replaces exec_lock in Redis entirely.
                        .setWorkflowId(workflowId(loanRequestId))
                        .setWorkflowIdReusePolicy(
                                WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
                        .build());

        WorkflowClient.start(wf::run,
                new LenderFlowWorkflow.FlowInput(slug, loanRequestId, seed));
        System.out.println("started " + workflowId(loanRequestId)
                + " on " + taskQueue(slug));
    }

    private static void signal(WorkflowClient client, String loanRequestId, String name)
            throws Exception {
        WorkflowStub stub = client.newUntypedWorkflowStub(workflowId(loanRequestId));
        if ("pledgeCardReady".equals(name)) {
            Map<String, String> payload = new LinkedHashMap<>();
            payload.put("esignCompletedAt", "demo");
            stub.signal(name, payload);
        } else {
            stub.signal(name);
        }
        System.out.println("signalled " + name);
    }

    /**
     * Write values into a running loan's context: `patch LR-1001 branchCode=BLR-042`.
     *
     * The operator escape hatch for the one case config cannot reach — a value
     * nobody captured that the lender now demands. Typical sequence:
     *   1. the step parks on an unresolved ${ctx.x}
     *   2. patch x in
     *   3. signal retryStep — the re-run renders it
     */
    private static void patch(WorkflowClient client, String loanRequestId, String[] pairs) {
        if (pairs.length == 0) {
            System.err.println("usage: patch <loanRequestId> key=value [key=value ...]");
            System.exit(2);
        }
        Map<String, String> patch = new LinkedHashMap<>();
        for (String pair : pairs) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                System.err.println("bad pair '" + pair + "' — expected key=value");
                System.exit(2);
            }
            patch.put(pair.substring(0, eq), pair.substring(eq + 1));
        }
        client.newUntypedWorkflowStub(workflowId(loanRequestId)).signal("patchContext", patch);
        // A signal is fire-and-forget: the workflow may refuse identity keys
        // (loanRequestId, lenderSlug, _requestId), so do not claim success here.
        System.out.println("sent patch " + patch.keySet() + " to " + workflowId(loanRequestId));
        System.out.println("CONFIRM it landed:  status " + loanRequestId
                + "   (look for CONTEXT_PATCHED, or CONTEXT_PATCH_REFUSED for identity keys)");
        System.out.println("then release the step:  signal " + loanRequestId + " retryStep");
    }

    private static void status(WorkflowClient client, String loanRequestId) throws Exception {
        WorkflowStub stub = client.newUntypedWorkflowStub(workflowId(loanRequestId));
        LenderFlowWorkflow.FlowStatus s =
                stub.query("status", LenderFlowWorkflow.FlowStatus.class);
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(s));
    }

    /**
     * Put the whole POC back to scenario zero, in one command.
     *
     * Three stores drift during a demo and all three have to move together, or
     * the next scenario silently inherits the last one's state:
     *   1. the CONFIG STORE   — a scenario edits bindings/knobs
     *   2. the MOCK           — a scenario flips panStatusCode, dedupeFlag, ...
     *   3. TEMPORAL           — workflow IDs are single-use, so a finished loan
     *                           blocks its own ID from being started again
     *
     * Deleting (not just terminating) the executions is what frees the workflow
     * ID, so every scenario below can reuse the same loan id and stay copy-paste.
     */
    private static void reset(WorkflowClient client, String loanRequestId) {
        ConfigStore store = ConfigStores.fromEnv();
        store.clear();   // drop anything a scenario published, not just overwrite
        ConfigSeeder.Report r = ConfigSeeder.seedAll(store);
        System.out.println("1/3 config  wiped and re-seeded into " + store.describe()
                + " (" + r.stages + " stages, " + r.bindings + " bindings)");

        System.out.println("2/3 mock    " + resetMock(store));

        List<String> ids = new ArrayList<>();
        if (loanRequestId != null) {
            ids.add(workflowId(loanRequestId));
        } else {
            try {
                client.listExecutions("WorkflowId STARTS_WITH 'lender-flow-'")
                        .forEach(e -> ids.add(e.getExecution().getWorkflowId()));
            } catch (Exception e) {
                System.out.println("3/3 temporal  cannot list executions: " + e.getMessage());
                return;
            }
        }

        int deleted = 0;
        for (String id : ids) {
            // Terminate first: a running execution has to stop before it can go.
            try {
                client.newUntypedWorkflowStub(id).terminate("reset");
            } catch (Exception ignored) {
                // already closed, or never existed
            }
            try {
                client.getWorkflowServiceStubs().blockingStub().deleteWorkflowExecution(
                        DeleteWorkflowExecutionRequest.newBuilder()
                                .setNamespace(client.getOptions().getNamespace())
                                .setWorkflowExecution(
                                        WorkflowExecution.newBuilder().setWorkflowId(id).build())
                                .build());
                deleted++;
            } catch (Exception e) {
                System.out.println("      could not delete " + id + ": " + e.getMessage());
            }
        }
        System.out.println("3/3 temporal deleted " + deleted + " execution(s) "
                + (loanRequestId == null ? "(all lender-flow-*)" : "(" + workflowId(loanRequestId) + ")"));
        System.out.println("ready — workflow IDs are free to reuse");
    }

    /** Mock base URL comes from the seeded transport document, so it cannot drift. */
    private static String resetMock(ConfigStore store) {
        String baseUrl = "http://localhost:9099";
        try {
            com.fasterxml.jackson.databind.JsonNode t = store.read(ConfigStore.TRANSPORTS,
                    ConfigStore.id("federal", ConfigSeeder.TRANSPORT_VERSION));
            if (t != null && t.hasNonNull("baseUrl")) {
                baseUrl = t.get("baseUrl").asText();
            }
        } catch (Exception ignored) {
            // fall back to the default
        }
        try {
            HttpResponse<String> resp = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(3)).build()
                    .send(HttpRequest.newBuilder(URI.create(baseUrl + "/_reset"))
                            .timeout(Duration.ofSeconds(3)).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
            return "scenario knobs restored via " + baseUrl + "/_reset (HTTP "
                    + resp.statusCode() + ")";
        } catch (Exception e) {
            return "NOT reachable at " + baseUrl + " — start mock/mock_lender.py "
                    + "(skipping; config and Temporal were still reset)";
        }
    }

    private Runner() {
    }
}
