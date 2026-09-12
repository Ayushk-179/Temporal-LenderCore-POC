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
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

import java.util.LinkedHashMap;
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
            default:
                System.err.println("unknown command: " + args[0]);
                System.exit(2);
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

    private static void status(WorkflowClient client, String loanRequestId) throws Exception {
        WorkflowStub stub = client.newUntypedWorkflowStub(workflowId(loanRequestId));
        LenderFlowWorkflow.FlowStatus s =
                stub.query("status", LenderFlowWorkflow.FlowStatus.class);
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(s));
    }

    private Runner() {
    }
}
