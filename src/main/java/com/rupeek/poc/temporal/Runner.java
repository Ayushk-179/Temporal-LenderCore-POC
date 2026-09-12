package com.rupeek.poc.temporal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rupeek.poc.temporal.activity.DynamicStageActivityImpl;
import com.rupeek.poc.temporal.activity.StageActivityImpl;
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
 *   worker  <slug>                     start a worker on the lender's own task queue
 *   start   <slug> <loanRequestId>     start a flow
 *   signal  <loanRequestId> <name>     pledgeCardReady | retryLoanStatus | retryFundTransferStatus
 *   status  <loanRequestId>            query live progress
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
            System.err.println("usage: worker|start|signal|status ...");
            System.exit(2);
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

    private static void worker(WorkflowClient client, String slug) {
        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker(taskQueue(slug));
        worker.registerWorkflowImplementationTypes(LenderFlowWorkflowImpl.class);
        // Typed impl serves loadConfig; the dynamic dispatcher serves every
        // config-named call type (PRECHECK.nameDobCheck, FUND_TRANSFER.disburse, ...).
        worker.registerActivitiesImplementations(
                new StageActivityImpl(), new DynamicStageActivityImpl());
        System.out.println("worker up on " + taskQueue(slug) + " — ctrl-C to kill it mid-flow");
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
