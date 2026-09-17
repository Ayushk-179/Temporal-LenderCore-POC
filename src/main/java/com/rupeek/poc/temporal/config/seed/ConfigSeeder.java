package com.rupeek.poc.temporal.config.seed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rupeek.poc.temporal.config.LenderConfig;
import com.rupeek.poc.temporal.config.model.CallBinding;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.Pointer;
import com.rupeek.poc.temporal.config.model.Predicate;
import com.rupeek.poc.temporal.config.model.Retry;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import com.rupeek.poc.temporal.config.model.Transport;
import com.rupeek.poc.temporal.config.store.ConfigStore;
import com.rupeek.poc.temporal.config.store.MongoConfigStore;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Normalises the legacy single-blob lender-config.json into the store.
 *
 * This is the migration, and it is also the argument. Every field lands in
 * exactly one collection, and the classification is mechanical:
 *
 *   does it change WHAT the workflow does next?   -> stages   (pinned)
 *   is it an ActivityOption the workflow sets?    -> stage_knobs (per stage)
 *   is it part of the HTTP contract?              -> bindings (per attempt, hot)
 *
 * Nothing in the existing config resisted classification. That is itself the
 * finding: the blob was already two planes wearing one hat.
 *
 * After seeding, lender-config.json is only a seed source and offline fallback.
 */
public final class ConfigSeeder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String SKELETON_VERSION = "v1";
    public static final String BINDING_VERSION = "b1";
    public static final String TRANSPORT_VERSION = "t1";

    /** Counts written per collection, so `seed` can report what it did. */
    public static class Report {
        public final List<String> lenders = new ArrayList<>();
        public int stages;
        public int bindings;
        public int stageKnobs;

        @Override
        public String toString() {
            return lenders + " — " + stages + " stage docs, " + bindings
                    + " binding docs, " + stageKnobs + " stage_knobs docs";
        }
    }

    public static Report seedAll(ConfigStore store) {
        return seedAll(store, loadLegacy());
    }

    public static Report seedAll(ConfigStore store, LenderConfig legacy) {
        if (store instanceof MongoConfigStore) {
            ((MongoConfigStore) store).ensureIndexes();
        }
        Report report = new Report();
        for (Map.Entry<String, LenderConfig.Lender> e : legacy.lenders.entrySet()) {
            seed(store, e.getKey(), e.getValue(), report);
            report.lenders.add(e.getKey());
        }
        return report;
    }

    public static LenderConfig loadLegacy() {
        try (InputStream in = ConfigSeeder.class.getClassLoader()
                .getResourceAsStream("lender-config.json")) {
            if (in == null) {
                throw new IllegalStateException("lender-config.json not found on classpath");
            }
            return MAPPER.readValue(in, LenderConfig.class);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot read seed config: " + e.getMessage(), e);
        }
    }

    public static void seed(ConfigStore store, String slug, LenderConfig.Lender lender,
                            Report report) {
        // ---------------------------------------------------------- lender
        ObjectNode lenderDoc = MAPPER.createObjectNode();
        lenderDoc.put("slug", slug);
        lenderDoc.put("displayName", lender.displayName);
        store.write(ConfigStore.LENDERS, slug, lenderDoc);

        // ------------------------------------------------------- transport
        Transport transport = new Transport();
        transport.baseUrl = lender.transport.baseUrl;
        transport.defaultTimeoutSeconds = lender.transport.defaultTimeoutSeconds;
        transport.staticHeaders.putAll(lender.transport.staticHeaders);

        ObjectNode transportDoc = MAPPER.createObjectNode();
        transportDoc.put("lenderSlug", slug);
        transportDoc.put("version", TRANSPORT_VERSION);
        transportDoc.setAll((ObjectNode) MAPPER.valueToTree(transport));
        store.write(ConfigStore.TRANSPORTS,
                ConfigStore.id(slug, TRANSPORT_VERSION), transportDoc);

        // ------------------------------------ flow: stage ORDER, one place only
        ArrayNode stageNames = MAPPER.createArrayNode();
        lender.stages.forEach(s -> stageNames.add(s.name));

        ObjectNode flowDoc = MAPPER.createObjectNode();
        flowDoc.put("lenderSlug", slug);
        flowDoc.put("version", SKELETON_VERSION);
        flowDoc.put("displayName", lender.displayName);
        flowDoc.set("stages", stageNames);
        store.write(ConfigStore.FLOWS, ConfigStore.id(slug, SKELETON_VERSION), flowDoc);

        // -------------------------------------------------- per-stage documents
        int seq = 0;
        for (LenderConfig.Stage ls : lender.stages) {
            FlowSkeleton.Stage stage = new FlowSkeleton.Stage();
            stage.name = ls.name;

            StageKnobs knobs = new StageKnobs();
            knobs.lenderSlug = slug;
            knobs.transportVersion = TRANSPORT_VERSION;
            knobs.stage = ls.name;
            // Materialised, not inherited — see StageKnobs javadoc.
            knobs.retry = retry(lender.defaultRetry);
            knobs.timeoutSeconds = lender.transport.defaultTimeoutSeconds;

            for (LenderConfig.Step lstep : ls.steps) {
                FlowSkeleton.Step step = new FlowSkeleton.Step();
                step.name = lstep.name;
                step.mode = lstep.mode;
                step.condition = predicate(lstep.condition);
                step.onBranchFailure = lstep.onBranchFailure;
                step.minSuccess = lstep.minSuccess;
                step.pollIntervalSeconds = lstep.pollIntervalSeconds;
                step.maxAttempts = lstep.maxAttempts;
                step.onExhausted = lstep.onExhausted;
                step.exhaustedSignalName = lstep.exhaustedSignalName;
                step.signalName = lstep.signalName;
                step.timeoutHours = lstep.timeoutHours;

                // Every mode that can suffer a TECHNICAL failure parks rather than
                // killing the loan: SYNC, POLL and PARALLEL. A parked PARALLEL step
                // re-runs only the branches that failed, so branches that already
                // succeeded are never re-fired.
                //
                // AWAIT_SIGNAL is excluded because it has no technical failure mode
                // — it is already a wait, with its own timeout.
                //
                // A BUSINESS_REJECT never parks whatever this says: a lender decline
                // is an answer, and no config edit turns it into a yes.
                step.onFailure = "AWAIT_SIGNAL".equalsIgnoreCase(lstep.mode) ? "FAIL" : "PARK";

                String stepKey = ls.name + "." + lstep.name;

                if ("PARALLEL".equalsIgnoreCase(lstep.mode)) {
                    for (LenderConfig.Call call : lstep.calls) {
                        FlowSkeleton.Branch branch = new FlowSkeleton.Branch();
                        branch.name = call.name;
                        branch.condition = predicate(call.condition);
                        branch.mandatory = call.mandatory;
                        step.branches.add(branch);

                        String callKey = stepKey + "." + call.name;
                        writeBinding(store, slug, ls.name, lstep.name, call.name, callKey, call);
                        report.bindings++;
                        addOverride(knobs, callKey, call);
                    }
                } else if (lstep.call != null) {
                    writeBinding(store, slug, ls.name, lstep.name, null, stepKey, lstep.call);
                    report.bindings++;
                    addOverride(knobs, stepKey, lstep.call);
                }
                stage.steps.add(step);
            }

            ObjectNode stageDoc = MAPPER.createObjectNode();
            stageDoc.put("lenderSlug", slug);
            stageDoc.put("flowVersion", SKELETON_VERSION);
            stageDoc.put("name", ls.name);
            stageDoc.put("seq", seq++);
            stageDoc.set("steps", MAPPER.valueToTree(stage.steps));
            store.write(ConfigStore.STAGES,
                    ConfigStore.id(slug, SKELETON_VERSION, ls.name), stageDoc);
            report.stages++;

            store.write(ConfigStore.STAGE_KNOBS,
                    ConfigStore.id(slug, TRANSPORT_VERSION, ls.name),
                    MAPPER.valueToTree(knobs));
            report.stageKnobs++;
        }

        // ------------------------------------ pointer: the one mutable document
        Pointer pointer = new Pointer();
        pointer.skeletonVersion = SKELETON_VERSION;
        pointer.bindingVersion = BINDING_VERSION;
        pointer.transportVersion = TRANSPORT_VERSION;
        store.write(ConfigStore.POINTERS, slug, MAPPER.valueToTree(pointer));
    }

    // ---------------------------------------------------------------- mapping

    private static void writeBinding(ConfigStore store, String slug, String stage, String step,
                                     String branch, String callKey, LenderConfig.Call call) {
        CallBinding b = new CallBinding();
        b.method = call.method;
        b.path = call.path;
        b.queryParams.putAll(call.queryParams);
        b.headers.putAll(call.headers);
        b.extract.putAll(call.extract);
        b.successWhen = predicate(call.successWhen);
        b.terminalWhen = predicate(call.terminalWhen);
        b.businessRejectWhen = predicate(call.businessRejectWhen);
        b.timeoutSeconds = call.timeoutSeconds;

        // Flat Map<String,String> -> JSON object. The renderer accepts any tree,
        // so a lender needing nested payloads is a document edit, not a release.
        ObjectNode req = MAPPER.createObjectNode();
        call.request.forEach(req::put);
        b.request = req;

        // Index fields first so the document reads top-down in Compass and in a diff.
        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("lenderSlug", slug);
        doc.put("bindingVersion", BINDING_VERSION);
        doc.put("stage", stage);
        doc.put("step", step);
        if (branch != null) {
            doc.put("call", branch);
        }
        doc.put("callKey", callKey);
        doc.setAll((ObjectNode) MAPPER.valueToTree(b));

        store.write(ConfigStore.BINDINGS, ConfigStore.id(slug, BINDING_VERSION, callKey), doc);
    }

    /** Per-call retry/timeout are ActivityOptions, so they belong in the knobs plane. */
    private static void addOverride(StageKnobs knobs, String callKey, LenderConfig.Call call) {
        if (call.retry == null && call.timeoutSeconds == null) {
            return;
        }
        StageKnobs.CallOverride o = new StageKnobs.CallOverride();
        o.callKey = callKey;
        o.retry = call.retry == null ? null : retry(call.retry);
        o.timeoutSeconds = call.timeoutSeconds;
        knobs.callOverrides.add(o);
    }

    private static Retry retry(LenderConfig.Retry r) {
        Retry out = new Retry();
        if (r == null) {
            return out;
        }
        out.initialIntervalSeconds = r.initialIntervalSeconds;
        out.backoffCoefficient = r.backoffCoefficient;
        out.maximumAttempts = r.maximumAttempts;
        out.nonRetryableErrorTypes = new ArrayList<>(r.nonRetryableErrorTypes);
        return out;
    }

    private static Predicate predicate(LenderConfig.Predicate p) {
        if (p == null) {
            return null;
        }
        Predicate out = new Predicate();
        out.path = p.path;
        out.ctx = p.ctx;
        out.op = p.op;
        out.value = p.value;
        out.values = new ArrayList<>(p.values);
        return out;
    }

    private ConfigSeeder() {
    }
}
