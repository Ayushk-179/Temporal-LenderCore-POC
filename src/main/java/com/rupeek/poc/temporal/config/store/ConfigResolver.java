package com.rupeek.poc.temporal.config.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rupeek.poc.temporal.config.model.CallBinding;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.Pointer;
import com.rupeek.poc.temporal.config.model.ResolvedBinding;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import com.rupeek.poc.temporal.config.model.Transport;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns normalised documents into the three things the runtime asks for, one per
 * resolution point:
 *
 *   skeleton(slug)          ONCE per execution   flows + stages  -> pinned in history
 *   knobs(slug, stage)      ONCE per stage       one stage_knobs document
 *   binding(slug, callKey)  EVERY attempt        one bindings + one transports document
 *
 * Every lookup starts at the pointer, so a version flip is picked up by the very
 * next call. No caching in the POC — see FileConfigStore for why.
 */
public class ConfigResolver {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConfigStore store;

    public ConfigResolver(ConfigStore store) {
        this.store = store;
    }

    public ConfigStore store() {
        return store;
    }

    public Pointer pointer(String lenderSlug) {
        JsonNode n = store.read(ConfigStore.POINTERS, lenderSlug);
        if (n == null) {
            throw new ConfigMissingException("No pointer document for lenderSlug=" + lenderSlug
                    + " in " + store.describe() + ". Known lenders: " + store.lenders()
                    + ". Did you run `Runner seed`?");
        }
        return convert(n, Pointer.class, ConfigStore.POINTERS, lenderSlug);
    }

    /**
     * The pinned plane, assembled from the flow document (which owns stage ORDER)
     * plus one document per stage (which owns its own step order).
     *
     * Stage order lives in exactly one place. That is the point of normalising:
     * reordering stages is an edit to one array, not a risky reshuffle of a blob.
     */
    public FlowSkeleton skeleton(String lenderSlug) {
        Pointer p = pointer(lenderSlug);
        String flowId = ConfigStore.id(lenderSlug, p.skeletonVersion);

        JsonNode flow = require(ConfigStore.FLOWS, flowId);
        FlowSkeleton skeleton = new FlowSkeleton();
        skeleton.lenderSlug = lenderSlug;
        skeleton.version = p.skeletonVersion;
        skeleton.displayName = text(flow, "displayName");

        // One query/listing, then ordered by the flow document's stage list.
        Map<String, JsonNode> byName = new LinkedHashMap<>();
        for (JsonNode s : store.readAll(ConfigStore.STAGES, flowId)) {
            byName.put(text(s, "name"), s);
        }

        JsonNode order = flow.get("stages");
        if (order == null || !order.isArray() || order.isEmpty()) {
            throw new ConfigMissingException("Flow document " + flowId
                    + " lists no stages; nothing to run");
        }
        for (JsonNode nameNode : order) {
            String name = nameNode.asText();
            JsonNode doc = byName.get(name);
            if (doc == null) {
                throw new ConfigMissingException("Flow " + flowId + " references stage '" + name
                        + "' but no such document exists in " + ConfigStore.STAGES
                        + ". Present: " + byName.keySet());
            }
            skeleton.stages.add(convert(doc, FlowSkeleton.Stage.class,
                    ConfigStore.STAGES, ConfigStore.id(flowId, name)));
        }
        return skeleton;
    }

    /** The per-stage operational slice — exactly one document. */
    public StageKnobs knobs(String lenderSlug, String stageName) {
        Pointer p = pointer(lenderSlug);
        String id = ConfigStore.id(lenderSlug, p.transportVersion, stageName);
        StageKnobs k = convert(require(ConfigStore.STAGE_KNOBS, id), StageKnobs.class,
                ConfigStore.STAGE_KNOBS, id);
        k.lenderSlug = lenderSlug;
        k.transportVersion = p.transportVersion;
        k.stage = stageName;
        return k;
    }

    /**
     * The hot plane — resolved INSIDE the activity on every attempt, which is
     * precisely why editing one binding document un-sticks a loan that is
     * mid-retry.
     */
    public ResolvedBinding binding(String lenderSlug, String callKey) {
        Pointer p = pointer(lenderSlug);

        String bindingId = ConfigStore.id(lenderSlug, p.bindingVersion, callKey);
        JsonNode doc = store.read(ConfigStore.BINDINGS, bindingId);
        if (doc == null) {
            throw new ConfigMissingException("No binding document " + ConfigStore.BINDINGS
                    + "/" + bindingId + " in " + store.describe()
                    + ". Known callKeys for this version: " + callKeys(lenderSlug, p.bindingVersion));
        }
        CallBinding binding = convert(doc, CallBinding.class, ConfigStore.BINDINGS, bindingId);

        String transportId = ConfigStore.id(lenderSlug, p.transportVersion);
        Transport transport = convert(require(ConfigStore.TRANSPORTS, transportId),
                Transport.class, ConfigStore.TRANSPORTS, transportId);

        return new ResolvedBinding(callKey, p.bindingVersion, p.transportVersion,
                binding, transport);
    }

    /** Every callKey defined for a binding version — used for diagnostics and `show`. */
    public List<String> callKeys(String lenderSlug, String bindingVersion) {
        List<JsonNode> docs = store.readAll(ConfigStore.BINDINGS,
                ConfigStore.id(lenderSlug, bindingVersion));
        java.util.List<String> out = new java.util.ArrayList<>();
        for (JsonNode d : docs) {
            out.add(text(d, "callKey"));
        }
        return out;
    }

    // ------------------------------------------------------------------ internals

    private JsonNode require(String collection, String id) {
        JsonNode n = store.read(collection, id);
        if (n == null) {
            throw new ConfigMissingException("Missing config document " + collection + "/" + id
                    + " in " + store.describe());
        }
        return n;
    }

    private <T> T convert(JsonNode n, Class<T> type, String collection, String id) {
        try {
            return MAPPER.treeToValue(n, type);
        } catch (Exception e) {
            throw new ConfigMissingException("Malformed config document "
                    + collection + "/" + id + ": " + e.getMessage());
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    /** Config problems are never fixed by retrying, so the activity maps this to non-retryable. */
    public static class ConfigMissingException extends RuntimeException {
        public ConfigMissingException(String message) {
            super(message);
        }
    }
}
