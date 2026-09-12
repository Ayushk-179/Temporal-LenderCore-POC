package com.rupeek.poc.temporal.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * THE HOT PLANE — the wire contract for exactly one call.
 *
 * Resolved INSIDE the activity, on every attempt, from the live store. It never
 * crosses into the workflow, so nothing in this class can cause a
 * non-determinism error. That is what makes "change the payload of a running
 * loan without a redeploy" safe rather than reckless.
 *
 * Keyed by callKey = "<STAGE>.<step>" for SYNC/POLL steps, or
 * "<STAGE>.<step>.<branch>" for a member of a PARALLEL fan-out.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CallBinding {

    public String method = "POST";
    public String path;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> queryParams = new LinkedHashMap<>();
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> headers = new LinkedHashMap<>();

    /**
     * Nested JSON template, not a flat map. Any string leaf may carry
     * "${ctx.key}"; see TemplateRenderer for the typed / default / optional-key
     * forms. Null for GET.
     */
    public JsonNode request;

    /** contextKey -> path into the response, e.g. "$.data.customer_id". */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> extract = new LinkedHashMap<>();

    public Predicate successWhen;
    public Predicate terminalWhen;
    public Predicate businessRejectWhen;

    /** Per-call HTTP timeout. Truly hot — applies on the very next attempt. */
    public Integer timeoutSeconds;
}
