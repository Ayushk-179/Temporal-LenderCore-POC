package com.rupeek.poc.temporal.activity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rupeek.poc.temporal.config.LenderConfig;
import io.temporal.failure.ApplicationFailure;

import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The ONLY place lender HTTP happens. Note what is NOT here: no per-lender
 * class, no if/else on slug. One executor, driven entirely by the Call config.
 */
public class StageActivityImpl implements StageActivity {

    /** Failure type wired into RetryOptions.setDoNotRetry(...) — a rejection is not a fault. */
    public static final String BUSINESS_REJECT = "BUSINESS_REJECT";
    public static final String TECHNICAL_FAILURE = "TECHNICAL_FAILURE";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Override
    public LenderConfig loadConfig() {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("lender-config.json")) {
            if (in == null) {
                throw new IllegalStateException("lender-config.json not found on classpath");
            }
            return MAPPER.readValue(in, LenderConfig.class);
        } catch (Exception e) {
            throw ApplicationFailure.newFailure("Config load failed: " + e.getMessage(),
                    TECHNICAL_FAILURE);
        }
    }

    @Override
    public CallResult execute(CallRequest req) {
        LenderConfig.Call call = req.call;
        String label = req.stageName + "/" + req.stepName
                + (req.callName == null ? "" : "/" + req.callName);

        String url = buildUrl(req);
        String body = renderBody(call.request, req.context);

        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(
                        call.timeoutSeconds != null
                                ? call.timeoutSeconds
                                : req.transport.defaultTimeoutSeconds));

        req.transport.staticHeaders.forEach(b::header);
        call.headers.forEach((k, v) -> b.header(k, resolve(v, req.context)));

        if ("GET".equalsIgnoreCase(call.method)) {
            b.GET();
        } else {
            b.method(call.method.toUpperCase(), HttpRequest.BodyPublishers.ofString(body));
        }

        HttpResponse<String> resp;
        try {
            resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Transient — let Temporal's retry policy handle it.
            throw ApplicationFailure.newFailure(
                    label + " transport error: " + e.getMessage(), TECHNICAL_FAILURE);
        }

        JsonNode json = parse(resp.body(), label);

        if (matches(call.businessRejectWhen, json, req.context)) {
            // Non-retryable: the lender said no. Retrying cannot change the answer.
            throw ApplicationFailure.newNonRetryableFailure(
                    label + " rejected by lender", BUSINESS_REJECT, resp.body());
        }

        if (resp.statusCode() >= 500) {
            throw ApplicationFailure.newFailure(
                    label + " HTTP " + resp.statusCode(), TECHNICAL_FAILURE);
        }

        CallResult out = new CallResult();
        out.callName = req.callName == null ? req.stepName : req.callName;
        out.httpStatus = resp.statusCode();
        out.rawResponseSnippet = snippet(resp.body());
        out.extracted = extract(call.extract, json);
        out.terminal = call.terminalWhen != null && matches(call.terminalWhen, json, req.context);

        if (call.successWhen != null && !matches(call.successWhen, json, req.context)) {
            throw ApplicationFailure.newFailure(
                    label + " successWhen not satisfied: " + snippet(resp.body()),
                    TECHNICAL_FAILURE);
        }

        return out;
    }

    // ------------------------------------------------------------ internals

    private String buildUrl(CallRequest req) {
        StringBuilder sb = new StringBuilder(req.transport.baseUrl).append(req.call.path);
        if (!req.call.queryParams.isEmpty()) {
            sb.append('?');
            boolean first = true;
            for (Map.Entry<String, String> e : req.call.queryParams.entrySet()) {
                if (!first) {
                    sb.append('&');
                }
                sb.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8))
                        .append('=')
                        .append(URLEncoder.encode(resolve(e.getValue(), req.context),
                                StandardCharsets.UTF_8));
                first = false;
            }
        }
        return sb.toString();
    }

    private String renderBody(Map<String, String> template, Map<String, String> ctx) {
        Map<String, String> out = new LinkedHashMap<>();
        template.forEach((k, v) -> out.put(k, resolve(v, ctx)));
        try {
            return MAPPER.writeValueAsString(out);
        } catch (Exception e) {
            throw ApplicationFailure.newFailure("body render failed", TECHNICAL_FAILURE);
        }
    }

    /** Resolves "${ctx.someKey}" against the context snapshot; passes literals through. */
    static String resolve(String template, Map<String, String> ctx) {
        if (template == null) {
            return null;
        }
        String out = template;
        int guard = 0;
        while (out.contains("${ctx.") && guard++ < 20) {
            int start = out.indexOf("${ctx.");
            int end = out.indexOf('}', start);
            if (end < 0) {
                break;
            }
            String key = out.substring(start + 6, end);
            String val = ctx.getOrDefault(key, "");
            out = out.substring(0, start) + val + out.substring(end + 1);
        }
        return out;
    }

    private JsonNode parse(String body, String label) {
        try {
            return MAPPER.readTree(body == null || body.isBlank() ? "{}" : body);
        } catch (Exception e) {
            throw ApplicationFailure.newFailure(
                    label + " unparseable response: " + snippet(body), TECHNICAL_FAILURE);
        }
    }

    private Map<String, String> extract(Map<String, String> spec, JsonNode json) {
        Map<String, String> out = new LinkedHashMap<>();
        spec.forEach((key, path) -> {
            String v = at(json, path);
            if (v != null) {
                out.put(key, v);
            }
        });
        return out;
    }

    /** "$.data.customer_id" -> node walk. No JSONPath dependency needed for the POC. */
    static String at(JsonNode root, String path) {
        if (root == null || path == null) {
            return null;
        }
        String p = path.startsWith("$.") ? path.substring(2) : path;
        JsonNode cur = root;
        for (String seg : p.split("\\.")) {
            if (cur == null) {
                return null;
            }
            cur = cur.get(seg);
        }
        if (cur == null || cur.isNull()) {
            return null;
        }
        return cur.isValueNode() ? cur.asText() : cur.toString();
    }

    private boolean matches(LenderConfig.Predicate p, JsonNode json, Map<String, String> ctx) {
        if (p == null) {
            return false;
        }
        String actual = p.path != null ? at(json, p.path) : ctx.get(p.ctx);
        return evaluate(p, actual);
    }

    /** Shared with the workflow side so condition semantics cannot drift. */
    public static boolean evaluate(LenderConfig.Predicate p, String actual) {
        String op = p.op == null ? "EQUALS" : p.op.toUpperCase();
        List<String> vals = p.values;
        switch (op) {
            case "EQUALS":
                return actual != null && actual.equals(p.value);
            case "NOT_EQUALS":
                return actual == null || !actual.equals(p.value);
            case "IN":
                return actual != null && vals.contains(actual);
            case "NOT_IN":
                return actual == null || !vals.contains(actual);
            case "BLANK":
                return actual == null || actual.isBlank();
            case "NOT_BLANK":
                return actual != null && !actual.isBlank();
            default:
                throw new IllegalArgumentException("Unknown predicate op: " + op);
        }
    }

    private static String snippet(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
