package com.rupeek.poc.temporal.activity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rupeek.poc.temporal.config.model.CallBinding;
import com.rupeek.poc.temporal.config.model.FlowSkeleton;
import com.rupeek.poc.temporal.config.model.Predicate;
import com.rupeek.poc.temporal.config.model.Predicates;
import com.rupeek.poc.temporal.config.model.ResolvedBinding;
import com.rupeek.poc.temporal.config.model.StageKnobs;
import com.rupeek.poc.temporal.config.model.Transport;
import com.rupeek.poc.temporal.config.store.ConfigResolver;
import com.rupeek.poc.temporal.config.store.ConfigStores;
import com.rupeek.poc.temporal.config.template.JsonPaths;
import com.rupeek.poc.temporal.config.template.TemplateRenderer;
import com.rupeek.poc.temporal.config.template.UnresolvedTemplateException;
import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The ONLY place lender HTTP happens. Note what is NOT here: no per-lender class,
 * no if/else on slug. One executor, driven entirely by a CallBinding.
 *
 * The important change from the blob version: the binding is NOT handed in by the
 * workflow any more — it is fetched from the store at the top of every attempt.
 * A retry that starts 60 seconds after the previous one resolves config as it is
 * NOW, so editing a payload template while a loan sits in backoff repairs it with
 * no redeploy, no signal and no reset.
 */
public class StageActivityImpl implements StageActivity {

    /** Failure type wired into RetryOptions.setDoNotRetry(...) — a rejection is not a fault. */
    public static final String BUSINESS_REJECT = "BUSINESS_REJECT";
    public static final String TECHNICAL_FAILURE = "TECHNICAL_FAILURE";
    /**
     * Bad config or an unresolvable template. Deliberately RETRYABLE: the retry
     * ladder is the window in which an operator can fix the document and have the
     * next attempt succeed on its own. If the ladder runs out, the step parks.
     */
    public static final String CONFIG_ERROR = "CONFIG_ERROR";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(StageActivityImpl.class);

    /**
     * POC default: log full request and response bodies. Those bodies carry PAN,
     * Aadhaar reference and mobile — set LOG_RAW=false to suppress them, and treat
     * raw-body logging as something that must NOT ship (see CONFIG-DESIGN.md §7.16).
     *
     * An activity is ordinary code, not workflow code, so a plain slf4j logger is
     * correct here — activities do not replay.
     */
    private static final boolean LOG_RAW =
            !"false".equalsIgnoreCase(String.valueOf(envOrProp("LOG_RAW", "true")));
    private static final int RAW_CAP = 2000;

    private final ConfigResolver config;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public StageActivityImpl() {
        this(new ConfigResolver(ConfigStores.fromEnv()));
    }

    public StageActivityImpl(ConfigResolver config) {
        this.config = config;
    }

    @Override
    public FlowSkeleton loadFlowSkeleton(String lenderSlug) {
        try {
            FlowSkeleton skeleton = config.skeleton(lenderSlug);
            log.info("CONFIG loadFlowSkeleton lender={} version={} stages={}",
                    lenderSlug, skeleton.version, skeleton.stages.size());
            return skeleton;
        } catch (ConfigResolver.ConfigMissingException e) {
            throw ApplicationFailure.newNonRetryableFailure(e.getMessage(), CONFIG_ERROR);
        } catch (Exception e) {
            throw ApplicationFailure.newFailure(
                    "skeleton load failed: " + e.getMessage(), CONFIG_ERROR);
        }
    }

    @Override
    public StageKnobs loadStageKnobs(String lenderSlug, String stageName) {
        try {
            StageKnobs knobs = config.knobs(lenderSlug, stageName);
            log.info("CONFIG loadStageKnobs lender={} stage={} transport={} "
                            + "retry={}x timeout={}s overrides={}",
                    lenderSlug, stageName, knobs.transportVersion,
                    knobs.retry.maximumAttempts, knobs.timeoutSeconds,
                    knobs.callOverrides.size());
            return knobs;
        } catch (ConfigResolver.ConfigMissingException e) {
            throw ApplicationFailure.newNonRetryableFailure(e.getMessage(), CONFIG_ERROR);
        } catch (Exception e) {
            throw ApplicationFailure.newFailure(
                    "knobs load failed: " + e.getMessage(), CONFIG_ERROR);
        }
    }

    @Override
    public CallResult execute(CallRequest req) {
        // For SYNC/POLL the callName IS the step name; don't print it twice.
        String label = req.stageName + "/" + req.stepName
                + (req.callName == null || req.callName.equals(req.stepName)
                        ? "" : "/" + req.callName);
        int attempt = currentAttempt();

        // ---- resolution point: EVERY attempt reads the store as it is right now
        ResolvedBinding resolved;
        try {
            resolved = config.binding(req.lenderSlug, req.callKey);
        } catch (ConfigResolver.ConfigMissingException e) {
            log.error("[{}] attempt {} CONFIG MISSING for callKey={} — {}",
                    label, attempt, req.callKey, e.getMessage());
            throw ApplicationFailure.newFailure(label + " " + e.getMessage(), CONFIG_ERROR);
        }
        log.info("[{}] attempt {} RESOLVED callKey={} binding={} transport={} "
                        + "(re-read from the store on THIS attempt)",
                label, attempt, req.callKey, resolved.bindingVersion, resolved.transportVersion);

        CallBinding call = resolved.binding;
        Transport transport = resolved.transport;

        String url;
        String body;
        HttpRequest.Builder b;
        try {
            url = buildUrl(transport, call, req.context);
            body = renderBody(call, req.context);

            b = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(call.timeoutSeconds != null
                            ? call.timeoutSeconds
                            : transport.defaultTimeoutSeconds));

            transport.staticHeaders.forEach(b::header);
            for (Map.Entry<String, String> h : call.headers.entrySet()) {
                b.header(h.getKey(), TemplateRenderer.renderString(h.getValue(), req.context, true));
            }
        } catch (UnresolvedTemplateException e) {
            // Retryable on purpose — see CONFIG_ERROR above. The retry ladder is
            // the window in which an operator can fix the template.
            log.error("[{}] attempt {} TEMPLATE ERROR [binding {}] {} | ctx keys available: {}",
                    label, attempt, resolved.bindingVersion, e.getMessage(), req.context.keySet());
            throw ApplicationFailure.newFailure(
                    label + " [binding " + resolved.bindingVersion + "] " + e.getMessage(),
                    CONFIG_ERROR);
        }

        log.info("[{}] attempt {} --> {} {}", label, attempt, call.method, url);
        if (LOG_RAW) {
            log.info("[{}] --> headers {}", label, headersFor(transport, call, req.context));
            log.info("[{}] --> body {}", label, cap(body));
        }

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
            log.error("[{}] attempt {} TRANSPORT ERROR to {} — {}",
                    label, attempt, url, e.getMessage());
            throw ApplicationFailure.newFailure(
                    label + " transport error to " + url + ": " + e.getMessage(),
                    TECHNICAL_FAILURE);
        }

        log.info("[{}] attempt {} <-- HTTP {}", label, attempt, resp.statusCode());
        if (LOG_RAW) {
            log.info("[{}] <-- body {}", label, cap(resp.body()));
        }

        JsonNode json = parse(resp.body(), label);

        if (matches("businessRejectWhen", call.businessRejectWhen, json, req.context, label)) {
            // Non-retryable: the lender said no. No retry and no config edit can
            // change the answer, so this must never park either.
            log.warn("[{}] attempt {} BUSINESS REJECT — lender declined; no retry, no park",
                    label, attempt);
            throw ApplicationFailure.newNonRetryableFailure(
                    label + " rejected by lender", BUSINESS_REJECT, resp.body());
        }

        if (resp.statusCode() >= 500) {
            log.warn("[{}] attempt {} HTTP {} — retryable technical failure",
                    label, attempt, resp.statusCode());
            throw ApplicationFailure.newFailure(
                    label + " HTTP " + resp.statusCode(), TECHNICAL_FAILURE);
        }

        CallResult out = new CallResult();
        out.callName = req.callName == null ? req.stepName : req.callName;
        out.httpStatus = resp.statusCode();
        out.rawResponseSnippet = snippet(resp.body());
        out.extracted = extract(call.extract, json, label);
        out.terminal = matches("terminalWhen", call.terminalWhen, json, req.context, label);
        out.bindingVersion = resolved.bindingVersion;
        out.transportVersion = resolved.transportVersion;
        log.info("[{}] attempt {} EXTRACTED {} (terminal={})",
                label, attempt, out.extracted, out.terminal);

        if (call.successWhen != null
                && !matches("successWhen", call.successWhen, json, req.context, label)) {
            log.warn("[{}] attempt {} SUCCESS CHECK FAILED — treating as retryable",
                    label, attempt);
            throw ApplicationFailure.newFailure(
                    label + " successWhen not satisfied (HTTP " + resp.statusCode()
                            + ", binding " + resolved.bindingVersion + "): " + snippet(resp.body()),
                    TECHNICAL_FAILURE);
        }

        return out;
    }

    // ------------------------------------------------------------ internals

    /** The path is templated too, so "/loan/${ctx.loanId}/status" is expressible. */
    private String buildUrl(Transport transport, CallBinding call, Map<String, String> ctx) {
        StringBuilder sb = new StringBuilder(transport.baseUrl)
                .append(TemplateRenderer.renderString(call.path, ctx, true));
        if (!call.queryParams.isEmpty()) {
            sb.append('?');
            boolean first = true;
            for (Map.Entry<String, String> e : call.queryParams.entrySet()) {
                if (!first) {
                    sb.append('&');
                }
                sb.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8))
                        .append('=')
                        .append(URLEncoder.encode(
                                TemplateRenderer.renderString(e.getValue(), ctx, true),
                                StandardCharsets.UTF_8));
                first = false;
            }
        }
        return sb.toString();
    }

    private String renderBody(CallBinding call, Map<String, String> ctx) {
        if (call.request == null || call.request.isNull()) {
            return "";
        }
        try {
            return MAPPER.writeValueAsString(TemplateRenderer.render(call.request, ctx, true));
        } catch (UnresolvedTemplateException e) {
            throw e;
        } catch (Exception e) {
            throw ApplicationFailure.newFailure("body render failed: " + e.getMessage(),
                    CONFIG_ERROR);
        }
    }

    private JsonNode parse(String body, String label) {
        try {
            return MAPPER.readTree(body == null || body.isBlank() ? "{}" : body);
        } catch (Exception e) {
            throw ApplicationFailure.newFailure(
                    label + " unparseable response: " + snippet(body), TECHNICAL_FAILURE);
        }
    }

    private Map<String, String> extract(Map<String, String> spec, JsonNode json, String label) {
        Map<String, String> out = new LinkedHashMap<>();
        spec.forEach((key, path) -> {
            String v;
            try {
                v = JsonPaths.extract(json, path);
            } catch (IllegalArgumentException e) {
                throw ApplicationFailure.newFailure(label + " " + e.getMessage(), CONFIG_ERROR);
            }
            if (v != null) {
                out.put(key, v);
            }
        });
        return out;
    }

    /**
     * Every binding predicate is logged with the value it actually saw. "Why did
     * this call fail successWhen" is the single most common config question, and
     * it should be answerable from the log without re-running anything.
     */
    private boolean matches(String name, Predicate p, JsonNode json,
                            Map<String, String> ctx, String label) {
        if (p == null) {
            return false;
        }
        String source = p.path != null ? p.path : "ctx." + p.ctx;
        String actual = p.path != null ? JsonPaths.extract(json, p.path) : ctx.get(p.ctx);
        String op = p.op == null ? "EQUALS" : p.op.toUpperCase();
        boolean result;
        try {
            result = Predicates.evaluate(p, actual);
        } catch (IllegalArgumentException e) {
            log.error("[{}] {} has an unknown op '{}'", label, name, p.op);
            throw ApplicationFailure.newNonRetryableFailure(
                    label + " " + name + ": " + e.getMessage(), CONFIG_ERROR);
        }
        log.info("[{}]   {}: {} {} {} | actual={} -> {}", label, name, source, op,
                "BLANK".equals(op) || "NOT_BLANK".equals(op)
                        ? "" : (p.values != null && !p.values.isEmpty() ? p.values : p.value),
                actual, result);
        return result;
    }

    /** Rendered headers, for the request log. Static + per-call, same as sent. */
    private Map<String, String> headersFor(Transport transport, CallBinding call,
                                           Map<String, String> ctx) {
        Map<String, String> all = new LinkedHashMap<>(transport.staticHeaders);
        call.headers.forEach((k, v) -> all.put(k, TemplateRenderer.renderString(v, ctx, false)));
        return all;
    }

    private static String cap(String s) {
        if (s == null || s.isEmpty()) {
            return "(empty)";
        }
        return s.length() <= RAW_CAP ? s : s.substring(0, RAW_CAP) + "...[truncated]";
    }

    /** Attempt number for the log; 1 when called outside an activity context. */
    private static int currentAttempt() {
        try {
            return Activity.getExecutionContext().getInfo().getAttempt();
        } catch (Exception e) {
            return 1;
        }
    }

    private static String envOrProp(String key, String fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            v = System.getProperty(key);
        }
        return v == null || v.isBlank() ? fallback : v;
    }

    private static String snippet(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
