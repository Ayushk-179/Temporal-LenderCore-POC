package com.rupeek.poc.temporal.config.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Iterator;
import java.util.Map;

/**
 * Renders a nested JSON template against the workflow context.
 *
 * The predecessor could only do flat Map&lt;String,String&gt; with plain
 * "${ctx.key}" substitution, so every lender payload had to be flat strings.
 * This one walks an arbitrary JSON tree:
 *
 *   "pan":          "${ctx.pan}"                 string interpolation
 *   "amount":       "${num:ctx.loanAmount}"      typed leaf -> JSON number
 *   "active":       "${bool:ctx.isActive}"       typed leaf -> JSON boolean
 *   "items":        "${json:ctx.itemsJson}"      splice a stored JSON subtree
 *   "agent":        "${ctx.agentId ?: \"SELF\"}" default when absent/blank
 *   "name":         "${ctx.customerName | upper}" function pipeline
 *   "?middleName":  "${ctx.middleName}"          key dropped when unresolved
 *
 * Deliberately NOT an expression engine. There is no evaluation, no reflection
 * and no method invocation, because hot-editable config that can evaluate
 * arbitrary expressions is a remote-code-execution path into the worker pods.
 * The function list below is the entire vocabulary.
 */
public final class TemplateRenderer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonNodeFactory NF = JsonNodeFactory.instance;

    /**
     * @param strict when true, an unresolved reference with no default throws
     *               instead of rendering an empty string.
     */
    public static JsonNode render(JsonNode template, Map<String, String> ctx, boolean strict) {
        if (template == null || template.isNull()) {
            return NullNode.instance;
        }
        if (template.isObject()) {
            ObjectNode out = NF.objectNode();
            Iterator<Map.Entry<String, JsonNode>> it = template.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                boolean optional = key.startsWith("?");
                // An optional key never fails the render; it just disappears.
                JsonNode v = render(e.getValue(), ctx, strict && !optional);
                if (optional) {
                    if (isBlank(v)) {
                        continue;
                    }
                    key = key.substring(1);
                }
                out.set(key, v);
            }
            return out;
        }
        if (template.isArray()) {
            ArrayNode out = NF.arrayNode();
            for (JsonNode child : template) {
                out.add(render(child, ctx, strict));
            }
            return out;
        }
        if (template.isTextual()) {
            return renderLeaf(template.asText(), ctx, strict);
        }
        return template;
    }

    /** Single-string rendering, for URL paths, query values and header values. */
    public static String renderString(String template, Map<String, String> ctx, boolean strict) {
        if (template == null) {
            return null;
        }
        JsonNode n = renderLeaf(template, ctx, strict);
        return n.isNull() ? "" : n.asText();
    }

    // ---------------------------------------------------------------- leaves

    private static JsonNode renderLeaf(String s, Map<String, String> ctx, boolean strict) {
        // A leaf that is EXACTLY one expression may produce a non-string node.
        // Anything else is string interpolation and always produces a string.
        if (s.startsWith("${") && s.endsWith("}") && s.indexOf('}') == s.length() - 1) {
            return evaluate(s.substring(2, s.length() - 1), ctx, strict);
        }
        return TextNode.valueOf(interpolate(s, ctx, strict));
    }

    private static String interpolate(String s, Map<String, String> ctx, boolean strict) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (true) {
            int start = s.indexOf("${", i);
            if (start < 0) {
                out.append(s, i, s.length());
                return out.toString();
            }
            int end = s.indexOf('}', start);
            if (end < 0) {
                out.append(s, i, s.length());
                return out.toString();
            }
            out.append(s, i, start);
            JsonNode v = evaluate(s.substring(start + 2, end), ctx, strict);
            out.append(v.isNull() ? "" : v.asText());
            i = end + 1;
        }
    }

    // ------------------------------------------------------------ expressions

    private static JsonNode evaluate(String expression, Map<String, String> ctx, boolean strict) {
        String body = expression.trim();

        String fallback = null;
        int d = body.indexOf("?:");
        if (d >= 0) {
            fallback = body.substring(d + 2).trim();
            body = body.substring(0, d).trim();
        }

        String[] pipeline = body.split("\\|");
        String head = pipeline[0].trim();

        String type = null;
        int colon = head.indexOf(':');
        if (colon > 0 && !head.startsWith("ctx.")) {
            type = head.substring(0, colon).trim();
            head = head.substring(colon + 1).trim();
        }

        String value = lookup(head, ctx, true, expression);
        if (value == null && fallback != null) {
            value = lookup(fallback, ctx, false, expression);
        }
        if (value == null) {
            if (strict) {
                throw new UnresolvedTemplateException(
                        "unresolved template ${" + expression + "} — context has no value for '"
                                + head + "' and no ?: default was given");
            }
            return NullNode.instance;
        }

        for (int i = 1; i < pipeline.length; i++) {
            value = apply(pipeline[i].trim(), value, expression);
        }
        return coerce(type, value, expression);
    }

    /**
     * "ctx.key" reads context; a quoted "literal" is itself.
     *
     * In HEAD position a bare token is REJECTED. Without that rule a typo —
     * ${cts.pan} instead of ${ctx.pan} — would quietly resolve to the literal
     * string "cts.pan" and ship it to the lender in the payload. That is the same
     * class of silent-wrong-value bug as the old getOrDefault(key, ""), so it gets
     * the same treatment: fail loudly and name the fix.
     *
     * In FALLBACK position (after ?:) a bare token IS a literal, which is what
     * makes ${num:ctx.loanAmount ?: 150000} read naturally.
     */
    private static String lookup(String ref, Map<String, String> ctx,
                                 boolean headPosition, String expression) {
        if (ref.startsWith("ctx.")) {
            String v = ctx.get(ref.substring(4));
            return v == null || v.isBlank() ? null : v;
        }
        if (ref.length() >= 2 && ref.startsWith("\"") && ref.endsWith("\"")) {
            return ref.substring(1, ref.length() - 1);
        }
        if (headPosition) {
            throw new UnresolvedTemplateException("'" + ref + "' in ${" + expression
                    + "} is not a context reference. Use ${ctx." + ref
                    + "} to read context, or \"" + ref + "\" (quoted) for a literal.");
        }
        return ref.isBlank() ? null : ref;
    }

    private static String apply(String fn, String value, String expression) {
        String name = fn;
        String args = "";
        int paren = fn.indexOf('(');
        if (paren > 0 && fn.endsWith(")")) {
            name = fn.substring(0, paren).trim();
            args = fn.substring(paren + 1, fn.length() - 1).trim();
        }
        switch (name) {
            case "upper":
                return value.toUpperCase();
            case "lower":
                return value.toLowerCase();
            case "trim":
                return value.trim();
            case "substr": {
                String[] a = args.split(",");
                int from = Integer.parseInt(a[0].trim());
                int to = a.length > 1 ? Integer.parseInt(a[1].trim()) : value.length();
                from = Math.max(0, Math.min(from, value.length()));
                to = Math.max(from, Math.min(to, value.length()));
                return value.substring(from, to);
            }
            default:
                throw new UnresolvedTemplateException("unknown function '" + name + "' in ${"
                        + expression + "}. Supported: upper, lower, trim, substr(a,b)");
        }
    }

    private static JsonNode coerce(String type, String value, String expression) {
        if (type == null || "raw".equals(type) || "str".equals(type)) {
            return TextNode.valueOf(value);
        }
        try {
            switch (type) {
                case "num":
                    return value.matches("-?\\d+")
                            ? new LongNode(Long.parseLong(value))
                            : new DoubleNode(Double.parseDouble(value));
                case "bool":
                    return BooleanNode.valueOf(Boolean.parseBoolean(value));
                case "json":
                    return MAPPER.readTree(value);
                default:
                    throw new UnresolvedTemplateException("unknown type prefix '" + type
                            + "' in ${" + expression + "}. Supported: num, bool, json, raw");
            }
        } catch (UnresolvedTemplateException e) {
            throw e;
        } catch (Exception e) {
            throw new UnresolvedTemplateException("cannot coerce '" + value + "' to " + type
                    + " in ${" + expression + "}");
        }
    }

    private static boolean isBlank(JsonNode n) {
        return n == null || n.isNull() || n.isMissingNode()
                || (n.isTextual() && n.asText().isBlank());
    }

    private TemplateRenderer() {
    }
}
