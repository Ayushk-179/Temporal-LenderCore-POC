package com.rupeek.poc.temporal.config.template;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Response-side extraction. Two forms, both config-editable at runtime:
 *
 *   "$.data.customer_id"          dot path
 *   "$.data.items[0].ref"         with array indexing
 *   "regex:$.message:/REF-(\d+)/" pull a capture group out of a string field
 *
 * The regex form is deliberately confined to EXTRACTION. Regex is the right tool
 * for scraping a reference id out of a lender's free-text message; it is the
 * wrong tool for constructing a request payload, which is what TemplateRenderer
 * is for.
 */
public final class JsonPaths {

    /** Returns null when the path does not resolve or the regex does not match. */
    public static String extract(JsonNode root, String spec) {
        if (root == null || spec == null) {
            return null;
        }
        if (spec.startsWith("regex:")) {
            String rest = spec.substring(6);
            int sep = rest.indexOf(":/");
            if (sep < 0 || !rest.endsWith("/")) {
                throw new IllegalArgumentException(
                        "Bad regex extractor '" + spec + "'. Expected regex:<path>:/<pattern>/");
            }
            String value = at(root, rest.substring(0, sep));
            if (value == null) {
                return null;
            }
            Matcher m = Pattern.compile(rest.substring(sep + 2, rest.length() - 1)).matcher(value);
            if (!m.find()) {
                return null;
            }
            return m.groupCount() >= 1 ? m.group(1) : m.group();
        }
        return at(root, spec);
    }

    /** "$.data.items[0].id" -> node walk. No JSONPath dependency needed. */
    public static String at(JsonNode root, String path) {
        if (root == null || path == null) {
            return null;
        }
        String p = path.startsWith("$.") ? path.substring(2) : path;
        JsonNode cur = root;
        for (String rawSeg : p.split("\\.")) {
            if (cur == null) {
                return null;
            }
            String seg = rawSeg;
            int bracket = seg.indexOf('[');
            if (bracket >= 0) {
                String field = seg.substring(0, bracket);
                if (!field.isEmpty()) {
                    cur = cur.get(field);
                }
                // Supports chained indices: items[0][2]
                String indices = seg.substring(bracket);
                Matcher m = Pattern.compile("\\[(\\d+)]").matcher(indices);
                while (cur != null && m.find()) {
                    cur = cur.get(Integer.parseInt(m.group(1)));
                }
            } else {
                cur = cur.get(seg);
            }
        }
        if (cur == null || cur.isNull()) {
            return null;
        }
        return cur.isValueNode() ? cur.asText() : cur.toString();
    }

    private JsonPaths() {
    }
}
