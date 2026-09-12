package com.rupeek.poc.temporal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rupeek.poc.temporal.config.template.JsonPaths;
import com.rupeek.poc.temporal.config.template.TemplateRenderer;
import com.rupeek.poc.temporal.config.template.UnresolvedTemplateException;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The payload/response half of "change the API contract without a redeploy".
 *
 * Everything here is expressible in a config document, so a lender changing its
 * request shape is an edit, not a release.
 */
class TemplateRendererTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static Map<String, String> ctx() {
        Map<String, String> c = new LinkedHashMap<>();
        c.put("pan", "aaapz1234c");
        c.put("loanAmount", "150000");
        c.put("isActive", "true");
        c.put("itemsJson", "[{\"sku\":\"GOLD-22K\"},{\"sku\":\"GOLD-18K\"}]");
        c.put("customerName", "test customer");
        return c;
    }

    private static JsonNode tmpl(String json) throws Exception {
        return M.readTree(json);
    }

    @Test
    void nestedObjectsAndArrays_areRendered() throws Exception {
        JsonNode out = TemplateRenderer.render(tmpl(
                "{\"customer\":{\"pan\":\"${ctx.pan | upper}\"},"
                        + "\"tags\":[\"${ctx.lenderSlug ?: \\\"federal\\\"}\",\"static\"]}"),
                ctx(), true);

        assertEquals("AAAPZ1234C", out.get("customer").get("pan").asText());
        assertEquals("federal", out.get("tags").get(0).asText());
        assertEquals("static", out.get("tags").get(1).asText());
    }

    @Test
    void typedLeaves_produceRealJsonTypes_notStrings() throws Exception {
        JsonNode out = TemplateRenderer.render(tmpl(
                "{\"amount\":\"${num:ctx.loanAmount}\",\"active\":\"${bool:ctx.isActive}\","
                        + "\"items\":\"${json:ctx.itemsJson}\"}"),
                ctx(), true);

        assertTrue(out.get("amount").isNumber());
        assertEquals(150000, out.get("amount").asInt());
        assertTrue(out.get("active").isBoolean());
        assertTrue(out.get("active").asBoolean());
        assertTrue(out.get("items").isArray());
        assertEquals("GOLD-22K", out.get("items").get(0).get("sku").asText());
    }

    @Test
    void optionalKey_isDroppedWhenUnresolved_evenInStrictMode() throws Exception {
        JsonNode out = TemplateRenderer.render(tmpl(
                "{\"pan\":\"${ctx.pan}\",\"?middleName\":\"${ctx.middleName}\"}"),
                ctx(), true);

        assertTrue(out.has("pan"));
        assertFalse(out.has("middleName"));
        assertFalse(out.has("?middleName"));
    }

    /**
     * The bug this replaces: the old renderer did ctx.getOrDefault(key, ""), so a
     * missing value silently became an empty string in a request body. On a
     * disbursal call that reaches the lender.
     */
    @Test
    void strictMode_failsLoudly_insteadOfSendingAnEmptyString() throws Exception {
        UnresolvedTemplateException e = assertThrows(UnresolvedTemplateException.class,
                () -> TemplateRenderer.render(
                        tmpl("{\"accountNumber\":\"${ctx.loanAccountNumber}\"}"), ctx(), true));
        assertTrue(e.getMessage().contains("loanAccountNumber"));
    }

    @Test
    void nonStrictMode_rendersNull_forCallersThatWantIt() throws Exception {
        JsonNode out = TemplateRenderer.render(
                tmpl("{\"accountNumber\":\"${ctx.loanAccountNumber}\"}"), ctx(), false);
        assertTrue(out.get("accountNumber").isNull());
    }

    @Test
    void interpolationInsideALongerString_stillWorks() {
        assertEquals("/loan/150000/status",
                TemplateRenderer.renderString("/loan/${ctx.loanAmount}/status", ctx(), true));
    }

    @Test
    void unknownFunction_isRejected_ratherThanIgnored() {
        assertThrows(UnresolvedTemplateException.class,
                () -> TemplateRenderer.renderString("${ctx.pan | shell}", ctx(), true));
    }

    // ------------------------------------------------------------ extraction

    @Test
    void extraction_supportsArrayIndexing() throws Exception {
        JsonNode resp = M.readTree(
                "{\"data\":{\"items\":[{\"ref\":\"R-1\"},{\"ref\":\"R-2\"}]}}");
        assertEquals("R-2", JsonPaths.extract(resp, "$.data.items[1].ref"));
        assertNull(JsonPaths.extract(resp, "$.data.items[9].ref"));
    }

    /** Regex belongs in extraction — scraping an id out of a lender's prose. */
    @Test
    void extraction_supportsRegexCaptureOutOfAFreeTextField() throws Exception {
        JsonNode resp = M.readTree(
                "{\"message\":\"Disbursal queued with reference REF-889213 for today\"}");
        assertEquals("889213", JsonPaths.extract(resp, "regex:$.message:/REF-(\\d+)/"));
        assertNull(JsonPaths.extract(resp, "regex:$.message:/UTR-(\\d+)/"));
    }
}
