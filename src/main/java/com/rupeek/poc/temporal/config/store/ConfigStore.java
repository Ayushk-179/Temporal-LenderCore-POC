package com.rupeek.poc.temporal.config.store;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * A normalised document store: collections of small documents addressed by a
 * colon-separated id. Dumb on purpose — all the meaning lives in
 * {@link ConfigResolver}, so a backend is ~60 lines.
 *
 * THE COLLECTIONS, and why the data is shaped this way:
 *
 *   lenders      "federal"                          display metadata
 *   pointers     "federal"                          the ONE mutable document
 *   flows        "federal:v1"                       stage NAMES in order
 *   stages       "federal:v1:PRECHECK"              one document PER STAGE
 *   bindings     "federal:b1:PRECHECK.nameDobCheck" one document PER CALL
 *   transports   "federal:t1"                       baseUrl, static headers
 *   stage_knobs  "federal:t1:PRECHECK"              retry/timeout PER STAGE
 *
 * Normalising by stage buys three concrete things:
 *
 *   1. EDITING. Changing one lender call rewrites one small document instead of
 *      a 9 KB blob, so two people editing two different calls cannot clobber
 *      each other.
 *   2. READING. The runtime's three resolution points map 1:1 onto documents —
 *      loadStageKnobs is ONE stage_knobs read, a call attempt is ONE bindings
 *      read. No sub-document extraction, no over-fetching.
 *   3. BROWSING. In Compass you can filter bindings by {stage: "FUND_TRANSFER"}
 *      and see exactly what money movement touches. As one blob you cannot.
 *
 * It also removes a real Mongo hazard: callKeys contain dots
 * ("PRECHECK.nameDobCheck"). As a blob those were FIELD NAMES, which is why the
 * first cut stored opaque JSON strings. Normalised, a callKey is an _id VALUE,
 * so documents are stored as genuine BSON and are queryable and editable in
 * mongosh like anything else.
 */
public interface ConfigStore {

    String LENDERS = "lenders";
    String POINTERS = "pointers";
    String FLOWS = "flows";
    String STAGES = "stages";
    String BINDINGS = "bindings";
    String TRANSPORTS = "transports";
    String STAGE_KNOBS = "stage_knobs";

    /** Returns null when the document does not exist. */
    JsonNode read(String collection, String id);

    /** Every document in the collection whose id starts with idPrefix + ":". */
    List<JsonNode> readAll(String collection, String idPrefix);

    void write(String collection, String id, JsonNode doc);

    List<String> lenders();

    /** Human-readable location, printed on worker boot so a demo cannot lie to you. */
    String describe();

    static String id(String... parts) {
        return String.join(":", parts);
    }
}
