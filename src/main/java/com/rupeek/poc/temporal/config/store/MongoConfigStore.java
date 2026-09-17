package com.rupeek.poc.temporal.config.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The normalised store in Mongo. One collection per entity, one document per
 * stage / per call. Enabled with CONFIG_STORE=mongo.
 *
 * Documents are stored as REAL BSON, not as opaque JSON strings. That only
 * became possible once the data was normalised: a callKey like
 * "PRECHECK.nameDobCheck" used to be a field NAME (dots in BSON field names are
 * a minefield) and is now an _id VALUE. The payoff is that everything is
 * queryable and editable the normal way:
 *
 *   db.bindings.find({ lenderSlug: "federal", stage: "FUND_TRANSFER" })
 *   db.bindings.updateOne({_id: "federal:b1:STP.customerInfo"},
 *                         {$set: {path: "/stp/v2/customer-info"}})
 *
 * and that second command repairs a loan that is currently in retry backoff.
 */
public class MongoConfigStore implements ConfigStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(MongoConfigStore.class);

    private final MongoClient client;
    private final MongoDatabase db;
    private final String uri;
    private final String dbName;

    public MongoConfigStore(String uri, String dbName) {
        this.uri = uri;
        this.dbName = dbName;
        this.client = MongoClients.create(uri);
        this.db = client.getDatabase(dbName);
    }

    private MongoCollection<Document> col(String collection) {
        return db.getCollection(collection);
    }

    @Override
    public JsonNode read(String collection, String id) {
        Document d = col(collection).find(Filters.eq("_id", id)).first();
        log.debug("read {}/{} -> {}", collection, id, d == null ? "MISSING" : "ok");
        return d == null ? null : toJson(d);
    }

    @Override
    public List<JsonNode> readAll(String collection, String idPrefix) {
        Bson filter = Filters.regex("_id", Pattern.compile("^" + Pattern.quote(idPrefix + ":")));
        List<JsonNode> out = new ArrayList<>();
        for (Document d : col(collection).find(filter).sort(Sorts.ascending("_id"))) {
            out.add(toJson(d));
        }
        return out;
    }

    @Override
    public void write(String collection, String id, JsonNode doc) {
        try {
            Document d = Document.parse(MAPPER.writeValueAsString(doc));
            d.put("_id", id);
            col(collection).replaceOne(Filters.eq("_id", id), d,
                    new ReplaceOptions().upsert(true));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot write " + collection + "/" + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void clear() {
        for (String c : new String[]{LENDERS, POINTERS, FLOWS, STAGES, BINDINGS,
                TRANSPORTS, STAGE_KNOBS}) {
            col(c).drop();
        }
        log.debug("dropped all config collections in {}", dbName);
    }

    @Override
    public List<String> lenders() {
        List<String> out = new ArrayList<>();
        for (String s : col(POINTERS).distinct("_id", String.class)) {
            out.add(s);
        }
        Collections.sort(out);
        return out;
    }

    @Override
    public String describe() {
        return "mongo:" + uri + "/" + dbName;
    }

    /**
     * Indexes that make the per-stage view fast and the maintenance queries
     * natural. Created by the seeder; harmless to re-run.
     */
    public void ensureIndexes() {
        col(BINDINGS).createIndex(new Document("lenderSlug", 1)
                .append("bindingVersion", 1).append("stage", 1));
        col(STAGES).createIndex(new Document("lenderSlug", 1).append("flowVersion", 1));
        col(STAGE_KNOBS).createIndex(new Document("lenderSlug", 1).append("transportVersion", 1));
    }

    private JsonNode toJson(Document d) {
        try {
            return MAPPER.readTree(d.toJson());
        } catch (Exception e) {
            throw new IllegalStateException("Corrupt config document " + d.get("_id"), e);
        }
    }

    public void close() {
        client.close();
    }
}
