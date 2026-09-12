package com.rupeek.poc.temporal.config.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * The normalised store as a directory tree. Each ":" in a document id becomes a
 * directory level, so the collections lay themselves out exactly as they read:
 *
 *   config-store/pointers/federal.json
 *   config-store/flows/federal/v1.json
 *   config-store/stages/federal/v1/PRECHECK.json
 *   config-store/bindings/federal/b1/PRECHECK.nameDobCheck.json
 *   config-store/stage_knobs/federal/t1/FUND_TRANSFER.json
 *
 * Every read hits the filesystem — no caching, deliberately. That is what lets
 * you edit one call's document while a loan is in retry backoff and watch the
 * next attempt pick it up. Production caches immutable versions forever and
 * re-reads only the pointer.
 */
public class FileConfigStore implements ConfigStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path root;

    public FileConfigStore(Path root) {
        this.root = root;
    }

    private Path pathFor(String collection, String id) {
        Path p = root.resolve(collection);
        String[] parts = id.split(":");
        for (int i = 0; i < parts.length - 1; i++) {
            p = p.resolve(parts[i]);
        }
        return p.resolve(parts[parts.length - 1] + ".json");
    }

    private Path dirFor(String collection, String idPrefix) {
        Path p = root.resolve(collection);
        if (idPrefix != null && !idPrefix.isEmpty()) {
            for (String part : idPrefix.split(":")) {
                p = p.resolve(part);
            }
        }
        return p;
    }

    @Override
    public JsonNode read(String collection, String id) {
        Path p = pathFor(collection, id);
        if (!Files.exists(p)) {
            return null;
        }
        try {
            return MAPPER.readTree(Files.readAllBytes(p));
        } catch (IOException e) {
            throw new UncheckedIOException("Unreadable config document " + p, e);
        }
    }

    @Override
    public List<JsonNode> readAll(String collection, String idPrefix) {
        Path dir = dirFor(collection, idPrefix);
        if (!Files.isDirectory(dir)) {
            return Collections.emptyList();
        }
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.list(dir)) {
            s.filter(f -> f.getFileName().toString().endsWith(".json")).forEach(files::add);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list " + dir, e);
        }
        Collections.sort(files);
        List<JsonNode> out = new ArrayList<>();
        for (Path f : files) {
            try {
                out.add(MAPPER.readTree(Files.readAllBytes(f)));
            } catch (IOException e) {
                throw new UncheckedIOException("Unreadable config document " + f, e);
            }
        }
        return out;
    }

    @Override
    public void write(String collection, String id, JsonNode doc) {
        Path p = pathFor(collection, id);
        try {
            Files.createDirectories(p.getParent());
            Files.write(p, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(doc));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write config document " + id, e);
        }
    }

    @Override
    public List<String> lenders() {
        Path dir = root.resolve(POINTERS);
        if (!Files.isDirectory(dir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> s = Files.list(dir)) {
            List<String> out = new ArrayList<>();
            s.filter(f -> f.getFileName().toString().endsWith(".json"))
                    .forEach(f -> out.add(f.getFileName().toString().replace(".json", "")));
            Collections.sort(out);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list " + dir, e);
        }
    }

    @Override
    public String describe() {
        return "file:" + root.toAbsolutePath().normalize();
    }
}
