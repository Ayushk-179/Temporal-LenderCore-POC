package com.rupeek.poc.temporal.config.store;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Picks the store from the environment, so the worker and the seeder can never
 * disagree about where config lives.
 *
 *   CONFIG_STORE=file   (default)  CONFIG_DIR=./config-store
 *   CONFIG_STORE=mongo             MONGO_URI=mongodb://localhost:27017
 *                                  MONGO_DB=lender_config
 */
public final class ConfigStores {

    public static ConfigStore fromEnv() {
        String kind = env("CONFIG_STORE", "file");
        if ("mongo".equalsIgnoreCase(kind)) {
            return new MongoConfigStore(
                    env("MONGO_URI", "mongodb://localhost:27017"),
                    env("MONGO_DB", "lender_config"));
        }
        return new FileConfigStore(Paths.get(env("CONFIG_DIR", "./config-store")));
    }

    public static ConfigStore file(Path root) {
        return new FileConfigStore(root);
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            v = System.getProperty(key);
        }
        return v == null || v.isBlank() ? fallback : v;
    }

    private ConfigStores() {
    }
}
