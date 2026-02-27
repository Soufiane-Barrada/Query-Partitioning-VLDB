package com.ibm.flexdata.splitter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class SoStatsRegistry {

    private final Map<String, SoStats.TableStats> tables; // tableName(lower) -> stats

    private SoStatsRegistry(Map<String, SoStats.TableStats> tables) {
        this.tables = tables;
    }

    private static final Map<Dataset, SoStatsRegistry> INSTANCES =
        new EnumMap<>(Dataset.class);

    public static synchronized SoStatsRegistry getInstance() {
        return getInstance(Dataset.STACKOVERFLOW);
    }

    public static synchronized SoStatsRegistry getInstance(Dataset dataset) {
        if (dataset == null) {
            dataset = Dataset.STACKOVERFLOW;
        }
        SoStatsRegistry instance = INSTANCES.get(dataset);
        if (instance == null) {
            instance = loadFromResource(dataset.getStatsResource());
            INSTANCES.put(dataset, instance);
        }
        return instance;
    }

    public SoStats.TableStats getTableStats(String tableName) {
        if (tableName == null) return null;
        return tables.get(tableName.toLowerCase());
    }

    private static SoStats.TableStats normalizeTableStats(String key, SoStats.TableStats ts) {
        if (ts == null) {
            ts = new SoStats.TableStats();
        }
        if (ts.table == null) {
            ts.table = key.toLowerCase();
        }
        return ts;
    }

    private static SoStatsRegistry loadFromResource(String resourceName) {
        try (InputStream in = SoStatsRegistry.class
                .getClassLoader()
                .getResourceAsStream(resourceName)) {

            if (in == null) {
                throw new IllegalStateException(
                    "Could not find " + resourceName +
                    " on classpath. Put it under src/main/resources/");
            }

            ObjectMapper mapper = new ObjectMapper();

            // top-level JSON: Map<tableName, TableStats>
            Map<String, SoStats.TableStats> raw =
                mapper.readValue(in,
                    new TypeReference<Map<String, SoStats.TableStats>>() {});

            Map<String, SoStats.TableStats> normalized = new HashMap<>();
            for (Map.Entry<String, SoStats.TableStats> e : raw.entrySet()) {
                String key = e.getKey().toLowerCase();
                SoStats.TableStats ts = normalizeTableStats(key, e.getValue());
                normalized.put(key, ts);
            }

            return new SoStatsRegistry(normalized);
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to load stats from " + resourceName, e);
        }
    }
}
