package com.ibm.flexdata.splitter;

import java.util.Locale;

public enum Dataset {
    STACKOVERFLOW("STACK", "so_relations_data.json"),
    IMDB("IMDB", "imdb_relations_data.json"),
    TPCH1("TPCH", "tpch1_relations_data.json"),
    TPCH10("TPCH", "tpch10_relations_data.json");

    private final String schemaName;
    private final String statsResource;

    Dataset(String schemaName, String statsResource) {
        this.schemaName = schemaName;
        this.statsResource = statsResource;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getStatsResource() {
        return statsResource;
    }

    public static Dataset fromArgOrDefault(String value) {
        if (value == null || value.isBlank()) {
            return STACKOVERFLOW;
        }
        String norm = value.trim().toUpperCase(Locale.ROOT);
        switch (norm) {
            case "STACK":
            case "STACKOVERFLOW":
            case "SO":
                return STACKOVERFLOW;
            case "IMDB":
                return IMDB;
            case "TPCH":
            case "TPCH1":
            case "TPCH-SF1":
            case "TPCH_SF1":
            case "TPCH-1":
                return TPCH1;
            case "TPCH10":
            case "TPCH-SF10":
            case "TPCH_SF10":
            case "TPCH-10":
                return TPCH10;
            default:
                throw new IllegalArgumentException(
                    "Unknown schema '" + value + "'. Use STACK, IMDB, TPCH1, or TPCH10.");
        }
    }
}
