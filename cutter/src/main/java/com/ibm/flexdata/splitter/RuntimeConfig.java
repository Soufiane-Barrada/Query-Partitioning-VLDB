package com.ibm.flexdata.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumMap;
import java.util.Map;

/**
 * Runtime configuration loaded from a JSON resource on the classpath.
 *
 * Required JSON shape:
 * {
 *   "pythonExecutable": "...",
 *   "predictorScript": "...",
 *   "models": {
 *     "DUCKDB": { "JOIN": "...", "FILTER": "...", "AGGREGATE": "...", "SORT": "..." },
 *     "DATAFUSION": { "JOIN": "...", "FILTER": "...", "AGGREGATE": "...", "SORT": "..." }
 *   },
 *   "transferModels": {
 *     "DUCKDB":     {"aMs":..,"bMsPerRow":..,"cMsPerRowSizeByte":..,"dMsPerOutputByte":..},
 *     "DATAFUSION": {"aMs":..,"bMsPerRow":..,"cMsPerRowSizeByte":..,"dMsPerOutputByte":..}
 *   }
 * }
 */
public class RuntimeConfig {

    public String pythonExecutable;
    public String predictorScript;
    public Map<String, Map<String, String>> models;

    // learned transfer model coefficients per receiver engine
    public Map<String, TransferModelCoeffs> transferModels;

    public static final class TransferModelCoeffs {
        public double aMs;
        public double bMsPerRow;
        public double cMsPerRowSizeByte;
        public double dMsPerOutputByte;
    }

    /**
     * Load runtime config from a JSON resource on the classpath.
     * Returns null if the resource does not exist.
     */
    public static RuntimeConfig loadFromClasspath(String resourceName) throws IOException {
        ClassLoader cl = RuntimeConfig.class.getClassLoader();
        try (InputStream in = cl.getResourceAsStream(resourceName)) {
            if (in == null) {
                return null;
            }
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(in, RuntimeConfig.class);
        }
    }

    public Map<Engine, Map<OperatorKind, String>> toModelPaths() {
        if (models == null) {
            throw new IllegalStateException("models section missing in runtime config");
        }

        Map<Engine, Map<OperatorKind, String>> result = new EnumMap<>(Engine.class);

        for (Map.Entry<String, Map<String, String>> engineEntry : models.entrySet()) {
            Engine engine = Engine.valueOf(engineEntry.getKey());
            Map<OperatorKind, String> byKind = new EnumMap<>(OperatorKind.class);

            for (Map.Entry<String, String> opEntry : engineEntry.getValue().entrySet()) {
                OperatorKind kind = OperatorKind.valueOf(opEntry.getKey());
                byKind.put(kind, opEntry.getValue());
            }

            result.put(engine, byKind);
        }

        // Strict: must have both engines present
        if (!result.containsKey(Engine.DUCKDB) || !result.containsKey(Engine.DATAFUSION)) {
            throw new IllegalStateException("models must include DUCKDB and DATAFUSION");
        }

        return result;
    }

    public Map<Engine, TransferModelCoeffs> toTransferModelsStrict() {
        if (transferModels == null) {
            throw new IllegalStateException("transferModels section missing in runtime config");
        }

        TransferModelCoeffs duck = transferModels.get("DUCKDB");
        TransferModelCoeffs df = transferModels.get("DATAFUSION");

        if (duck == null || df == null) {
            throw new IllegalStateException("transferModels must include DUCKDB and DATAFUSION");
        }

        requireFiniteNonNegative("transferModels.DUCKDB.aMs", duck.aMs);
        requireFiniteNonNegative("transferModels.DUCKDB.bMsPerRow", duck.bMsPerRow);
        requireFiniteNonNegative("transferModels.DUCKDB.cMsPerRowSizeByte", duck.cMsPerRowSizeByte);
        requireFiniteNonNegative("transferModels.DUCKDB.dMsPerOutputByte", duck.dMsPerOutputByte);

        requireFiniteNonNegative("transferModels.DATAFUSION.aMs", df.aMs);
        requireFiniteNonNegative("transferModels.DATAFUSION.bMsPerRow", df.bMsPerRow);
        requireFiniteNonNegative("transferModels.DATAFUSION.cMsPerRowSizeByte", df.cMsPerRowSizeByte);
        requireFiniteNonNegative("transferModels.DATAFUSION.dMsPerOutputByte", df.dMsPerOutputByte);

        Map<Engine, TransferModelCoeffs> out = new EnumMap<>(Engine.class);
        out.put(Engine.DUCKDB, duck);
        out.put(Engine.DATAFUSION, df);
        return out;
    }

    private static void requireFiniteNonNegative(String name, double v) {
        if (!Double.isFinite(v) || v < 0.0) {
            throw new IllegalStateException(name + " must be finite and >= 0 (got " + v + ")");
        }
    }
}
