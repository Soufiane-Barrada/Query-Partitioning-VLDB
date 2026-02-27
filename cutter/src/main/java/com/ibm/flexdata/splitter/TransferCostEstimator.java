package com.ibm.flexdata.splitter;

import java.util.EnumMap;
import java.util.Map;

public final class TransferCostEstimator {

    public static final class Coeffs {
        public final double aMs;
        public final double bMsPerRow;
        public final double cMsPerRowSizeByte;
        public final double dMsPerOutputByte;

        public Coeffs(double aMs,
                      double bMsPerRow,
                      double cMsPerRowSizeByte,
                      double dMsPerOutputByte) {
            this.aMs = aMs;
            this.bMsPerRow = bMsPerRow;
            this.cMsPerRowSizeByte = cMsPerRowSizeByte;
            this.dMsPerOutputByte = dMsPerOutputByte;
        }
    }

    private final EnumMap<Engine, Coeffs> byReceiverEngine;
    private final double extraConstantMs;

    public TransferCostEstimator(Map<Engine, Coeffs> byReceiverEngine,
                                 double extraConstantMs) {
        if (byReceiverEngine == null) {
            throw new IllegalArgumentException("byReceiverEngine is null");
        }
        if (!byReceiverEngine.containsKey(Engine.DUCKDB)
            || !byReceiverEngine.containsKey(Engine.DATAFUSION)) {
            throw new IllegalArgumentException(
                "Transfer coeffs must be provided for both DUCKDB and DATAFUSION");
        }
        this.byReceiverEngine = new EnumMap<>(Engine.class);
        this.byReceiverEngine.putAll(byReceiverEngine);

        if (!Double.isFinite(extraConstantMs) || extraConstantMs < 0.0) {
            throw new IllegalArgumentException("extraConstantMs must be finite and >= 0");
        }
        this.extraConstantMs = extraConstantMs;
    }


    // lets JSON record what coeffs were used for the run
    public Map<Engine, Coeffs> getAllCoeffsView() {
        return java.util.Collections.unmodifiableMap(byReceiverEngine);
    }


    /**
     * Estimate transfer time in milliseconds for a cut whose subtree output is:
     *   outputBytes = rows * rowSizeBytes
     */
    public double estimateMs(Engine receiver, double rows, double rowSizeBytes) {
        if (receiver == null) {
            throw new IllegalArgumentException("receiver is null");
        }
        if (!Double.isFinite(rows) || rows < 0.0) {
            throw new IllegalArgumentException("rows must be finite and >= 0 (got " + rows + ")");
        }
        if (!Double.isFinite(rowSizeBytes) || rowSizeBytes <= 0.0) {
            throw new IllegalArgumentException(
                "rowSizeBytes must be finite and > 0 (got " + rowSizeBytes + ")");
        }

        Coeffs c = byReceiverEngine.get(receiver);
        if (c == null) {
            throw new IllegalStateException("Missing coeffs for receiver " + receiver);
        }

        double outBytes = rows * rowSizeBytes;

        double ms = c.aMs
                  + c.bMsPerRow * rows
                  + c.cMsPerRowSizeByte * rowSizeBytes
                  + c.dMsPerOutputByte * outBytes;

        if (!Double.isFinite(ms) || ms < 0.0) {
            throw new IllegalStateException("Computed transfer ms invalid: " + ms);
        }

        return ms + extraConstantMs;
    }

    public double getExtraConstantMs() {
        return extraConstantMs;
    }
}
