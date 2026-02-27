package com.ibm.flexdata.splitter;

import java.util.EnumMap;
import java.util.Map;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

public class MlRuntimeModel implements RuntimeModel {

    private final Map<Engine, Map<OperatorKind, SingleOperatorModel>> models;

    public MlRuntimeModel(
            Map<Engine, Map<OperatorKind, SingleOperatorModel>> models) {
        this.models = models;
    }

    @Override
    public double predict(Engine engine,
                          OperatorKind kind,
                          OperatorFeatures features) {
        Map<OperatorKind, SingleOperatorModel> byKind = models.get(engine);
        if (byKind == null) {
            throw new IllegalStateException("No models for engine " + engine);
        }
        SingleOperatorModel m = byKind.get(kind);
        if (m == null) {

            return heuristicFallback(engine, kind, features);
        }
        return m.predict(features);
    }

    private double heuristicFallback(Engine engine,
                                     OperatorKind kind,
                                     OperatorFeatures f) {
        // double rows = (f.outputRows > 0) ? f.outputRows : 1.0;
        // double width = (f.rowSizeOutBytes > 0) ? f.rowSizeOutBytes : 8.0;
        // double base = rows * width;
        // // Maybe assume DUCKDB baseline, DATAFUSION a bit faster:
        // return (engine == Engine.DUCKDB) ? base : base * 0.8;

        return 0;
    }


    public static MlRuntimeModel dummy() {
        Map<Engine, Map<OperatorKind, SingleOperatorModel>> map =
            new EnumMap<>(Engine.class);

        for (Engine engine : Engine.values()) {
            Map<OperatorKind, SingleOperatorModel> byKind =
                new EnumMap<>(OperatorKind.class);

            byKind.put(OperatorKind.FILTER, features ->
                baseCost(features) * (engine == Engine.DUCKDB ? 1.0 : 0.9));
            byKind.put(OperatorKind.JOIN, features ->
                baseCost(features) * 2.0 * (engine == Engine.DUCKDB ? 1.0 : 0.8));
            byKind.put(OperatorKind.SORT, features ->
                baseCost(features) * 1.5 * (engine == Engine.DUCKDB ? 1.0 : 0.85));
            byKind.put(OperatorKind.AGGREGATE, features ->
                baseCost(features) * 1.2 * (engine == Engine.DUCKDB ? 1.0 : 0.9));
            // OTHER left out → fallback heuristic

            map.put(engine, byKind);
        }

        return new MlRuntimeModel(map);
    }

    private static double baseCost(OperatorFeatures features) {
        double rows = (features.outputRows > 0) ? features.outputRows : 1.0;
        double width =
            (features.rowSizeOutBytes > 0) ? features.rowSizeOutBytes : 8.0;
        return rows * width;
    }
}
