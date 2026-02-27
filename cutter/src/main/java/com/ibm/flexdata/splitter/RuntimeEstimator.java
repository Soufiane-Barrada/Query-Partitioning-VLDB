package com.ibm.flexdata.splitter;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

public class RuntimeEstimator {

    private final RuntimeModel runtimeModel;

    public RuntimeEstimator(RuntimeModel runtimeModel) {
        this.runtimeModel = runtimeModel;
    }

    /**
     * @return map: relId -> (Engine -> predicted runtime)
     */
    public Map<Integer, Map<Engine, Double>> estimateRuntimes(
            Map<Integer, OperatorFeatures> featuresById) {

        Map<Integer, Map<Engine, Double>> runtimes = new ConcurrentHashMap<>();

        featuresById.values()
            .parallelStream()
            .forEach(f -> {
                EnumMap<Engine, Double> byEngine = new EnumMap<>(Engine.class);
                for (Engine engine : Engine.values()) {
                    double cost = runtimeModel.predict(engine, f.operatorKind, f);
                    byEngine.put(engine, cost);
                }
                runtimes.put(f.relId, byEngine);
            });

        return runtimes;
    }
}
