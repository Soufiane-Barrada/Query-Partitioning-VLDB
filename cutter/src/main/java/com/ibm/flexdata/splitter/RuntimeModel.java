package com.ibm.flexdata.splitter;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

public interface RuntimeModel {

    /**
     * Predict execution time (or cost) of a single operator on a given engine.
     */
    double predict(Engine engine, OperatorKind kind, OperatorFeatures features);
}
