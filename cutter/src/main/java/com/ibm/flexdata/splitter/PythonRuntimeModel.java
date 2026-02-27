package com.ibm.flexdata.splitter;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

public class PythonRuntimeModel implements RuntimeModel {

    private final PythonRuntimeClient client;

    public PythonRuntimeModel(PythonRuntimeClient client) {
        this.client = client;
    }

    @Override
    public double predict(Engine engine, OperatorKind kind, OperatorFeatures features) {
        return client.predict(engine, kind, features);
    }
}
