package com.ibm.flexdata.splitter;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

@FunctionalInterface
public interface SingleOperatorModel {
    double predict(OperatorFeatures features);
}
