package com.ibm.flexdata.splitter;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;

public class PlanOptimizer {

    private final HepPlanner planner;
    private final JaninoRelMetadataProvider metadataProvider;

    public PlanOptimizer(HepProgram program,
                         JaninoRelMetadataProvider metadataProvider) {
        this.planner = new HepPlanner(program);
        this.metadataProvider = metadataProvider;
    }

    public RelRoot optimize(RelRoot root) {
        // use our metadata provider for this plan's cluster
        root.rel.getCluster().setMetadataProvider(metadataProvider);

        planner.clear();
        planner.setRoot(root.rel);
        RelNode best = planner.findBestExp();

        return root.withRel(best);
    }

    // PlanOptimizer
    public JaninoRelMetadataProvider getMetadataProvider() {
        return metadataProvider;
    }

}
