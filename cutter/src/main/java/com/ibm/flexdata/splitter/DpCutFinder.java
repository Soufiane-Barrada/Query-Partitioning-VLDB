package com.ibm.flexdata.splitter;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;
import org.apache.calcite.rel.RelNode;

import java.util.*;

public class DpCutFinder {

    public enum AggMode { SUM, MAX }

    public static class PlanChoice {
        public final boolean hasCut;
        public int cutNodeId;                // -1 if no cut
        public final Engine q1Engine;        // engine for subtree (Q1)
        public final Engine q2Engine;        // engine for remainder (Q2)
        public final double costAllDuckdb;
        public final double costAllDatafusion;
        public final double chosenCost;

        public PlanChoice(boolean hasCut,
                          int cutNodeId,
                          Engine q1Engine,
                          Engine q2Engine,
                          double costAllDuckdb,
                          double costAllDatafusion,
                          double chosenCost) {
            this.hasCut = hasCut;
            this.cutNodeId = cutNodeId;
            this.q1Engine = q1Engine;
            this.q2Engine = q2Engine;
            this.costAllDuckdb = costAllDuckdb;
            this.costAllDatafusion = costAllDatafusion;
            this.chosenCost = chosenCost;
        }
    }

    private static class AggCost {
        final double duckdb;
        final double datafusion;

        AggCost(double duckdb, double datafusion) {
            this.duckdb = duckdb;
            this.datafusion = datafusion;
        }
    }

    private final AggMode mode;
    private final TransferCostEstimator transferEstimator;

    // SUM-mode tracking (per call)
    private double bestNetSavingDuckToDf;
    private int bestNodeDuckToDf;

    private double bestNetSavingDfToDuck;
    private int bestNodeDfToDuck;

    public DpCutFinder(AggMode mode, TransferCostEstimator transferEstimator) {
        if (transferEstimator == null) {
            throw new IllegalArgumentException("transferEstimator is null");
        }
        this.mode = (mode == null) ? AggMode.SUM : mode;
        this.transferEstimator = transferEstimator;
    }

    public PlanChoice findBestPlan(RelNode root,
                                   Map<Integer, Map<Engine, Double>> runtimes,
                                   Map<Integer, OperatorFeatures> featuresById) {
        if (root == null) throw new IllegalArgumentException("root is null");
        if (runtimes == null) throw new IllegalArgumentException("runtimes is null");
        if (featuresById == null) throw new IllegalArgumentException("featuresById is null");

        if (mode == AggMode.MAX) {
            return findBestPlanMax(root, runtimes, featuresById);
        }
        return findBestPlanSum(root, runtimes, featuresById);
    }

    // =========================================================================
    //  SUM MODE
    // =========================================================================

    private PlanChoice findBestPlanSum(RelNode root,
                                       Map<Integer, Map<Engine, Double>> runtimes,
                                       Map<Integer, OperatorFeatures> featuresById) {

        bestNetSavingDuckToDf = Double.NEGATIVE_INFINITY;
        bestNodeDuckToDf = -1;
        bestNetSavingDfToDuck = Double.NEGATIVE_INFINITY;
        bestNodeDfToDuck = -1;

        AggCost rootCost = visitSum(root, runtimes, featuresById);
        double rootDuck = rootCost.duckdb;
        double rootDf = rootCost.datafusion;

        // Candidate 1: all DuckDB
        double bestCost = rootDuck;
        boolean bestHasCut = false;
        int bestCutId = -1;
        Engine bestQ1 = Engine.DUCKDB;
        Engine bestQ2 = Engine.DUCKDB;

        // Candidate 2: all DataFusion
        if (rootDf < bestCost) {
            bestCost = rootDf;
            bestHasCut = false;
            bestCutId = -1;
            bestQ1 = Engine.DATAFUSION;
            bestQ2 = Engine.DATAFUSION;
        }

        // Candidate 3: base DUCKDB, subtree DATAFUSION
        if (bestNodeDuckToDf >= 0 && bestNetSavingDuckToDf > 0.0) {
            double cand = rootDuck - bestNetSavingDuckToDf;
            if (cand < bestCost) {
                bestCost = cand;
                bestHasCut = true;
                bestCutId = bestNodeDuckToDf;
                bestQ1 = Engine.DATAFUSION;
                bestQ2 = Engine.DUCKDB;
            }
        }

        // Candidate 4: base DATAFUSION, subtree DUCKDB
        if (bestNodeDfToDuck >= 0 && bestNetSavingDfToDuck > 0.0) {
            double cand = rootDf - bestNetSavingDfToDuck;
            if (cand < bestCost) {
                bestCost = cand;
                bestHasCut = true;
                bestCutId = bestNodeDfToDuck;
                bestQ1 = Engine.DUCKDB;
                bestQ2 = Engine.DATAFUSION;
            }
        }

        return new PlanChoice(
            bestHasCut,
            bestCutId,
            bestQ1,
            bestQ2,
            rootDuck,
            rootDf,
            bestCost
        );
    }

    private AggCost visitSum(RelNode node,
                             Map<Integer, Map<Engine, Double>> runtimes,
                             Map<Integer, OperatorFeatures> featuresById) {

        int id = node.getId();

        Map<Engine, Double> self = runtimes.get(id);
        if (self == null) {
            throw new IllegalStateException("Missing runtimes for nodeId=" + id);
        }
        Double selfDuck = self.get(Engine.DUCKDB);
        Double selfDf = self.get(Engine.DATAFUSION);
        if (selfDuck == null || selfDf == null) {
            throw new IllegalStateException("Missing per-engine runtimes for nodeId=" + id);
        }
        requireFiniteNonNegative("runtime(DUCKDB) nodeId=" + id, selfDuck);
        requireFiniteNonNegative("runtime(DATAFUSION) nodeId=" + id, selfDf);

        double costDuck = selfDuck;
        double costDf = selfDf;

        for (RelNode input : node.getInputs()) {
            AggCost c = visitSum(input, runtimes, featuresById);
            costDuck += c.duckdb;
            costDf += c.datafusion;
        }

        // Net saving if base=DUCKDB and we run this subtree on DATAFUSION:
        // save = (costDuck - costDf) - transfer(receiver=DUCKDB, output of this node)
        double transferToDuckMs = transferMsForCutNode(id, Engine.DUCKDB, featuresById);
        double netSavingDuckToDf = (costDuck - costDf) - transferToDuckMs;

        if (netSavingDuckToDf > bestNetSavingDuckToDf) {
            bestNetSavingDuckToDf = netSavingDuckToDf;
            bestNodeDuckToDf = id;
        }

        // Net saving if base=DATAFUSION and we run this subtree on DUCKDB:
        // save = (costDf - costDuck) - transfer(receiver=DATAFUSION, output of this node)
        double transferToDfMs = transferMsForCutNode(id, Engine.DATAFUSION, featuresById);
        double netSavingDfToDuck = (costDf - costDuck) - transferToDfMs;

        if (netSavingDfToDuck > bestNetSavingDfToDuck) {
            bestNetSavingDfToDuck = netSavingDfToDuck;
            bestNodeDfToDuck = id;
        }

        return new AggCost(costDuck, costDf);
    }

    // =========================================================================
    //  MAX MODE
    // =========================================================================

    private static final class NodeInfo {
        final int id;
        int parentId = -1;
        final List<Integer> children = new ArrayList<>();

        double selfDuck;
        double selfDf;

        double subDuck;
        double subDf;

        int maxChildDuckId = -1;
        double maxChildDuck = 0.0;
        double secondMaxChildDuck = 0.0;

        int maxChildDfId = -1;
        double maxChildDf = 0.0;
        double secondMaxChildDf = 0.0;

        NodeInfo(int id) { this.id = id; }
    }

    private PlanChoice findBestPlanMax(RelNode root,
                                       Map<Integer, Map<Engine, Double>> runtimes,
                                       Map<Integer, OperatorFeatures> featuresById) {

        Map<Integer, NodeInfo> info = new HashMap<>();
        int rootId = root.getId();

        computeSubtreeMax(root, -1, runtimes, info);

        for (NodeInfo p : info.values()) {
            computeTop2Children(p, info);
        }

        double costAllDuck = info.get(rootId).subDuck;
        double costAllDf = info.get(rootId).subDf;

        double bestCost = costAllDuck;
        boolean bestHasCut = false;
        int bestCutId = -1;
        Engine bestQ1 = Engine.DUCKDB;
        Engine bestQ2 = Engine.DUCKDB;

        if (costAllDf < bestCost) {
            bestCost = costAllDf;
            bestQ1 = Engine.DATAFUSION;
            bestQ2 = Engine.DATAFUSION;
        }

        // try one cut at every non-root node, both directions
        for (NodeInfo x : info.values()) {
            if (x.id == rootId) continue;

            // base DUCKDB, subtree DATAFUSION, receiver=DUCKDB
            double cand1 = evalCutMax(x.id, Engine.DUCKDB, Engine.DATAFUSION, info)
                         + transferMsForCutNode(x.id, Engine.DUCKDB, featuresById);

            if (cand1 < bestCost) {
                bestCost = cand1;
                bestHasCut = true;
                bestCutId = x.id;
                bestQ1 = Engine.DATAFUSION;
                bestQ2 = Engine.DUCKDB;
            }

            // base DATAFUSION, subtree DUCKDB, receiver=DATAFUSION
            double cand2 = evalCutMax(x.id, Engine.DATAFUSION, Engine.DUCKDB, info)
                         + transferMsForCutNode(x.id, Engine.DATAFUSION, featuresById);

            if (cand2 < bestCost) {
                bestCost = cand2;
                bestHasCut = true;
                bestCutId = x.id;
                bestQ1 = Engine.DUCKDB;
                bestQ2 = Engine.DATAFUSION;
            }
        }

        return new PlanChoice(
            bestHasCut,
            bestCutId,
            bestQ1,
            bestQ2,
            costAllDuck,
            costAllDf,
            bestCost
        );
    }

    private double evalCutMax(int cutId,
                              Engine baseEngine,
                              Engine subtreeEngine,
                              Map<Integer, NodeInfo> info) {

        NodeInfo cut = info.get(cutId);
        double val = (subtreeEngine == Engine.DUCKDB) ? cut.subDuck : cut.subDf;

        int childId = cutId;
        int pId = cut.parentId;

        while (pId != -1) {
            NodeInfo p = info.get(pId);

            double self = (baseEngine == Engine.DUCKDB) ? p.selfDuck : p.selfDf;
            double siblingMax = maxSiblingExcluding(p, childId, baseEngine);

            val = self + Math.max(val, siblingMax);

            childId = pId;
            pId = p.parentId;
        }

        return val;
    }

    private double maxSiblingExcluding(NodeInfo parent, int childId, Engine base) {
        if (base == Engine.DUCKDB) {
            return (parent.maxChildDuckId == childId) ? parent.secondMaxChildDuck : parent.maxChildDuck;
        } else {
            return (parent.maxChildDfId == childId) ? parent.secondMaxChildDf : parent.maxChildDf;
        }
    }

    private void computeTop2Children(NodeInfo p, Map<Integer, NodeInfo> info) {
        p.maxChildDuckId = -1;
        p.maxChildDuck = 0.0;
        p.secondMaxChildDuck = 0.0;

        p.maxChildDfId = -1;
        p.maxChildDf = 0.0;
        p.secondMaxChildDf = 0.0;

        for (int cid : p.children) {
            NodeInfo c = info.get(cid);

            double vd = c.subDuck;
            if (vd >= p.maxChildDuck) {
                p.secondMaxChildDuck = p.maxChildDuck;
                p.maxChildDuck = vd;
                p.maxChildDuckId = cid;
            } else if (vd > p.secondMaxChildDuck) {
                p.secondMaxChildDuck = vd;
            }

            double vf = c.subDf;
            if (vf >= p.maxChildDf) {
                p.secondMaxChildDf = p.maxChildDf;
                p.maxChildDf = vf;
                p.maxChildDfId = cid;
            } else if (vf > p.secondMaxChildDf) {
                p.secondMaxChildDf = vf;
            }
        }
    }

    private void computeSubtreeMax(RelNode node,
                                   int parentId,
                                   Map<Integer, Map<Engine, Double>> runtimes,
                                   Map<Integer, NodeInfo> info) {

        int id = node.getId();
        NodeInfo ni = info.computeIfAbsent(id, NodeInfo::new);
        if (parentId != -1) {
            ni.parentId = parentId;
        }

        Map<Engine, Double> self = runtimes.get(id);
        if (self == null) {
            throw new IllegalStateException("Missing runtimes for nodeId=" + id);
        }

        Double sd = self.get(Engine.DUCKDB);
        Double sf = self.get(Engine.DATAFUSION);
        if (sd == null || sf == null) {
            throw new IllegalStateException("Missing per-engine runtimes for nodeId=" + id);
        }
        requireFiniteNonNegative("runtime(DUCKDB) nodeId=" + id, sd);
        requireFiniteNonNegative("runtime(DATAFUSION) nodeId=" + id, sf);

        ni.selfDuck = sd;
        ni.selfDf = sf;

        double maxChildDuck = 0.0;
        double maxChildDf = 0.0;

        for (RelNode ch : node.getInputs()) {
            int cid = ch.getId();
            ni.children.add(cid);

            computeSubtreeMax(ch, id, runtimes, info);

            NodeInfo ci = info.get(cid);
            maxChildDuck = Math.max(maxChildDuck, ci.subDuck);
            maxChildDf = Math.max(maxChildDf, ci.subDf);
        }

        ni.subDuck = ni.selfDuck + maxChildDuck;
        ni.subDf = ni.selfDf + maxChildDf;
    }

    // =========================================================================
    //  Transfer cost computation (STRICT)
    // =========================================================================

    private double transferMsForCutNode(int nodeId,
                                        Engine receiver,
                                        Map<Integer, OperatorFeatures> featuresById) {

        OperatorFeatures f = featuresById.get(nodeId);
        if (f == null) {
            throw new IllegalStateException("Missing OperatorFeatures for nodeId=" + nodeId);
        }

        requireFiniteNonNegative("outputRows nodeId=" + nodeId, f.outputRows);
        requireFinitePositive("rowSizeOutBytes nodeId=" + nodeId, f.rowSizeOutBytes);

        return transferEstimator.estimateMs(receiver, f.outputRows, f.rowSizeOutBytes);
    }

    private static void requireFiniteNonNegative(String name, double v) {
        if (!Double.isFinite(v) || v < 0.0) {
            throw new IllegalStateException(name + "must be finite and >= 0 (got " + v + ")");
        }
    }

    private static void requireFinitePositive(String name, double v) {
        if (!Double.isFinite(v) || v <= 0.0) {
            throw new IllegalStateException(name + "must be finite and > 0 (got " + v + ")");
        }
    }


    // getters for logging
    public TransferCostEstimator getTransferEstimator() {
        return transferEstimator;
    }

    public AggMode getMode() {
        return mode;
    }

}
