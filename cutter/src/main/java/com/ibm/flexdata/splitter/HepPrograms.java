package com.ibm.flexdata.splitter;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;

public final class HepPrograms {

    private HepPrograms() {}

    public static HepProgram defaultProgram() {
        return bushyProgram();
    }

    public static HepProgram leftDeepProgram() {
        HepProgramBuilder builder = new HepProgramBuilder();

        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

        // ---- Phase 0: expand subqueries to correlates ----
        builder.addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        builder.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        builder.addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);

        // ---- Phase 1: local simplifications ----
        builder.addGroupBegin();
        builder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        builder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        builder.addRuleInstance(CoreRules.PROJECT_REMOVE);
        builder.addGroupEnd();

        // ---- Phase 2: push filters & projects down ----
        builder.addGroupBegin();
        builder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
        builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
        builder.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
        builder.addRuleInstance(CoreRules.PROJECT_MERGE);
        builder.addGroupEnd();

        // ---- Phase 3: build MultiJoin ----
        builder.addGroupBegin();
        builder.addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN);
        builder.addRuleInstance(CoreRules.FILTER_MULTI_JOIN_MERGE);
        builder.addRuleInstance(CoreRules.PROJECT_MULTI_JOIN_MERGE);
        builder.addGroupEnd();

        // ---- Phase 4: join reordering (LEFT-DEEP ONLY) ----
        builder.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE);

        return builder.build();
    }



    /** Optimization program that allows BUSHY join trees. */
    public static HepProgram bushyProgram() {
        HepProgramBuilder b = new HepProgramBuilder();

        // Safety net against pathological rule explosions
        b.addMatchLimit(20000);

        // ===== 1) Subqueries → correlates + simplifications =====
        b.addMatchOrder(HepMatchOrder.BOTTOM_UP);

        // simplificationRules()
        b.addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS);
        b.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS);

        // subqueryRules()
        b.addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        b.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        b.addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);

        // ===== 2) Table project (PROJECT_TABLE_SCAN) + simplification =====
        b.addMatchOrder(HepMatchOrder.BOTTOM_UP);

        // simplificationRules() again
        b.addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS);
        b.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS);

        // tableProjectRules()
        b.addRuleInstance(CoreRules.PROJECT_TABLE_SCAN);

        // ===== 3) Predicate pushdown + simplification =====
        b.addMatchOrder(HepMatchOrder.BOTTOM_UP);

        // simplificationRules() again
        b.addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS);
        b.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS);
        b.addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS);

        // filterRules()
        b.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        b.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
        b.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        b.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
        b.addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE);
        b.addRuleInstance(CoreRules.FILTER_MERGE);

        // ===== 4) Join ordering: bushy + left-deep fallback =====
        b.addMatchOrder(HepMatchOrder.BOTTOM_UP);

        // MultiJoin-based reordering + fallback
        b.addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN);
        b.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);
        b.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE); // fallback

        // ===== 5) Final Flink-style logical cleanups =====
        b.addMatchOrder(HepMatchOrder.ARBITRARY);

        // filterRules() again
        b.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        b.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
        b.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        b.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
        b.addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE);
        b.addRuleInstance(CoreRules.FILTER_MERGE);

        // projectRules()
        b.addRuleInstance(CoreRules.PROJECT_MERGE);
        b.addRuleInstance(CoreRules.PROJECT_REMOVE);
        b.addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
        b.addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE);

        // extras
        b.addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE);
        b.addRuleInstance(CoreRules.SORT_REMOVE);
        b.addRuleInstance(CoreRules.UNION_REMOVE);
        b.addRuleInstance(CoreRules.UNION_TO_DISTINCT);
        b.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
        b.addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
        b.addRuleInstance(CoreRules.AGGREGATE_UNION_AGGREGATE);

        return b.build();
    }


    
    // public static HepProgram bushyProgram() {
    //     HepProgramBuilder builder = new HepProgramBuilder();

    //     builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    //     // ---- Phase 1: local simplifications ----
    //     builder.addGroupBegin();
    //     builder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    //     builder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
    //     builder.addRuleInstance(CoreRules.PROJECT_REMOVE);
    //     builder.addGroupEnd();

    //     // ---- Phase 2: push filters & projects down ----
    //     builder.addGroupBegin();
    //     builder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
    //     builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
    //     builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
    //     builder.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
    //     builder.addRuleInstance(CoreRules.PROJECT_MERGE);
    //     builder.addGroupEnd();

    //     // ---- Phase 3: bushy join reordering on LogicalJoin ----
    //     builder.addGroupBegin();
    //     // Associate: (A ⋈ B) ⋈ C  <->  A ⋈ (B ⋈ C)
    //     builder.addRuleInstance(CoreRules.JOIN_ASSOCIATE);
    //     // Commute: A ⋈ B  <->  B ⋈ A
    //     builder.addRuleInstance(CoreRules.JOIN_COMMUTE);
    //     // Push join through join (can create/reshape bushy trees)
    //     builder.addRuleInstance(JoinPushThroughJoinRule.LEFT);
    //     builder.addRuleInstance(JoinPushThroughJoinRule.RIGHT);

    //     builder.addGroupEnd();

    //     return builder.build();
    // }





    // public static HepProgram bushyProgram() {
    //     HepProgramBuilder builder = new HepProgramBuilder();

    //     builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    //     // ---- Phase 1: local simplifications ----
    //     builder.addGroupBegin();
    //     builder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    //     builder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
    //     builder.addRuleInstance(CoreRules.PROJECT_REMOVE);
    //     builder.addGroupEnd();

    //     // ---- Phase 2: push filters & projects down ----
    //     builder.addGroupBegin();
    //     builder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
    //     builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
    //     builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
    //     builder.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
    //     builder.addRuleInstance(CoreRules.PROJECT_MERGE);
    //     builder.addGroupEnd();

    //     // ---- Phase 3: build MultiJoin ----
    //     builder.addGroupBegin();
    //     builder.addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN);
    //     builder.addRuleInstance(CoreRules.FILTER_MULTI_JOIN_MERGE);
    //     builder.addRuleInstance(CoreRules.PROJECT_MULTI_JOIN_MERGE);
    //     builder.addGroupEnd();

    //     // ---- Phase 4: join reordering (BUSHY ALLOWED) ----
    //     builder.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);

    //     return builder.build();
    // }
}
