package com.ibm.flexdata.splitter;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class FeatureVectorBuilder {

    private FeatureVectorBuilder() {}

    /**
     * Build a feature map for the given operator kind.
     * Returns null if this kind is not modeled (OTHER).
     */
    public static Map<String, Object> build(OperatorKind kind, OperatorFeatures f) {
        switch (kind) {
            case JOIN:
                return buildJoinFeatures(f);
            case FILTER:
                return buildFilterFeatures(f);
            case AGGREGATE:
                return buildAggregateFeatures(f);
            case SORT:
                return buildSortFeatures(f);
            default:
                // no ML model for OTHER nodes (TableScan, Project, etc.)
                return null;
        }
    }

    // --------------------- helpers ------------------------------------------

    private static double safeNonNegative(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v) || v < 0.0) {
            return 0.0d;
        }
        return v;
    }

    private static boolean hasOp(Set<String> ops, String opName) {
        if (ops == null || ops.isEmpty()) return false;
        String target = opName.toUpperCase(Locale.ROOT);
        return ops.contains(target);
    }

    // --------------------- JOIN features ------------------------------------

    private static Map<String, Object> buildJoinFeatures(OperatorFeatures f) {
        Map<String, Object> m = new LinkedHashMap<>();

        double inputLeft  = safeNonNegative(f.inputRowsLeft);
        double inputRight = safeNonNegative(f.inputRowsRight);
        double output     = safeNonNegative(f.outputRows);
        double sizeLeft   = safeNonNegative(f.rowSizeInLeftBytes);
        double sizeRight  = safeNonNegative(f.rowSizeInRightBytes);

        m.put("input_rows_left",  inputLeft);
        m.put("input_rows_right", inputRight);
        m.put("output_rows",      output);
        m.put("row_size_right",   sizeRight);
        m.put("row_size_left",    sizeLeft);

        String jt = (f.joinType == null) ? "" :
                    f.joinType.toUpperCase(Locale.ROOT).trim();

        m.put("jt_cross", jt.equals("CROSS") ? 1 : 0);
        m.put("jt_full",  jt.equals("FULL")  ? 1 : 0);
        m.put("jt_inner", jt.equals("INNER") ? 1 : 0);
        m.put("jt_left",  jt.equals("LEFT")  ? 1 : 0);
        m.put("jt_right", jt.equals("RIGHT") ? 1 : 0);
        m.put("jt_semi",  jt.equals("SEMI")  ? 1 : 0);

        return m;
    }

    // --------------------- FILTER features ----------------------------------

    private static Map<String, Object> buildFilterFeatures(OperatorFeatures f) {
        Map<String, Object> m = new LinkedHashMap<>();

        double input  = safeNonNegative(f.inputRows);
        double output = safeNonNegative(f.outputRows);
        double width  = safeNonNegative(f.rowSizeInBytes);

        m.put("input_rows",         input);
        m.put("output_rows",        output);
        m.put("row_size_in",        width);
        m.put("logical_ops_count",  f.filterLogicalOpCount);

        Set<String> ops = (f.filterOps != null) ? f.filterOps : Set.of();

        m.put("op_between",     hasOp(ops, "BETWEEN")      ? 1 : 0);
        m.put("op_eq",          hasOp(ops, "=")            ? 1 : 0);
        m.put("op_ge",          hasOp(ops, ">=")           ? 1 : 0);
        m.put("op_gt",          hasOp(ops, ">")            ? 1 : 0);
        m.put("op_in",          hasOp(ops, "IN")           ? 1 : 0);
        m.put("op_is_not_null", hasOp(ops, "IS NOT NULL")  ? 1 : 0);
        m.put("op_is_null",     hasOp(ops, "IS NULL")      ? 1 : 0);
        m.put("op_le",          hasOp(ops, "<=")           ? 1 : 0);
        m.put("op_lt",          hasOp(ops, "<")            ? 1 : 0);
        m.put("op_ne",          hasOp(ops, "<>") || hasOp(ops, "!=") ? 1 : 0);

        return m;
    }

    // --------------------- AGGREGATE features -------------------------------

    private static Map<String, Object> buildAggregateFeatures(OperatorFeatures f) {
        Map<String, Object> m = new LinkedHashMap<>();

        double input  = safeNonNegative(f.inputRows);
        double output = safeNonNegative(f.outputRows);
        double width  = safeNonNegative(f.rowSizeInBytes);

        m.put("input_rows",        input);
        m.put("output_rows",       output);
        m.put("row_size_in",       width);
        m.put("group_keys_count",  f.groupKeyCount);
        m.put("aggregation_count", f.aggCount);
        m.put("distinct_count",    f.aggDistinctAggCount);

        return m;
    }

    // --------------------- SORT features ------------------------------------

    private static Map<String, Object> buildSortFeatures(OperatorFeatures f) {
        Map<String, Object> m = new LinkedHashMap<>();

        double input  = safeNonNegative(f.inputRows);
        double output = safeNonNegative(f.outputRows);
        double width  = safeNonNegative(f.rowSizeInBytes);

        m.put("input_rows",      input);
        m.put("row_size_in",     width);
        m.put("sort_keys_count", f.sortKeyCount);

        // Training: is_followed_by_limit = (output_rows < input_rows)
        int isLimit = (output > 0.0 && input > 0.0 && output < input) ? 1 : 0;
        m.put("is_followed_by_limit", isLimit);

        return m;
    }
}
