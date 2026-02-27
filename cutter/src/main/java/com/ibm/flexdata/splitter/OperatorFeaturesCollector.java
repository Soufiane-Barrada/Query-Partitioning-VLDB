package com.ibm.flexdata.splitter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;

import java.util.*;

public class OperatorFeaturesCollector {

    public static class OperatorFeatures {
        public int relId;
        public String relType;
        public OperatorKind operatorKind;

        // generic
        public double inputRows;          // sum of rows from children
        public double outputRows;         // rows produced by this node
        public double rowSizeInBytes;     // "main" input row size (unary) or 0
        public double rowSizeOutBytes;    // output row size

        // joins
        public double inputRowsLeft;
        public double inputRowsRight;
        public double rowSizeInLeftBytes;
        public double rowSizeInRightBytes;
        public String joinType;           // INNER, LEFT, RIGHT, FULL, etc.

        // aggregates
        public int groupKeyCount;
        public int aggCount;
        // number of aggregate calls that are DISTINCT (COUNT(DISTINCT ...), etc.)
        public int aggDistinctAggCount;

        // sort
        public int sortKeyCount;
        // true when this sort also encodes a LIMIT (i.e., fetch != null)
        public boolean hasLimit;

        // filters
        public int filterLogicalOpCount;  // number of AND/OR/NOT nodes
        public Set<String> filterOps = new HashSet<>(); // non-logical ops (>, =, LIKE, BETWEEN, ...)

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("OperatorFeatures{")
              .append("id=").append(relId)
              .append(", type=").append(relType)
              .append(", inputRows=").append(inputRows)
              .append(", outputRows=").append(outputRows)
              .append(", rowSizeInBytes=").append(rowSizeInBytes)
              .append(", rowSizeOutBytes=").append(rowSizeOutBytes);

            if (joinType != null) {
                sb.append(", joinType=").append(joinType)
                  .append(", inputRowsLeft=").append(inputRowsLeft)
                  .append(", inputRowsRight=").append(inputRowsRight)
                  .append(", rowSizeInLeftBytes=").append(rowSizeInLeftBytes)
                  .append(", rowSizeInRightBytes=").append(rowSizeInRightBytes);
            }

            if (groupKeyCount > 0 || aggCount > 0 || aggDistinctAggCount > 0) {
                sb.append(", groupKeyCount=").append(groupKeyCount)
                  .append(", aggCount=").append(aggCount)
                  .append(", aggDistinctAggCount=").append(aggDistinctAggCount);
            }

            if (sortKeyCount > 0 || hasLimit) {
                sb.append(", sortKeyCount=").append(sortKeyCount)
                  .append(", hasLimit=").append(hasLimit);
            }

            if (!filterOps.isEmpty() || filterLogicalOpCount > 0) {
                sb.append(", filterLogicalOpCount=").append(filterLogicalOpCount)
                  .append(", filterOps=").append(filterOps);
            }

            sb.append('}');
            return sb.toString();
        }
    }

    private final SoStatsRegistry statsRegistry;
    private final RelMetadataQuery mq;

    private final Map<Integer, OperatorFeatures> byId = new HashMap<>();
    private final Map<RelNode, Double> rowSizeOutCache = new IdentityHashMap<>();

    public OperatorFeaturesCollector(SoStatsRegistry statsRegistry) {
        this.statsRegistry = statsRegistry;
        this.mq = RelMetadataQuery.instance();
    }

    public Map<Integer, OperatorFeatures> collect(RelNode root) {
        computeRowSizeAndFeatures(root);
        return byId;
    }

    private double computeRowSizeAndFeatures(RelNode node) {
        Double cached = rowSizeOutCache.get(node);
        if (cached != null) {
            return cached;
        }

        List<RelNode> inputs = node.getInputs();
        double[] childSizes = new double[inputs.size()];
        double[] childRows  = new double[inputs.size()];
        for (int i = 0; i < inputs.size(); i++) {
            childSizes[i] = computeRowSizeAndFeatures(inputs.get(i));
            Double rows = mq.getRowCount(inputs.get(i));
            childRows[i] = (rows != null) ? rows : -1d;
        }

        OperatorFeatures f = new OperatorFeatures();
        f.relId = node.getId();
        f.relType = node.getRelTypeName();
        f.operatorKind = OperatorKind.OTHER;

        Double outRows = mq.getRowCount(node);
        f.outputRows = (outRows != null) ? outRows : -1d;

        double rowSizeOut;

        if (node instanceof LogicalTableScan) {
            f.operatorKind = OperatorKind.OTHER;
            LogicalTableScan scan = (LogicalTableScan) node;
            rowSizeOut = estimateScanRowSize(scan);
            f.rowSizeInBytes = 0d;
            f.rowSizeOutBytes = rowSizeOut;

            // Leaf: no children → inputRows=0
            f.inputRows = 0d;

        } else if (node instanceof LogicalProject) {
            f.operatorKind = OperatorKind.OTHER;
            LogicalProject proj = (LogicalProject) node;
            double childSize = childSizes.length > 0 ? childSizes[0] : 0d;
            double inRows    = childRows.length  > 0 ? childRows[0]  : -1d;

            // For now: projection doesn’t change width
            f.rowSizeInBytes = childSize;
            f.rowSizeOutBytes = childSize;
            rowSizeOut = childSize;

            f.inputRows = inRows;

        } else if (node instanceof LogicalFilter) {
            f.operatorKind = OperatorKind.FILTER;
            LogicalFilter filter = (LogicalFilter) node;
            double childSize = childSizes.length > 0 ? childSizes[0] : 0d;
            double inRows    = childRows.length  > 0 ? childRows[0]  : -1d;

            f.rowSizeInBytes = childSize;
            f.rowSizeOutBytes = childSize;
            rowSizeOut = childSize;

            f.inputRows = inRows;

            collectFilterOps(filter.getCondition(), f);

        } else if (node instanceof LogicalSort) {
            f.operatorKind = OperatorKind.SORT;
            LogicalSort sort = (LogicalSort) node;
            double childSize = childSizes.length > 0 ? childSizes[0] : 0d;
            double inRows    = childRows.length  > 0 ? childRows[0]  : -1d;

            f.rowSizeInBytes = childSize;
            f.rowSizeOutBytes = childSize;
            rowSizeOut = childSize;

            f.inputRows = inRows;

            f.sortKeyCount = sort.getCollation().getFieldCollations().size();
            // In Calcite, LIMIT is encoded as sort.fetch != null
            f.hasLimit = (sort.fetch != null);

        } else if (node instanceof LogicalAggregate) {
            f.operatorKind = OperatorKind.AGGREGATE;
            LogicalAggregate agg = (LogicalAggregate) node;
            double childSize = childSizes.length > 0 ? childSizes[0] : 0d;
            double inRows    = childRows.length  > 0 ? childRows[0]  : -1d;

            f.rowSizeInBytes = childSize;
            f.inputRows = inRows;

            f.groupKeyCount = agg.getGroupSet().cardinality();
            f.aggCount = agg.getAggCallList().size();

            // Count DISTINCT aggregates (“COUNT(DISTINCT ...)”, etc.)
            int distinctAggs = 0;
            for (org.apache.calcite.rel.core.AggregateCall ac : agg.getAggCallList()) {
                if (ac.isDistinct()) {
                    distinctAggs++;
                }
            }
            f.aggDistinctAggCount = distinctAggs;

            rowSizeOut = 8.0 * (f.groupKeyCount + f.aggCount);
            f.rowSizeOutBytes = rowSizeOut;

        } else if (node instanceof LogicalJoin) {
            f.operatorKind = OperatorKind.JOIN;
            LogicalJoin join = (LogicalJoin) node;

            double leftSize  = childSizes.length > 0 ? childSizes[0] : 0d;
            double rightSize = childSizes.length > 1 ? childSizes[1] : 0d;
            double leftRows  = childRows.length  > 0 ? childRows[0]  : -1d;
            double rightRows = childRows.length  > 1 ? childRows[1]  : -1d;

            f.rowSizeInLeftBytes = leftSize;
            f.rowSizeInRightBytes = rightSize;
            f.inputRowsLeft = leftRows;
            f.inputRowsRight = rightRows;
            f.inputRows = (leftRows > 0 ? leftRows : 0d)
                        + (rightRows > 0 ? rightRows : 0d);

            // join row width = left row + right row (semi/anti can be refined later)
            rowSizeOut = leftSize + rightSize;
            f.rowSizeOutBytes = rowSizeOut;

            f.joinType = join.getJoinType().name();

        } else {
            // Generic operator: pass-through size and sum input rows
            f.operatorKind = OperatorKind.OTHER;
            double childSize = childSizes.length > 0 ? childSizes[0] : 0d;
            f.rowSizeInBytes = childSize;
            f.rowSizeOutBytes = childSize;
            rowSizeOut = childSize;

            double sumIn = 0d;
            for (double cr : childRows) {
                if (cr > 0) sumIn += cr;
            }
            f.inputRows = sumIn;
        }

        byId.put(node.getId(), f);
        rowSizeOutCache.put(node, rowSizeOut);
        return rowSizeOut;
    }

    private void collectFilterOps(RexNode condition, OperatorFeatures f) {
        condition.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCall(RexCall call) {
                String opName = call.getOperator().getName().toUpperCase(Locale.ROOT);
                switch (opName) {
                    case "AND":
                    case "OR":
                    case "NOT":
                        f.filterLogicalOpCount++;
                        break;
                    default:
                        // non-logical operator used in this filter (e.g., =, >, <, LIKE, BETWEEN)
                        f.filterOps.add(opName);
                        break;
                }
                return super.visitCall(call);
            }
        });
    }

    private double estimateScanRowSize(LogicalTableScan scan) {
        // Qualified name is typically [schema, table]
        java.util.List<String> qn = scan.getTable().getQualifiedName();
        if (qn.isEmpty()) {
            return 0d;
        }
        String tableName = qn.get(qn.size() - 1).toLowerCase(Locale.ROOT);
        SoStats.TableStats ts = statsRegistry.getTableStats(tableName);
        if (ts == null || ts.columns == null) {
            return 0d;
        }

        double total = 0d;
        for (RelDataTypeField field : scan.getRowType().getFieldList()) {
            String colName = field.getName().toLowerCase(Locale.ROOT);
            SoStats.ColumnStats cs = ts.getColumn(colName);
            if (cs != null) {
                total += cs.column_size_bytes; // average per-value size from JSON
            }
        }
        return total;
    }

    public Map<Integer, OperatorFeatures> getById() {
        return byId;
    }
}
