package com.ibm.flexdata.splitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Locale;


import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class PlanCutter {

    public static class SplitPlan {
        private final RelNode q1Rel;
        private final RelNode q2Rel;
        private final String q1Sql;
        private final String q2Sql;

        public SplitPlan(RelNode q1Rel, RelNode q2Rel,
                         String q1Sql, String q2Sql) {
            this.q1Rel = q1Rel;
            this.q2Rel = q2Rel;
            this.q1Sql = q1Sql;
            this.q2Sql = q2Sql;
        }

        public RelNode q1Rel() { return q1Rel; }
        public RelNode q2Rel() { return q2Rel; }
        public String q1Sql() { return q1Sql; }
        public String q2Sql() { return q2Sql; }
    }

    private final SqlDialect dialect;

    public PlanCutter(SqlDialect dialect) {
        this.dialect = dialect;
    }

    public SplitPlan cut(RelNode optimizedRoot, int cutNodeId) {
        // 1) Find the cut node
        FindByIdVisitor finder = new FindByIdVisitor(cutNodeId);
        finder.go(optimizedRoot);
        RelNode cutNode = finder.result;
        if (cutNode == null) {
            throw new IllegalArgumentException(
                "Could not find node with id " + cutNodeId);
        }

        // Q1 is simply the subtree rooted at cutNode
        // For SQL, wrap in an identity project so column aliases match rowType exactly.
        RelNode q1Rel = addIdentityProject(cutNode);

        // 2) Build s1 scan with same row type as cutNode
        RelOptCluster cluster = optimizedRoot.getCluster();
        RelDataType s1RowType = q1Rel.getRowType();

        Table s1Table = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return s1RowType;
            }
        };

        RelOptSchema relOptSchema =
            cluster.getPlanner().getContext().unwrap(RelOptSchema.class);

        RelOptTableImpl s1RelOptTable =
            RelOptTableImpl.create(
                relOptSchema, s1RowType, s1Table, ImmutableList.of("s1"));

        RelNode s1Scan =
            LogicalTableScan.create(cluster, s1RelOptTable,
                                    ImmutableList.<RelHint>of());

        // 3) Rewrite the original plan: replace cutNode with s1Scan
        CutRewriter rewriter = new CutRewriter(cutNodeId, s1Scan);
        RelNode q2Raw = optimizedRoot.accept(rewriter);
        // Q2: also wrapped in identity project for stable, sanitized names
        RelNode q2Rel = addIdentityProject(q2Raw);

        // 4) Convert both subplans back to SQL
        RelToSqlConverter toSql = new RelToSqlConverter(dialect);

        SqlNode q1SqlNode = toSql.visitRoot(q1Rel).asStatement();
        String q1Sql = q1SqlNode.toSqlString(dialect).getSql();

        SqlNode q2SqlNode = toSql.visitRoot(q2Rel).asStatement();
        String q2Sql = q2SqlNode.toSqlString(dialect).getSql();

        return new SplitPlan(q1Rel, q2Rel, q1Sql, q2Sql);
    }

    public void writeSqlFiles(SplitPlan plan,
                              Path outputDir,
                              String baseName,
                              Engine q1Engine,
                              Engine q2Engine) throws IOException {
        Files.createDirectories(outputDir);

        String q1Suffix = "_" + q1Engine.name().toLowerCase();
        String q2Suffix = "_" + q2Engine.name().toLowerCase();

        Path q1Path = outputDir.resolve(baseName + "_q1" + q1Suffix + ".sql");
        Path q2Path = outputDir.resolve(baseName + "_q2" + q2Suffix + ".sql");

        Files.writeString(q1Path, plan.q1Sql(), StandardCharsets.UTF_8);
        Files.writeString(q2Path, plan.q2Sql(), StandardCharsets.UTF_8);
    }

    // ------------------------
    // Helper visitors

    static class FindByIdVisitor extends RelVisitor {
        private final int targetId;
        RelNode result;

        FindByIdVisitor(int targetId) {
            this.targetId = targetId;
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node.getId() == targetId) {
                result = node;
                // Stop descending
            } else {
                super.visit(node, ordinal, parent);
            }
        }
    }

    static class CutRewriter extends RelShuttleImpl {
        private final int targetId;
        private final RelNode replacement;

        CutRewriter(int targetId, RelNode replacement) {
            this.targetId = targetId;
            this.replacement = replacement;
        }

        private RelNode helperVisit(RelNode other) {
            if (other.getId() == targetId) {
                return replacement;
            }
            return super.visit(other);
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            return helperVisit(aggregate);
        }

        @Override
        public RelNode visit(LogicalJoin join){
            return helperVisit(join);
        }

        @Override
        public RelNode visit(LogicalFilter filter){
            return helperVisit(filter);
        }

        @Override
        public RelNode visit(LogicalProject project){
            return helperVisit(project);
        }

        @Override
        public RelNode visit(LogicalSort sort){
            return helperVisit(sort);
        }

        @Override
        public RelNode visit(LogicalUnion union){
            return helperVisit(union);
        }

        @Override
        public RelNode visit(TableScan scan){
            return helperVisit(scan);
        }

        @Override
        public RelNode visit(RelNode other) {
            return helperVisit(other);
        }
    }

    public static class IdPrinter extends RelVisitor {
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            System.out.println(
                "Node id=" + node.getId() + ", type=" + node.getRelTypeName());
            super.visit(node, ordinal, parent);
        }
    }





    private static String sanitizeFieldName(
            String originalName,
            int index,
            Set<String> usedLowerNames) {

        String name = originalName;

        // 1) Fix empty / null names
        if (name == null || name.isEmpty()) {
            name = "FD_COL_" + index;
        }

        // 2) Avoid DataFusion placeholders: names starting with '$', names like EXPR$0
        if (!name.isEmpty()) {
            String upper = name.toUpperCase(Locale.ROOT);
            if (name.charAt(0) == '$' || upper.startsWith("EXPR$")) {
                name = "FD_COL_" + index;
            }
        }

        // 3) Ensure uniqueness in this row type ignoring case*
        String candidate = name;
        String lower = candidate.toLowerCase(Locale.ROOT);

        while (usedLowerNames.contains(lower)) {
            candidate = candidate + "_";
            lower = candidate.toLowerCase(Locale.ROOT);
        }

        usedLowerNames.add(lower);
        return candidate;
    }




   private RelNode addIdentityProject(RelNode rel) {
        RelOptCluster cluster = rel.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType rowType = rel.getRowType();

        int fieldCount = rowType.getFieldCount();
        java.util.List<RexNode> projects = new java.util.ArrayList<>(fieldCount);
        java.util.List<String> names   = new java.util.ArrayList<>(fieldCount);

        Set<String> usedLowerNames = new HashSet<>();

        for (int i = 0; i < fieldCount; i++) {
            RelDataTypeField f = rowType.getFieldList().get(i);

            // Base input ref
            RexNode inputRef = rexBuilder.makeInputRef(rel, i);

            RexNode expr;
            if (i == 0) {
                // Make the first column non-trivial (but type-preserving):
                expr = rexBuilder.makeCall(
                    SqlStdOperatorTable.COALESCE, inputRef, inputRef
                );
            } else {
                // Others are plain input refs
                expr = inputRef;
            }

            projects.add(expr);

            String rawName = f.getName();
            String sanitized = sanitizeFieldName(rawName, i, usedLowerNames);
            names.add(sanitized);
        }

        return LogicalProject.create(
            rel,
            com.google.common.collect.ImmutableList.of(), // no hints
            projects,
            names
        );
    }




}
