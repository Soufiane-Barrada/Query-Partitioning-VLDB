package com.ibm.flexdata.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Locale;
import java.util.Collections;
import java.util.LinkedHashMap;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.plan.RelOptUtil;

import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;

import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public class QuerySplitPipeline {

    private final Path sqlDir;
    private final Path outputDir;
    private final CalciteEnvironment calciteEnv;
    private final PlanOptimizer optimizer;
    private final SoStatsRegistry statsRegistry;
    private final RuntimeEstimator runtimeEstimator;
    private final DpCutFinder cutFinder;
    private final PlanCutter planCutter;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final boolean doOptimize;
    private final String runId;
    private final String metadataProviderName;
    private final DpCutFinder.AggMode dpMode;
    private final TransferCostEstimator transferEstimator;
    private final CutPolicyMode cutPolicy;
    private final long randomSeed;
    private final Random rng;
    private final Dataset dataset;
    private final boolean verbosePlans;

    private Map<Integer, OperatorFeatures> curFeaturesById = null;
    private Map<Integer, Map<Engine, Double>> curRuntimesById = null;



    /** Result of attempting to split a single query (w.r.t. produced SQL files). */
    private enum SplitResult {
        CUT,
        NO_BENEFICIAL_CUT
    }

    private static final class StepTimings {
        long parseMs;
        long optMs;
        long mlMs;
        long dpMs;
        long cutMs;

        long totalMs() {
            return parseMs + optMs + mlMs + dpMs + cutMs;
        }
    }


    public QuerySplitPipeline(Path sqlDir,
                          Path outputDir,
                          CalciteEnvironment calciteEnv,
                          PlanOptimizer optimizer,
                          SoStatsRegistry statsRegistry,
                          RuntimeEstimator runtimeEstimator,
                          DpCutFinder cutFinder,
                          PlanCutter planCutter,
                          boolean doOptimize,
                          String runId,
                          String metadataProviderName,
                          DpCutFinder.AggMode dpMode,
                          CutPolicyMode cutPolicy,
                          long randomSeed,
                          Dataset dataset,
                          boolean verbosePlans) {
        this.sqlDir = sqlDir;
        this.outputDir = outputDir;
        this.calciteEnv = calciteEnv;
        this.optimizer = optimizer;
        this.statsRegistry = statsRegistry;
        this.runtimeEstimator = runtimeEstimator;
        this.cutFinder = cutFinder;
        this.planCutter = planCutter;
        this.doOptimize = doOptimize;

        this.runId = runId;
        this.metadataProviderName = metadataProviderName;
        this.dpMode = dpMode;
        this.transferEstimator = cutFinder.getTransferEstimator();

        this.cutPolicy = (cutPolicy == null) ? CutPolicyMode.DP : cutPolicy;
        this.randomSeed = randomSeed;
        this.rng = new Random(randomSeed);
        this.dataset = (dataset == null) ? Dataset.STACKOVERFLOW : dataset;
        this.verbosePlans = verbosePlans;
    }


    public void run() throws Exception {
        SqlQueryLoader loader = new SqlQueryLoader(sqlDir);

        initTimingsCsv();

        int totalQueries = 0;
        int failedQueries = 0;
        int failedUnexpected = 0;

        List<String> failedNames = new ArrayList<>();
        List<String> acceptedCutNames = new ArrayList<>();
        List<String> acceptedNoCutNames = new ArrayList<>();

        for (SqlQueryLoader.SqlQuery q : loader.loadQueries()) {
            totalQueries++;

            System.out.println("\n========================================");
            System.out.println("Processing query: " + q.baseName());
            System.out.println("========================================");

            boolean success = false;
            StepTimings timings = new StepTimings();

            try {
                SplitResult result = processSingleQuery(q, timings);
                success = true;

                if (result == SplitResult.CUT) {
                    acceptedCutNames.add(q.baseName());
                } else {
                    acceptedNoCutNames.add(q.baseName());
                }

            } catch (SqlParseException | ValidationException e) {
                failedQueries++;
                failedNames.add(q.baseName());

                System.out.println("[WARNING] Skipping query " + q.baseName()
                    + " due to parse/validation error:");
                System.out.println("  " + e.getMessage());

            } catch (AssertionError e) {
                // planner assertion errors (e.g., MultiJoinOptimizeBushy)
                failedQueries++;
                failedUnexpected++;
                failedNames.add(q.baseName() + " (AssertionError in optimizer)");

                System.out.println("[WARNING] Skipping query " + q.baseName()
                    + " due to planner assertion: " + e.getMessage());
                e.printStackTrace(System.out);

            } catch (Exception e) {
                failedQueries++;
                failedUnexpected++;
                failedNames.add(q.baseName() + " (Unexpected)");

                System.out.println("[WARNING] Skipping query " + q.baseName()
                    + " due to unexpected error: " + e.toString());
                e.printStackTrace(System.out);
            } finally {
                long totalMs = success ? timings.totalMs() : 0L;
                appendTimingRow(
                    q.baseName(),
                    success,
                    totalMs,
                    timings.parseMs,
                    timings.optMs,
                    timings.mlMs,
                    timings.dpMs,
                    timings.cutMs
                );
            }

            // Write status file after each query
            writeStatusFile(
                totalQueries,
                failedQueries,
                failedUnexpected,
                acceptedCutNames,
                acceptedNoCutNames,
                failedNames
            );
        }

        int successful = totalQueries - failedQueries;

        // ---- Final summary ----
        System.out.println("\n========================================");
        System.out.println("Query processing summary");
        System.out.println("========================================");
        System.out.println("Total queries                 : " + totalQueries);
        System.out.println("Successful queries (overall)  : " + successful);
        System.out.println("  ├─ with cut                 : " + acceptedCutNames.size());
        System.out.println("  └─ accepted but not cut     : " + acceptedNoCutNames.size());
        System.out.println("Failed queries                : " + failedQueries);
        System.out.println("Failed queries UNEXPECTED     : " + failedUnexpected);

        if (!failedNames.isEmpty()) {
            System.out.println("\nFailed query files:");
            for (String name : failedNames) {
                System.out.println("  - " + name);
            }
        }
        System.out.println("========================================");
    }

    private SplitResult processSingleQuery(SqlQueryLoader.SqlQuery query,
                                           StepTimings timings) throws Exception {

        final long t0 = System.nanoTime();

        // 1) SQL -> Calcite rel tree
        String originalSql = query.sql();
        RelRoot root = calciteEnv.sqlToRelRoot(query.sql());
        RelNode logical = root.rel;

        // 2) Optimize (Optional)
        RelRoot planRoot;
        RelNode plan;

        final long tBeforeOpt = System.nanoTime();

        if (doOptimize) {
            // this also runs SUB_QUERY_TO_CORRELATE etc.
            long optStart = System.nanoTime();
            planRoot = optimizer.optimize(root);
            plan = planRoot.rel;
            long optEnd = System.nanoTime();
            timings.optMs = TimeUnit.NANOSECONDS.toMillis(optEnd - optStart);

        } else {
            // IMPORTANT: still set the metadata provider, otherwise rowcount/size may degrade
            root.rel.getCluster().setMetadataProvider(optimizer.getMetadataProvider());

            planRoot = root;
            plan = logical;

            timings.optMs = 0L;
        }

        timings.parseMs = TimeUnit.NANOSECONDS.toMillis(tBeforeOpt - t0);

        // 3) Decorrelate
        RelBuilder relBuilder = RelBuilder.create(calciteEnv.getFrameworkConfig());
        RelNode decorrelated = RelDecorrelator.decorrelateQuery(plan, relBuilder);
        RelRoot decorrelatedRoot = RelRoot.of(decorrelated, root.kind);

        // make sure decorrelated plan also uses the custom provider
        decorrelated.getCluster().setMetadataProvider(optimizer.getMetadataProvider());

        // Optional safety: bail out if for some reason we still see $cor*
        String decorStr = RelOptUtil.toString(decorrelated);
        if (decorStr.contains("$cor")) {
            System.out.println("[WARNING] Plan still contains correlation ($cor*). "
                + "Skipping split for query " + query.baseName());
            throw new Exception("Plan still contains correlation");
        }

        if (verbosePlans) {
            System.out.println("=== Logical plan (raw) ===");
            System.out.println(RelOptUtil.toString(logical));

            if (doOptimize) {
                System.out.println("=== Optimized plan (before decorrelation) ===");
                System.out.println(RelOptUtil.toString(plan));
            } else {
                System.out.println("=== Optimization disabled: using raw logical plan ===");
            }

            System.out.println("=== Decorrelated plan ===");
            System.out.println(decorStr);
        }

        // 4) Annotate operators (features) on decorrelated plan
        OperatorFeaturesCollector collector =
            new OperatorFeaturesCollector(statsRegistry);
        Map<Integer, OperatorFeatures> featuresById =
            collector.collect(decorrelated);

        if (verbosePlans) {
            System.out.println("=== Operator features ===");
            for (OperatorFeatures f : featuresById.values()) {
                System.out.println(f);
            }
        }

        // 5) ML inference: runtimes per operator & engine
        long mlStart = System.nanoTime();
        var runtimesById = runtimeEstimator.estimateRuntimes(featuresById);
        long mlEnd = System.nanoTime();
        timings.mlMs = TimeUnit.NANOSECONDS.toMillis(mlEnd - mlStart);

        this.curFeaturesById = featuresById;
        this.curRuntimesById = runtimesById;

        // 6) DP baseline costs (and DP plan if in DP mode)
        long dpStart = System.nanoTime();
        DpCutFinder.PlanChoice dpChoice =
            cutFinder.findBestPlan(decorrelated, runtimesById, featuresById);
        long dpEnd = System.nanoTime();
        timings.dpMs = TimeUnit.NANOSECONDS.toMillis(dpEnd - dpStart);

        DpCutFinder.PlanChoice choice;

        long cutStart = System.nanoTime();

        if (cutPolicy == CutPolicyMode.RANDOM) {
            // Random engines (CROSS-ENGINE by construction)
            Engine q1 = randomEngine();
            Engine q2 = otherEngine(q1);

            int cutId = pickRandomCoreCutId(decorrelated);

            if (cutId < 0) {
                // No eligible core op -> treat as not cut, run whole query on a random engine
                Engine single = randomEngine();
                double cost = (single == Engine.DUCKDB) ? dpChoice.costAllDuckdb : dpChoice.costAllDatafusion;

                choice = new DpCutFinder.PlanChoice(
                    false, -1,
                    single, single,
                    dpChoice.costAllDuckdb, dpChoice.costAllDatafusion,
                    cost
                );
            } else {
                //  lift immediately so the recorded cut is the one we actually execute
                int lifted = liftCutOverProjects(decorrelated, cutId);

                // We do not care about gains in RANDOM mode.
                // But we still produce a consistent chosenCost estimate for logging.
                double est = -1;

                choice = new DpCutFinder.PlanChoice(
                    true, lifted,
                    q1, q2,
                    dpChoice.costAllDuckdb, dpChoice.costAllDatafusion,
                    est
                );
            }

        } else {
            // DP mode: use DP decision
            choice = dpChoice;
        }


        DpCutFinder.PlanChoice finalChoice = choice;
        PlanCutter.SplitPlan mainSplitPlan = null;
        String mainQ1SqlNormalized = null;
        String mainQ2SqlNormalized = null;

        if (!choice.hasCut) {
            if (verbosePlans) {
                System.out.println("[INFO] No beneficial cut. Run entire query on " + choice.q1Engine);
            }
        } else {
            int liftedCutId = liftCutOverProjects(decorrelated, choice.cutNodeId);
            choice.cutNodeId = liftedCutId;

            // 7) Cut the decorrelated plan into Q1 and Q2
            PlanCutter.SplitPlan splitPlan = planCutter.cut(decorrelated, choice.cutNodeId);

            // Check if both subqueries have at least one core op (Filter/Sort/Join/Aggregate)
            boolean q1HasCore = hasCoreOperator(splitPlan.q1Rel());
            boolean q2HasCore = hasCoreOperator(splitPlan.q2Rel());

            if (!(q1HasCore && q2HasCore)) {
                // Choose best single-engine plan (no cross-engine split)
                Engine singleEngine;
                double singleCost;
                if (choice.costAllDuckdb <= choice.costAllDatafusion) {
                    singleEngine = Engine.DUCKDB;
                    singleCost = choice.costAllDuckdb;
                } else {
                    singleEngine = Engine.DATAFUSION;
                    singleCost = choice.costAllDatafusion;
                }

                // Final choice: no cut, but keep original all-DuckDB/all-Datafusion costs
                finalChoice =
                    new DpCutFinder.PlanChoice(
                         false,
                         -1,
                         singleEngine,
                        singleEngine,
                        choice.costAllDuckdb,
                        choice.costAllDatafusion,
                        singleCost
                    );
                if (verbosePlans) {
                    System.out.println("[INFO] Cut is trivial: at least one subquery has no "
                        + "Filter/Sort/Join/Aggregate. Treating as NOT cut.");
                }
            } else {
                // Real cut: both Q1 and Q2 have at least one core op
                mainSplitPlan = splitPlan;

                String q1Raw = splitPlan.q1Sql();
                String q2Raw = splitPlan.q2Sql();
                // Normalize SQL for execution engines (DuckDB / DataFusion)
                mainQ1SqlNormalized = SqlPostProcessor.normalize(
                    q1Raw, choice.q1Engine, dataset.getSchemaName());
                mainQ2SqlNormalized = SqlPostProcessor.normalize(
                    q2Raw, choice.q2Engine, dataset.getSchemaName());

                if (verbosePlans) {
                    System.out.println("=== DP decision summary ===");
                    System.out.println("  Cost if all DUCKDB      : " + choice.costAllDuckdb);
                    System.out.println("  Cost if all DATAFUSION  : " + choice.costAllDatafusion);
                    System.out.println("  Chosen plan cost        : " + choice.chosenCost);
                    System.out.println("[INFO] Best cut node id    : " + choice.cutNodeId);
                    System.out.println("       Q1 engine (subtree) : " + choice.q1Engine);
                    System.out.println("       Q2 engine (rest)    : " + choice.q2Engine);
                    System.out.println("=== Q1 SQL (" + choice.q1Engine + ") ===");
                    System.out.println(mainQ1SqlNormalized);
                    System.out.println("=== Q2 SQL (" + choice.q2Engine + ") ===");
                    System.out.println(mainQ2SqlNormalized);
                }

                // 8) Write to files, with engine labels in filenames
                planCutter.writeSqlFiles(
                    splitPlan,
                    outputDir,
                    query.baseName(),
                    choice.q1Engine,
                    choice.q2Engine
                );
            }
        }

        long cutEnd = System.nanoTime();
        timings.cutMs = TimeUnit.NANOSECONDS.toMillis(cutEnd - cutStart);

        List<CutPlanSpec> cutPlans =
            buildCutPlanSpecs(decorrelated, finalChoice, mainSplitPlan,
                              mainQ1SqlNormalized, mainQ2SqlNormalized);

        // 9) Write JSON DAG (main + exhaustive cuts)
        writeJsonPlan(
            query,
            originalSql,
            finalChoice,
            decorrelated,
            featuresById,
            mainSplitPlan,
            mainQ1SqlNormalized,
            mainQ2SqlNormalized,
            cutPlans
        );

        return finalChoice.hasCut ? SplitResult.CUT : SplitResult.NO_BENEFICIAL_CUT;
    }





    private void writeStatusFile(int totalQueries,
                                 int failedQueries,
                                 int failedUnexpected,
                                 List<String> acceptedCutNames,
                                 List<String> acceptedNoCutNames,
                                 List<String> failedNames) throws IOException {

        StringBuilder sb = new StringBuilder();
        int successful = totalQueries - failedQueries;

        sb.append("Total queries                 : ").append(totalQueries).append("\n");
        sb.append("Successful queries (overall)  : ").append(successful).append("\n");
        sb.append("  with cut                    : ").append(acceptedCutNames.size()).append("\n");
        sb.append("  accepted but not cut        : ").append(acceptedNoCutNames.size()).append("\n");
        sb.append("Failed queries                : ").append(failedQueries).append("\n");
        sb.append("Failed UNEXPECTED             : ").append(failedUnexpected).append("\n\n");

        sb.append("Accepted queries (with cut):\n");
        for (String name : acceptedCutNames) {
            sb.append("  - ").append(name).append("\n");
        }

        sb.append("\nAccepted queries (no cut):\n");
        for (String name : acceptedNoCutNames) {
            sb.append("  - ").append(name).append("\n");
        }

        sb.append("\nFailed queries:\n");
        for (String name : failedNames) {
            sb.append("  - ").append(name).append("\n");
        }

        Path statusFile = outputDir.resolve("query_status.txt");
        Files.writeString(statusFile, sb.toString(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void initTimingsCsv() throws IOException {
        Path csvPath = outputDir.resolve("query_timings.csv");
        String header = "query_id,success,total_ms,parse_ms,opt_ms,ml_ms,dp_ms,cut_ms\n";
        Files.write(csvPath,
                    header.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void appendTimingRow(String queryId,
                                 boolean success,
                                 long totalMs,
                                 long parseMs,
                                 long optMs,
                                 long mlMs,
                                 long dpMs,
                                 long cutMs) {
        try {
            Path csvPath = outputDir.resolve("query_timings.csv");
            String row = queryId + ","
                + success + ","
                + totalMs + ","
                + parseMs + ","
                + optMs + ","
                + mlMs + ","
                + dpMs + ","
                + cutMs + "\n";
            Files.write(csvPath,
                        row.getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("[QuerySplitPipeline] Failed to append timing row for query "
                    + queryId + ": " + e.getMessage());
        }
    }








    public static final class JsonColumn {
        public String name;
        public String type;  // Calcite SqlTypeName
    }

    public static final class JsonSubQuery {
        public String id;
        public String engine;
        public String sql;
        public Map<String, String> inputs;
        public List<JsonColumn> schema;
    }

    private static List<JsonColumn> buildSchema(RelNode rel) {
        RelDataType rowType = rel.getRowType();
        List<JsonColumn> cols = new ArrayList<>();

        for (RelDataTypeField f : rowType.getFieldList()) {
            JsonColumn c = new JsonColumn();
            c.name = f.getName();
            c.type = f.getType().getSqlTypeName().name(); // e.g. INTEGER, BIGINT, VARCHAR
            cols.add(c);
        }
        return cols;
    }





    public static final class JsonDpSummary {
        public boolean hasCut;
        public int cutNodeId;
        public String q1Engine;
        public String q2Engine;
        public double costAllDuckdb;
        public double costAllDatafusion;
        public double chosenCost;
    }

    public static final class JsonCutPlan {
        public String planId;
        public JsonDpSummary dpSummary;
        public JsonAnalysis analysis;
        public JsonDag dag;
    }

    public static final class JsonDag {
        public String finalNodeId;
        public List<JsonSubQuery> nodes;
    }

    public static final class JsonPlan {
        public String queryId;
        public String originalSql;
        public JsonDpSummary dpSummary;
        public JsonAnalysis analysis;
        public JsonDag dag;
        public List<JsonCutPlan> cutPlans;
    }


    // analysis section
    public static final class JsonPipelineInfo {
        public String runId;
        public boolean doOptimize;
        public String metadataProvider; // "default" | "custom"
        public String dpMode;           // "MAX" | "SUM"
        public double transferExtraConstantMs;
        public Map<String, Map<String, Double>> transferCoeffs; // receiver -> {aMs,bMsPerRow,...}
        public String decorrelatedPlanSha256;
        public String cutPolicy;
        public long randomSeed;
        public String schemaName;

    }

    public static final class JsonPlanStats {
        public int totalNodes;
        public int maxDepth;
        public Map<String, Integer> operatorKindCounts; // FILTER/JOIN/SORT/AGGREGATE/OTHER
        public double rootOutputRows;
        public double rootRowSizeOutBytes;
    }

    public static final class JsonPartitionStats {
        public String engine; // "duckdb" | "datafusion"
        public int nodeCount;
        public int maxDepth;  // depth of Q1/Q2 subquery
        public Map<String, Integer> operatorKindCounts;
    }

    public static final class JsonCutAnalysis {
        public int cutDepth;
        public String cutNodeOperatorKind; // core kind (FILTER/JOIN/SORT/AGGREGATE) or OTHER

        // Transfer volume info
        public double cutOutputRows;
        public double cutRowSizeOutBytes;
        public double cutOutputBytes;
        public double transferEstimatedMs;

        public JsonPartitionStats q1Stats;
        public JsonPartitionStats q2Stats;
    }

    public static final class JsonAnalysis {
        public JsonPipelineInfo pipeline;
        public JsonPlanStats planStats;
        public JsonCutAnalysis cut; // null if no cut
    }




    private static final class CutPlanSpec {
        final String planId;
        final DpCutFinder.PlanChoice choice;
        final PlanCutter.SplitPlan splitPlan;
        final String q1SqlNormalized;
        final String q2SqlNormalized;

        CutPlanSpec(String planId,
                    DpCutFinder.PlanChoice choice,
                    PlanCutter.SplitPlan splitPlan,
                    String q1SqlNormalized,
                    String q2SqlNormalized) {
            this.planId = planId;
            this.choice = choice;
            this.splitPlan = splitPlan;
            this.q1SqlNormalized = q1SqlNormalized;
            this.q2SqlNormalized = q2SqlNormalized;
        }
    }

    private void writeJsonPlan(
        SqlQueryLoader.SqlQuery query,
        String originalSql,
        DpCutFinder.PlanChoice choice,
        RelNode decorrelatedPlan,
        Map<Integer, OperatorFeatures> featuresById,
        PlanCutter.SplitPlan splitPlanOrNull,
        String q1SqlNormalizedOrNull,
        String q2SqlNormalizedOrNull,
        List<CutPlanSpec> cutPlans) throws IOException {


        JsonPlan jp = new JsonPlan();
        jp.queryId = query.baseName();
        jp.originalSql = originalSql;

        // ---- DP summary ----
        JsonDpSummary dps = buildDpSummary(choice);
        jp.dpSummary = dps;

        PlanIndex idx = new PlanIndex();
        idx.dfs(decorrelatedPlan, 0);

        jp.analysis = buildAnalysisForChoice(
            choice,
            decorrelatedPlan,
            featuresById,
            splitPlanOrNull,
            idx
        );



        // ---- DAG (main plan) ----
        JsonDag dag = buildDagFromPlan(
            originalSql,
            dps,
            splitPlanOrNull,
            q1SqlNormalizedOrNull,
            q2SqlNormalizedOrNull
        );
        jp.dag = dag;

        // ---- Cut plans (main + exhaustive) ----
        jp.cutPlans = new ArrayList<>();
        if (cutPlans != null) {
            for (CutPlanSpec spec : cutPlans) {
                JsonCutPlan cp = new JsonCutPlan();
                cp.planId = spec.planId;
                cp.dpSummary = buildDpSummary(spec.choice);
                cp.analysis = buildAnalysisForChoice(
                    spec.choice,
                    decorrelatedPlan,
                    featuresById,
                    spec.splitPlan,
                    idx
                );
                cp.dag = buildDagFromPlan(
                    originalSql,
                    cp.dpSummary,
                    spec.splitPlan,
                    spec.q1SqlNormalized,
                    spec.q2SqlNormalized
                );
                jp.cutPlans.add(cp);
            }
        }

        Path jsonPath = outputDir.resolve(query.baseName() + ".json");
        jsonMapper.writerWithDefaultPrettyPrinter()
                .writeValue(jsonPath.toFile(), jp);
    }



    /**
     * Returns true if the given subplan contains at least one of the
     * four "core" operations: Filter, Sort, Join, Aggregate.
     *
     */
    private static boolean hasCoreOperator(RelNode root) {
        final boolean[] found = { false };

        RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (found[0]) {
                    // short-circuit: we've already seen at least one core op
                    return;
                }
                if (node instanceof Filter
                    || node instanceof Sort
                    || node instanceof Join
                    || node instanceof Aggregate) {
                    found[0] = true;
                    return;
                }
                super.visit(node, ordinal, parent);
            }
        };

        visitor.go(root);
        return found[0];
    }






    /**
     * If the chosen cut is below one or more LogicalProject nodes, move the cut up so
     * that those projects are also part of Q1.
     *
     */
    private static int liftCutOverProjects(RelNode root, int cutNodeId) {
        // Build parent-by-id map in one traversal.
        class ParentMapBuilder extends RelVisitor {
            final java.util.Map<Integer, RelNode> byId = new java.util.HashMap<>();
            final java.util.Map<Integer, RelNode> parent = new java.util.HashMap<>();

            @Override
            public void visit(RelNode node, int ordinal, RelNode parentNode) {
                byId.put(node.getId(), node);
                if (parentNode != null) {
                    parent.put(node.getId(), parentNode);
                }
                super.visit(node, ordinal, parentNode);
            }
        }

        ParentMapBuilder b = new ParentMapBuilder();
        b.go(root);

        RelNode current = b.byId.get(cutNodeId);
        if (current == null) {
            return cutNodeId;
        }

        RelNode parent = b.parent.get(current.getId());

        // Climb while the parent is a LogicalProject.
        while (parent instanceof LogicalProject) {
            current = parent;
            parent = b.parent.get(current.getId());
        }

        return current.getId();
    }


    private List<CutPlanSpec> buildCutPlanSpecs(RelNode decorrelated,
                                                DpCutFinder.PlanChoice mainChoice,
                                                PlanCutter.SplitPlan mainSplitPlan,
                                                String mainQ1SqlNormalized,
                                                String mainQ2SqlNormalized) {
        List<CutPlanSpec> specs = new ArrayList<>();

        Engine altQ1 = mainChoice.hasCut ? mainChoice.q1Engine : Engine.DUCKDB;
        Engine altQ2 = mainChoice.hasCut ? mainChoice.q2Engine : Engine.DATAFUSION;

        java.util.Set<Integer> seen = new java.util.LinkedHashSet<>();
        if (mainChoice.hasCut) {
            seen.add(mainChoice.cutNodeId);
        }

        List<Integer> candidateIds = collectAllOperatorIds(decorrelated, /*excludeRoot=*/true);
        for (int id : candidateIds) {
            int lifted = liftCutOverProjects(decorrelated, id);
            if (!seen.add(lifted)) {
                continue;
            }
            if (mainChoice.hasCut && lifted == mainChoice.cutNodeId) {
                continue;
            }

            PlanCutter.SplitPlan splitPlan;
            try {
                splitPlan = planCutter.cut(decorrelated, lifted);
            } catch (Exception e) {
                System.out.println("[WARNING] Failed to cut at node " + lifted
                    + ": " + e.getMessage());
                continue;
            }

            String q1SqlNormalized = SqlPostProcessor.normalize(
                splitPlan.q1Sql(), altQ1, dataset.getSchemaName());
            String q2SqlNormalized = SqlPostProcessor.normalize(
                splitPlan.q2Sql(), altQ2, dataset.getSchemaName());

            DpCutFinder.PlanChoice altChoice =
                new DpCutFinder.PlanChoice(
                    true,
                    lifted,
                    altQ1,
                    altQ2,
                    mainChoice.costAllDuckdb,
                    mainChoice.costAllDatafusion,
                    -1.0
                );

            specs.add(new CutPlanSpec(
                "cut_" + lifted,
                altChoice,
                splitPlan,
                q1SqlNormalized,
                q2SqlNormalized
            ));
        }

        return specs;
    }





    // analysis helpers

    private static final class PlanIndex {
        final Map<Integer, RelNode> byId = new java.util.HashMap<>();
        final Map<Integer, Integer> depthById = new java.util.HashMap<>();
        int totalNodes = 0;
        int maxDepth = 0;

        void dfs(RelNode node, int depth) {
            int id = node.getId();
            byId.put(id, node);
            depthById.put(id, depth);
            totalNodes++;
            if (depth > maxDepth) maxDepth = depth;
            for (RelNode ch : node.getInputs()) {
                dfs(ch, depth + 1);
            }
        }
    }

    private static int computeMaxDepth(RelNode root) {
        final int[] max = {0};
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                int depth = (parent == null) ? 0 : (max[0]); // not usable directly
                super.visit(node, ordinal, parent);
            }
        };
        // simpler recursion:
        return computeMaxDepthRec(root);
    }

    private static int computeMaxDepthRec(RelNode n) {
        int best = 0;
        for (RelNode ch : n.getInputs()) {
            best = Math.max(best, 1 + computeMaxDepthRec(ch));
        }
        return best;
    }

    private static Set<Integer> collectSubtreeIds(RelNode root) {
        Set<Integer> ids = new java.util.HashSet<>();
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                ids.add(node.getId());
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return ids;
    }

    private static void inc(Map<String, Integer> m, String k) {
        m.put(k, m.getOrDefault(k, 0) + 1);
    }

    private static Map<String, Integer> operatorKindCountsForIds(
            Set<Integer> ids,
            Map<Integer, OperatorFeatures> featuresById) {
        Map<String, Integer> out = new java.util.LinkedHashMap<>();
        for (int id : ids) {
            OperatorFeatures f = featuresById.get(id);
            if (f != null) {
                inc(out, f.operatorKind.name());
            }
        }
        return out;
    }

    /** If cut node is a Project, walk down single-input chain to first core op; else use cut node itself. */
    private static OperatorKind resolveCoreKindForCut(RelNode cutNode,
                                                    Map<Integer, OperatorFeatures> featuresById) {
        if (cutNode == null) return OperatorKind.OTHER;

        RelNode cur = cutNode;

        if (cur instanceof LogicalProject) {
            while (cur != null) {
                if (cur.getInputs().size() != 1) break;
                cur = cur.getInput(0);
                if (cur == null) break;

                OperatorFeatures f = featuresById.get(cur.getId());
                if (f == null) continue;

                if (f.operatorKind == OperatorKind.FILTER
                    || f.operatorKind == OperatorKind.JOIN
                    || f.operatorKind == OperatorKind.SORT
                    || f.operatorKind == OperatorKind.AGGREGATE) {
                    return f.operatorKind;
                }

                if (!(cur instanceof LogicalProject)) {
                    break;
                }
            }
            return OperatorKind.OTHER;
        }

        OperatorFeatures f = featuresById.get(cutNode.getId());
        return (f != null) ? f.operatorKind : OperatorKind.OTHER;
    }

    private static String sha256Hex(String s) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(dig.length * 2);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return null;
        }
    }

    private static JsonDpSummary buildDpSummary(DpCutFinder.PlanChoice choice) {
        JsonDpSummary dps = new JsonDpSummary();
        dps.hasCut = choice.hasCut;
        dps.cutNodeId = choice.cutNodeId;
        dps.q1Engine = choice.q1Engine.name().toLowerCase(Locale.ROOT);
        dps.q2Engine = choice.q2Engine.name().toLowerCase(Locale.ROOT);
        dps.costAllDuckdb = choice.costAllDuckdb;
        dps.costAllDatafusion = choice.costAllDatafusion;
        dps.chosenCost = choice.chosenCost;
        return dps;
    }

    private static JsonDag buildDagFromPlan(
            String originalSql,
            JsonDpSummary dps,
            PlanCutter.SplitPlan splitPlanOrNull,
            String q1SqlNormalizedOrNull,
            String q2SqlNormalizedOrNull) {
        JsonDag dag = new JsonDag();
        dag.nodes = new ArrayList<>();

        if (!dps.hasCut || splitPlanOrNull == null) {
            // trivial plan: single subquery runs the full query
            JsonSubQuery n0 = new JsonSubQuery();
            n0.id = "q0";
            n0.engine = dps.q1Engine;   // chosen engine from DP
            n0.sql = originalSql;
            n0.inputs = Collections.emptyMap();

            dag.finalNodeId = n0.id;
            dag.nodes.add(n0);
        } else {
            // current single-cut case: Q1 + Q2
            JsonSubQuery q1 = new JsonSubQuery();
            q1.id = "q1";
            q1.engine = dps.q1Engine;
            q1.sql = q1SqlNormalizedOrNull;
            q1.inputs = Collections.emptyMap(); // only base tables
            q1.schema = buildSchema(splitPlanOrNull.q1Rel());

            JsonSubQuery q2 = new JsonSubQuery();
            q2.id = "q2";
            q2.engine = dps.q2Engine;
            q2.sql = q2SqlNormalizedOrNull;
            q2.schema = buildSchema(splitPlanOrNull.q2Rel());

            // IMPORTANT: this must match the "s1" table name we use in PlanCutter
            Map<String, String> inputs = new LinkedHashMap<>();
            inputs.put("s1", "q1");
            q2.inputs = inputs;

            dag.finalNodeId = q2.id;
            dag.nodes.add(q1);
            dag.nodes.add(q2);
        }
        return dag;
    }

    private JsonAnalysis buildAnalysisForChoice(
        DpCutFinder.PlanChoice choice,
        RelNode decorrelatedPlan,
        Map<Integer, OperatorFeatures> featuresById,
        PlanCutter.SplitPlan splitPlanOrNull,
        PlanIndex idx) {

        JsonAnalysis analysis = new JsonAnalysis();

        //  pipeline info
        JsonPipelineInfo pi = new JsonPipelineInfo();
        pi.runId = runId;
        pi.doOptimize = doOptimize;
        pi.metadataProvider = metadataProviderName;
        pi.dpMode = (dpMode != null ? dpMode.name() : null);
        pi.transferExtraConstantMs = transferEstimator.getExtraConstantMs();
        pi.cutPolicy = cutPolicy.name();
        pi.randomSeed = randomSeed;
        pi.schemaName = dataset.getSchemaName();

        // coeffs
        Map<String, Map<String, Double>> coeffsOut = new LinkedHashMap<>();
        for (var e : transferEstimator.getAllCoeffsView().entrySet()) {
            var c = e.getValue();
            Map<String, Double> cc = new LinkedHashMap<>();
            cc.put("aMs", c.aMs);
            cc.put("bMsPerRow", c.bMsPerRow);
            cc.put("cMsPerRowSizeByte", c.cMsPerRowSizeByte);
            cc.put("dMsPerOutputByte", c.dMsPerOutputByte);
            coeffsOut.put(e.getKey().name().toLowerCase(Locale.ROOT), cc);
        }
        pi.transferCoeffs = coeffsOut;

        String planStr = RelOptUtil.toString(decorrelatedPlan);
        pi.decorrelatedPlanSha256 = sha256Hex(planStr);
        analysis.pipeline = pi;

        // plan stats
        JsonPlanStats ps = new JsonPlanStats();
        ps.totalNodes = idx.totalNodes;
        ps.maxDepth = idx.maxDepth;
        ps.operatorKindCounts = new LinkedHashMap<>();

        for (OperatorFeatures f : featuresById.values()) {
            inc(ps.operatorKindCounts, f.operatorKind.name());
        }

        OperatorFeatures rootF = featuresById.get(decorrelatedPlan.getId());
        ps.rootOutputRows = (rootF != null) ? rootF.outputRows : -1d;
        ps.rootRowSizeOutBytes = (rootF != null) ? rootF.rowSizeOutBytes : -1d;
        analysis.planStats = ps;

        // cut analysis
        if (choice.hasCut && choice.cutNodeId >= 0) {
            JsonCutAnalysis ca = new JsonCutAnalysis();

            RelNode cutNode = idx.byId.get(choice.cutNodeId);
            ca.cutDepth = idx.depthById.getOrDefault(choice.cutNodeId, -1);

            OperatorKind coreKind = resolveCoreKindForCut(cutNode, featuresById);
            ca.cutNodeOperatorKind = coreKind.name();

            // volume at cut node (for correlation)
            OperatorFeatures cf = featuresById.get(choice.cutNodeId);
            if (cf != null) {
                ca.cutOutputRows = cf.outputRows;
                ca.cutRowSizeOutBytes = cf.rowSizeOutBytes;
                ca.cutOutputBytes = cf.outputRows * cf.rowSizeOutBytes;

=                ca.transferEstimatedMs =
                    transferEstimator.estimateMs(choice.q2Engine, cf.outputRows, cf.rowSizeOutBytes);
            }

            Set<Integer> q1Ids = collectSubtreeIds(cutNode);
            Set<Integer> q2Ids = new java.util.HashSet<>(idx.byId.keySet());
            q2Ids.removeAll(q1Ids);

            // Q1 stats
            JsonPartitionStats q1s = new JsonPartitionStats();
            q1s.engine = choice.q1Engine.name().toLowerCase(Locale.ROOT);
            q1s.nodeCount = q1Ids.size();
            q1s.operatorKindCounts = operatorKindCountsForIds(q1Ids, featuresById);
            q1s.maxDepth = (splitPlanOrNull != null)
                ? computeMaxDepthRec(splitPlanOrNull.q1Rel())
                : computeMaxDepthRec(cutNode);

            // Q2 stats
            JsonPartitionStats q2s = new JsonPartitionStats();
            q2s.engine = choice.q2Engine.name().toLowerCase(Locale.ROOT);
            q2s.nodeCount = q2Ids.size();
            q2s.operatorKindCounts = operatorKindCountsForIds(q2Ids, featuresById);
            q2s.maxDepth = (splitPlanOrNull != null)
                ? computeMaxDepthRec(splitPlanOrNull.q2Rel())
                : -1;

            ca.q1Stats = q1s;
            ca.q2Stats = q2s;

            analysis.cut = ca;
        } else {
            analysis.cut = null;
        }

        return analysis;
    }



    // random cut helpers
    private static List<Integer> collectAllOperatorIds(RelNode root, boolean excludeRoot) {
        final int rootId = root.getId();
        final List<Integer> ids = new ArrayList<>();
        final Set<Integer> seen = new java.util.HashSet<>();

        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                int id = node.getId();
                if (!(excludeRoot && id == rootId)) {
                    if (seen.add(id)) {
                        ids.add(id);
                    }
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);

        return ids;
    }

    private static List<Integer> collectCoreOperatorIds(RelNode root, boolean excludeRoot) {
        final int rootId = root.getId();
        final List<Integer> ids = new ArrayList<>();
        final Set<Integer> seen = new java.util.HashSet<>();

        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                int id = node.getId();

                if (!(excludeRoot && id == rootId)) {
                    if (node instanceof Filter
                        || node instanceof Sort
                        || node instanceof Join
                        || node instanceof Aggregate) {
                        if (seen.add(id)) {
                            ids.add(id);
                        }
                    }
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);

        return ids;
    }

    private int pickRandomCoreCutId(RelNode root) {
        List<Integer> ids = collectCoreOperatorIds(root, /*excludeRoot=*/true);

        if (ids.isEmpty()) {
            // System.out.println("[RANDOM] Eligible cut candidates: (none)");
            return -1;
        }

        // System.out.println("[RANDOM] Eligible cut candidates (" + ids.size() + "):");
        for (int i = 0; i < ids.size(); i++) {
            int id = ids.get(i);

            // Best-effort details from cached maps (no signature changes)
            OperatorFeatures f = (curFeaturesById != null) ? curFeaturesById.get(id) : null;
            String kind = (f != null) ? f.operatorKind.name() : "?";
            double outRows = (f != null) ? f.outputRows : -1.0;
            double rowSize = (f != null) ? f.rowSizeOutBytes : -1.0;

            Map<Engine, Double> r = (curRuntimesById != null) ? curRuntimesById.get(id) : null;
            Double duck = (r != null) ? r.get(Engine.DUCKDB) : null;
            Double df   = (r != null) ? r.get(Engine.DATAFUSION) : null;

            // System.out.println("  [" + i + "] nodeId=" + id
            //         + " kind=" + kind
            //         + " rtDuckMs=" + (duck != null ? duck : "null")
            //         + " rtDfMs=" + (df != null ? df : "null")
            //         + " outRows=" + outRows
            //         + " rowSizeOutBytes=" + rowSize);
        }

        int idx = rng.nextInt(ids.size());
        int pickedId = ids.get(idx);

        OperatorFeatures pf = (curFeaturesById != null) ? curFeaturesById.get(pickedId) : null;
        String pk = (pf != null) ? pf.operatorKind.name() : "?";
        // System.out.println("[RANDOM] Picked cut: [" + idx + "] nodeId=" + pickedId + " kind=" + pk);

        return pickedId;
    }


    private Engine randomEngine() {
        return rng.nextBoolean() ? Engine.DUCKDB : Engine.DATAFUSION;
    }

    private static Engine otherEngine(Engine e) {
        return (e == Engine.DUCKDB) ? Engine.DATAFUSION : Engine.DUCKDB;
    }


}
