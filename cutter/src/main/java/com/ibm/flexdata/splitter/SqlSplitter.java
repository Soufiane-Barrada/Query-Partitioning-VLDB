package com.ibm.flexdata.splitter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;

public class SqlSplitter {

    private static final double transferExtraConstantMs = 0.0;

    private static String getArgValue(String[] args, String key) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                "Usage: SqlSplitter <sqlDir> <outputDir> " +
                "[--custom-metadata] [--no-optimize] [--agg-mode SUM|MAX] [--run-id <id>] " +
                "[--cut-policy DP|RANDOM] [--random-seed <long>] [--schema STACK|IMDB|TPCH1|TPCH10] " +
                "[--verbose-plans]"
                );
            System.exit(1);
        }

        boolean doOptimize = !Arrays.asList(args).contains("--no-optimize");
        System.out.println("Optimization: " + (doOptimize ? "ON" : "OFF"));

        Path sqlDir = Paths.get(args[0]);
        Path outputDir = Paths.get(args[1]);
        boolean useCustomMetadata = Arrays.asList(args).contains("--custom-metadata");
        boolean verbosePlans = Arrays.asList(args).contains("--verbose-plans");

        DpCutFinder.AggMode aggMode = DpCutFinder.AggMode.SUM;
        String aggModeStr = getArgValue(args, "--agg-mode");
        if (aggModeStr != null) {
            aggMode = DpCutFinder.AggMode.valueOf(aggModeStr.trim().toUpperCase(Locale.ROOT));
        }
        System.out.println("Using agg mode: " + aggMode);

        String runId = getArgValue(args, "--run-id");
        if (runId == null || runId.isBlank()) {
            runId = "run_" + System.currentTimeMillis();
        }
        String metadataProviderName = useCustomMetadata ? "custom" : "default";

        // Cut mode (DP vs RANDOM)
        CutPolicyMode cutPolicy = CutPolicyMode.DP;
        String cutPolicyStr = getArgValue(args, "--cut-policy");
        if (cutPolicyStr != null && !cutPolicyStr.isBlank()) {
            cutPolicy = CutPolicyMode.valueOf(cutPolicyStr.trim().toUpperCase(Locale.ROOT));
        }
        System.out.println("Cut policy: " + cutPolicy);

        long randomSeed = System.currentTimeMillis();
        String seedStr = getArgValue(args, "--random-seed");
        if (seedStr != null && !seedStr.isBlank()) {
            randomSeed = Long.parseLong(seedStr.trim());
        }
        System.out.println("Random seed: " + randomSeed);




        String schemaArg = getArgValue(args, "--schema");
        Dataset dataset = Dataset.fromArgOrDefault(schemaArg);
        System.out.println("Using schema: " + dataset.getSchemaName());

        // 1) Calcite environment
        CalciteEnvironment calciteEnv = new CalciteEnvironment(dataset);

        // 2) Metadata provider (default vs custom)
        JaninoRelMetadataProvider metadataProvider;
        if (useCustomMetadata) {
            metadataProvider = MetadataProviderFactory.customProvider(dataset);
            System.out.println("Using CUSTOM metadata provider (placeholder).");
        } else {
            metadataProvider = MetadataProviderFactory.defaultProvider();
            System.out.println("Using DEFAULT metadata provider.");
        }

        // 3) Optimization rules
        var hepProgram = HepPrograms.defaultProgram();

        // 4) Optimizer
        PlanOptimizer optimizer = new PlanOptimizer(hepProgram, metadataProvider);

        // 5) Stats + feature extractor
        SoStatsRegistry statsRegistry = SoStatsRegistry.getInstance(dataset);

        // 6) Load runtime config (STRICT: must exist)
        final String configResource = "runtime-config.json";
        RuntimeConfig cfg = RuntimeConfig.loadFromClasspath(configResource);
        if (cfg == null) {
            throw new IllegalStateException("runtime-config.json must be on classpath");
        }
        System.out.println("Using runtime config from classpath resource: " + configResource);

        if (cfg.pythonExecutable == null || cfg.predictorScript == null) {
            throw new IllegalStateException(
                "runtime-config.json must define pythonExecutable and predictorScript");
        }

        String pythonExe = cfg.pythonExecutable;
        String predictorScript = cfg.predictorScript;
        Map<Engine, Map<OperatorKind, String>> modelPaths = cfg.toModelPaths();

        // 7) Transfer model (STRICT)
        Map<Engine, RuntimeConfig.TransferModelCoeffs> tm = cfg.toTransferModelsStrict();
        Map<Engine, TransferCostEstimator.Coeffs> coeffs = new EnumMap<>(Engine.class);

        coeffs.put(
            Engine.DUCKDB,
            new TransferCostEstimator.Coeffs(
                tm.get(Engine.DUCKDB).aMs,
                tm.get(Engine.DUCKDB).bMsPerRow,
                tm.get(Engine.DUCKDB).cMsPerRowSizeByte,
                tm.get(Engine.DUCKDB).dMsPerOutputByte
            )
        );
        coeffs.put(
            Engine.DATAFUSION,
            new TransferCostEstimator.Coeffs(
                tm.get(Engine.DATAFUSION).aMs,
                tm.get(Engine.DATAFUSION).bMsPerRow,
                tm.get(Engine.DATAFUSION).cMsPerRowSizeByte,
                tm.get(Engine.DATAFUSION).dMsPerOutputByte
            )
        );

        TransferCostEstimator transferEstimator =
            new TransferCostEstimator(coeffs, transferExtraConstantMs);

        // 8) Build Python client + runtime model
        PythonRuntimeClient pyClient =
            new PythonRuntimeClient(pythonExe, predictorScript, modelPaths);

        RuntimeModel runtimeModel = new PythonRuntimeModel(pyClient);
        RuntimeEstimator runtimeEstimator = new RuntimeEstimator(runtimeModel);

        // 9) DP cut finder (now uses learned transfer model)
        DpCutFinder cutFinder = new DpCutFinder(aggMode, transferEstimator);

        // 10) Plan cutter (RelNode → two subplans + SQL)
        PlanCutter planCutter = new PlanCutter(calciteEnv.getDialect());

        // 11) Run pipeline
        QuerySplitPipeline pipeline =
            new QuerySplitPipeline(
                sqlDir, outputDir, calciteEnv,
                optimizer, statsRegistry,
                runtimeEstimator, cutFinder, planCutter,
                doOptimize,
                runId,
                metadataProviderName,
                aggMode,
                cutPolicy,
                randomSeed,
                dataset,
                verbosePlans
            );


             



        pipeline.run();

    }
}
