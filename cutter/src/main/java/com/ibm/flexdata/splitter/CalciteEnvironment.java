package com.ibm.flexdata.splitter;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;

import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;

import org.apache.calcite.sql.validate.SqlConformanceEnum;

import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import org.apache.calcite.rel.RelRoot;

public final class CalciteEnvironment {

    private final FrameworkConfig frameworkConfig;
    private final SqlDialect dialect;

    public CalciteEnvironment() {
        this(Dataset.STACKOVERFLOW);
    }

    public CalciteEnvironment(Dataset dataset) {
        if (dataset == null) {
            dataset = Dataset.STACKOVERFLOW;
        }
        SchemaPlus root = Frameworks.createRootSchema(true);
        SoStatsRegistry statsRegistry = SoStatsRegistry.getInstance(dataset);
        switch (dataset) {
            case IMDB:
                ImdbSchema.register(root, statsRegistry);
                break;
            case TPCH1:
            case TPCH10:
                TpchSchema.register(root, statsRegistry);
                break;
            case STACKOVERFLOW:
            default:
                StackOverflowSchema.register(root, statsRegistry);
                break;
        }

        // 1) Parser config: Babel parser + BABEL conformance
        SqlParser.Config parserCfg =
            SqlParser.config()
                .withCaseSensitive(false)
                .withParserFactory(SqlBabelParserImpl.FACTORY)
                .withConformance(SqlConformanceEnum.BABEL);

        // 2) Operator table: STANDARD + Postgres + MySQL functions
        SqlOperatorTable opTab =
            SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                SqlLibrary.STANDARD,
                SqlLibrary.POSTGRESQL,
                SqlLibrary.MYSQL
            );

        this.frameworkConfig =
            Frameworks.newConfigBuilder()
                .defaultSchema(root.getSubSchema(dataset.getSchemaName()))
                .parserConfig(parserCfg)
                .operatorTable(opTab)
                .build();

        // Use DuckDB dialect for Rel→SQL
        this.dialect = SqlDialect.DatabaseProduct.DUCKDB.getDialect();
    }

    public RelRoot sqlToRelRoot(String sql) throws Exception {
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode parsed = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        return planner.rel(validated);
    }

    public SqlDialect getDialect() {
        return dialect;
    }

    public FrameworkConfig getFrameworkConfig() {
        return frameworkConfig;
    }
}
