package com.ibm.flexdata.splitter;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

public class TpchSchema extends AbstractSchema {

    public static void register(SchemaPlus root, SoStatsRegistry statsRegistry) {
        SchemaPlus schema = root.add("TPCH", new TpchSchema());

        schema.add("customer", new SimpleTable(
            "customer",
            statsRegistry.getTableStats("customer")
        ));
        schema.add("lineitem", new SimpleTable(
            "lineitem",
            statsRegistry.getTableStats("lineitem")
        ));
        schema.add("nation", new SimpleTable(
            "nation",
            statsRegistry.getTableStats("nation")
        ));
        schema.add("orders", new SimpleTable(
            "orders",
            statsRegistry.getTableStats("orders")
        ));
        schema.add("part", new SimpleTable(
            "part",
            statsRegistry.getTableStats("part")
        ));
        schema.add("partsupp", new SimpleTable(
            "partsupp",
            statsRegistry.getTableStats("partsupp")
        ));
        schema.add("region", new SimpleTable(
            "region",
            statsRegistry.getTableStats("region")
        ));
        schema.add("supplier", new SimpleTable(
            "supplier",
            statsRegistry.getTableStats("supplier")
        ));
    }

    /** Table with row type + statistics (no data). */
    static class SimpleTable extends AbstractQueryableTable {

        private final String name;
        private final SoStats.TableStats stats;

        SimpleTable(String name, SoStats.TableStats stats) {
            super(Object[].class);
            this.name = name;
            this.stats = stats;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder b = typeFactory.builder();
            switch (name) {
                case "customer":
                    b.add("c_custkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("c_name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("c_address", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("c_nationkey", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("c_phone", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("c_acctbal", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("c_mktsegment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("c_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "lineitem":
                    b.add("l_orderkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("l_partkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("l_suppkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("l_linenumber", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("l_quantity", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("l_extendedprice", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("l_discount", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("l_tax", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("l_returnflag", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("l_linestatus", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("l_shipdate", typeFactory.createSqlType(SqlTypeName.DATE));
                    b.add("l_commitdate", typeFactory.createSqlType(SqlTypeName.DATE));
                    b.add("l_receiptdate", typeFactory.createSqlType(SqlTypeName.DATE));
                    b.add("l_shipinstruct", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("l_shipmode", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("l_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "nation":
                    b.add("n_nationkey", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("n_name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("n_regionkey", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("n_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "orders":
                    b.add("o_orderkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("o_custkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("o_orderstatus", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("o_totalprice", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("o_orderdate", typeFactory.createSqlType(SqlTypeName.DATE));
                    b.add("o_orderpriority", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("o_clerk", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("o_shippriority", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("o_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "part":
                    b.add("p_partkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("p_name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("p_mfgr", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("p_brand", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("p_type", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("p_size", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("p_container", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("p_retailprice", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("p_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "partsupp":
                    b.add("ps_partkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("ps_suppkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("ps_availqty", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("ps_supplycost", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("ps_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "region":
                    b.add("r_regionkey", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("r_name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("r_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "supplier":
                    b.add("s_suppkey", typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("s_name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("s_address", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("s_nationkey", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("s_phone", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("s_acctbal", typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2));
                    b.add("s_comment", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                default:
                    throw new IllegalArgumentException("Unknown table " + name);
            }
            return b.build();
        }

        @Override
        public Statistic getStatistic() {
            if (stats == null) {
                return Statistics.UNKNOWN;
            }
            double rowCount = (double) stats.getRowCount();

            // Unique keys: attempt to infer from stats when available.
            ImmutableList.Builder<ImmutableBitSet> keysBuilder =
                ImmutableList.builder();

            if (stats.columns != null) {
                SoStats.ColumnStats idStats = stats.getColumn("id");
                if (idStats != null &&
                    idStats.distinct_count != null &&
                    idStats.distinct_count == stats.row_count &&
                    idStats.null_count == 0) {

                    keysBuilder.add(ImmutableBitSet.of(0));
                }
            }

            return Statistics.of(rowCount, keysBuilder.build());
        }

        @Override
        public <T> Queryable<T> asQueryable(
                QueryProvider queryProvider,
                SchemaPlus schema,
                String tableName) {
            // No data – planning only.
            throw new UnsupportedOperationException("No data backing for " + name);
        }
    }
}
