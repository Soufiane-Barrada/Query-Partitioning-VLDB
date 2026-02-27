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

public class ImdbSchema extends AbstractSchema {

    public static void register(SchemaPlus root, SoStatsRegistry statsRegistry) {
        SchemaPlus schema = root.add("IMDB", new ImdbSchema());

        schema.add("aka_name", new SimpleTable(
            "aka_name",
            statsRegistry.getTableStats("aka_name")
        ));
        schema.add("aka_title", new SimpleTable(
            "aka_title",
            statsRegistry.getTableStats("aka_title")
        ));
        schema.add("cast_info", new SimpleTable(
            "cast_info",
            statsRegistry.getTableStats("cast_info")
        ));
        schema.add("char_name", new SimpleTable(
            "char_name",
            statsRegistry.getTableStats("char_name")
        ));
        schema.add("comp_cast_type", new SimpleTable(
            "comp_cast_type",
            statsRegistry.getTableStats("comp_cast_type")
        ));
        schema.add("company_name", new SimpleTable(
            "company_name",
            statsRegistry.getTableStats("company_name")
        ));
        schema.add("company_type", new SimpleTable(
            "company_type",
            statsRegistry.getTableStats("company_type")
        ));
        schema.add("complete_cast", new SimpleTable(
            "complete_cast",
            statsRegistry.getTableStats("complete_cast")
        ));
        schema.add("info_type", new SimpleTable(
            "info_type",
            statsRegistry.getTableStats("info_type")
        ));
        schema.add("keyword", new SimpleTable(
            "keyword",
            statsRegistry.getTableStats("keyword")
        ));
        schema.add("kind_type", new SimpleTable(
            "kind_type",
            statsRegistry.getTableStats("kind_type")
        ));
        schema.add("link_type", new SimpleTable(
            "link_type",
            statsRegistry.getTableStats("link_type")
        ));
        schema.add("movie_companies", new SimpleTable(
            "movie_companies",
            statsRegistry.getTableStats("movie_companies")
        ));
        schema.add("movie_info_idx", new SimpleTable(
            "movie_info_idx",
            statsRegistry.getTableStats("movie_info_idx")
        ));
        schema.add("movie_keyword", new SimpleTable(
            "movie_keyword",
            statsRegistry.getTableStats("movie_keyword")
        ));
        schema.add("movie_link", new SimpleTable(
            "movie_link",
            statsRegistry.getTableStats("movie_link")
        ));
        schema.add("name", new SimpleTable(
            "name",
            statsRegistry.getTableStats("name")
        ));
        schema.add("role_type", new SimpleTable(
            "role_type",
            statsRegistry.getTableStats("role_type")
        ));
        schema.add("title", new SimpleTable(
            "title",
            statsRegistry.getTableStats("title")
        ));
        schema.add("movie_info", new SimpleTable(
            "movie_info",
            statsRegistry.getTableStats("movie_info")
        ));
        schema.add("person_info", new SimpleTable(
            "person_info",
            statsRegistry.getTableStats("person_info")
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
                case "aka_name":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("person_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_index", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("name_pcode_cf", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("name_pcode_nf", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("surname_pcode", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("md5sum", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "aka_title":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("title", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_index", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("kind_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("production_year", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("phonetic_code", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("episode_of_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("season_nr", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("episode_nr", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("note", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("md5sum", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "cast_info":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("person_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("person_role_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("note", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("nr_order", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("role_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    break;

                case "char_name":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_index", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("name_pcode_nf", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("surname_pcode", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("md5sum", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "comp_cast_type":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("kind", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "company_name":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("country_code", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("name_pcode_nf", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("name_pcode_sf", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("md5sum", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "company_type":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("kind", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "complete_cast":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("subject_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("status_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    break;

                case "info_type":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("info", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "keyword":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("keyword", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("phonetic_code", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "kind_type":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("kind", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "link_type":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("link", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "movie_companies":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("company_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("company_type_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("note", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "movie_info_idx":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("info_type_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("info", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("note", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "movie_keyword":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("keyword_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    break;

                case "movie_link":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("linked_movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("link_type_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    break;

                case "name":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_index", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("gender", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("name_pcode_cf", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("name_pcode_nf", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("surname_pcode", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("md5sum", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "role_type":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("role", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "title":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("title", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("imdb_index", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("kind_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("production_year", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("imdb_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("phonetic_code", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("episode_of_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("season_nr", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("episode_nr", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("series_years", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("md5sum", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "movie_info":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("movie_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("info_type_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("info", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("note", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "person_info":
                    b.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("person_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("info_type_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("info", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("note", typeFactory.createSqlType(SqlTypeName.VARCHAR));
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

            // Unique keys: derive from NDV = row_count and null_count=0
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
