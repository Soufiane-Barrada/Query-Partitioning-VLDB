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

public class StackOverflowSchema extends AbstractSchema {

    public static void register(SchemaPlus root) {
        register(root, SoStatsRegistry.getInstance(Dataset.STACKOVERFLOW));
    }

    public static void register(SchemaPlus root, SoStatsRegistry statsRegistry) {
        SchemaPlus schema = root.add("STACK", new StackOverflowSchema());

        
        schema.add("PostHistoryTypes", new SimpleTable(
            "PostHistoryTypes",
            statsRegistry.getTableStats("posthistorytypes")
        ));
        schema.add("LinkTypes", new SimpleTable(
            "LinkTypes",
            statsRegistry.getTableStats("linktypes")
        ));
        schema.add("PostTypes", new SimpleTable(
            "PostTypes",
            statsRegistry.getTableStats("posttypes")
        ));
        schema.add("CloseReasonTypes", new SimpleTable(
            "CloseReasonTypes",
            statsRegistry.getTableStats("closereasontypes")
        ));
        schema.add("VoteTypes", new SimpleTable(
            "VoteTypes",
            statsRegistry.getTableStats("votetypes")
        ));
        schema.add("Users", new SimpleTable(
            "Users",
            statsRegistry.getTableStats("users")
        ));
        schema.add("Badges", new SimpleTable(
            "Badges",
            statsRegistry.getTableStats("badges")
        ));
        schema.add("Posts", new SimpleTable(
            "Posts",
            statsRegistry.getTableStats("posts")
        ));
        schema.add("Comments", new SimpleTable(
            "Comments",
            statsRegistry.getTableStats("comments")
        ));
        schema.add("PostHistory", new SimpleTable(
            "PostHistory",
            statsRegistry.getTableStats("posthistory")
        ));
        schema.add("PostLinks", new SimpleTable(
            "PostLinks",
            statsRegistry.getTableStats("postlinks")
        ));
        schema.add("Tags", new SimpleTable(
            "Tags",
            statsRegistry.getTableStats("tags")
        ));
        schema.add("Votes", new SimpleTable(
            "Votes",
            statsRegistry.getTableStats("votes")
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
                case "PostHistoryTypes":
                    b.add("Id",                 typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("Name",               typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "LinkTypes":
                    b.add("Id",   typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("Name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "PostTypes":
                    b.add("Id",   typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("Name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "CloseReasonTypes":
                    b.add("Id",   typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("Name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "VoteTypes":
                    b.add("Id",   typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("Name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "Posts":
                    b.add("Id",                 typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("PostTypeId",         typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("AcceptedAnswerId",   typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("ParentId",           typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("CreationDate",       typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("Score",              typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("ViewCount",          typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("Body",               typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("OwnerUserId",        typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("OwnerDisplayName",   typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("LastEditorUserId",   typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("LastEditorDisplayName", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("LastEditDate",       typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("LastActivityDate",   typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("Title",              typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("Tags",               typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("AnswerCount",        typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("CommentCount",       typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("FavoriteCount",      typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("ClosedDate",         typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("CommunityOwnedDate", typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("ContentLicense",     typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "Users":
                    b.add("Id",              typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("Reputation",      typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("CreationDate",    typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("DisplayName",     typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("LastAccessDate",  typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("WebsiteUrl",      typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("Location",        typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("AboutMe",         typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("Views",           typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("UpVotes",         typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("DownVotes",       typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("ProfileImageUrl", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("AccountId",       typeFactory.createSqlType(SqlTypeName.INTEGER));
                    break;

                case "Comments":
                    b.add("Id",              typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("PostId",          typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("Score",           typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("Text",            typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("CreationDate",    typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("UserDisplayName", typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("UserId",          typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("ContentLicense",  typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "Badges":
                    b.add("Id",       typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("UserId",   typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("Name",     typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("Date",     typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("Class",    typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("TagBased", typeFactory.createSqlType(SqlTypeName.BOOLEAN));
                    break;

                case "PostHistory":
                    b.add("Id",                typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("PostHistoryTypeId", typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("PostId",            typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("RevisionGUID",      typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("CreationDate",      typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("UserId",            typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("UserDisplayName",   typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("Comment",           typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("Text",              typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("ContentLicense",    typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    break;

                case "PostLinks":
                    b.add("Id",            typeFactory.createSqlType(SqlTypeName.BIGINT));
                    b.add("CreationDate",  typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("PostId",        typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("RelatedPostId", typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("LinkTypeId",    typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    break;

                case "Tags":
                    b.add("Id",              typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("TagName",         typeFactory.createSqlType(SqlTypeName.VARCHAR));
                    b.add("Count",           typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("ExcerptPostId",   typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("WikiPostId",      typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("IsModeratorOnly", typeFactory.createSqlType(SqlTypeName.BOOLEAN));
                    b.add("IsRequired",      typeFactory.createSqlType(SqlTypeName.BOOLEAN));
                    break;

                case "Votes":
                    b.add("Id",           typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("PostId",       typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("VoteTypeId",   typeFactory.createSqlType(SqlTypeName.SMALLINT));
                    b.add("UserId",       typeFactory.createSqlType(SqlTypeName.INTEGER));
                    b.add("CreationDate", typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
                    b.add("BountyAmount", typeFactory.createSqlType(SqlTypeName.INTEGER));
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
                // In our schemas, "Id" is column 0 for all three tables.
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
