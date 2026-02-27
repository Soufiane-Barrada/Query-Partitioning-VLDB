package com.ibm.flexdata.splitter;

import java.util.Map;

public class SoStats {

    public static class ColumnStats {
        // Use same field names as in JSON for easy Jackson mapping
        public String arrow_type;
        public boolean nullable;
        public long total_uncompressed_bytes;
        public double avg_uncompressed_size_bytes;
        public double column_size_bytes;

        public long number_of_rows;
        public long null_count;
        public double null_frac;
        public Long distinct_count;
        public Object min;
        public Object max;

        public ColumnStats() {}


        @Override
        public String toString() {
            return "ColumnStats{" +
                "arrow_type='" + arrow_type + '\'' +
                ", nullable=" + nullable +
                ", number_of_rows=" + number_of_rows +
                ", null_count=" + null_count +
                ", null_frac=" + null_frac +
                ", distinct_count=" + distinct_count +
                ", min=" + min +
                ", max=" + max +
                ", avg_uncompressed_size_bytes=" + avg_uncompressed_size_bytes +
                ", column_size_bytes=" + column_size_bytes +
                '}';
        }
    }

    public static class TableStats {
        public String table;
        public String path;
        public long row_count;
        public int row_groups;
        public double row_size_bytes;
        public Map<String, ColumnStats> columns; // keys are lowercase column names

        public TableStats() {}

        public long getRowCount() {
            return row_count;
        }

        public ColumnStats getColumn(String name) {
            if (columns == null) return null;
            return columns.get(name.toLowerCase());
        }

        @Override
        public String toString() {
            return "TableStats{" +
                "table='" + table + '\'' +
                ", row_count=" + row_count +
                ", row_groups=" + row_groups +
                ", row_size_bytes=" + row_size_bytes +
                ", columns=" + (columns != null ? columns.keySet() : null) +
                '}';
        }
    }
}
