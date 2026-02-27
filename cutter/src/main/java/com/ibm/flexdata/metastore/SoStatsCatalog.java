package com.ibm.flexdata.metastore;

import java.util.Map;

/**
 * Logical view over so_relations_data.json.
 */
public interface SoStatsCatalog {

  /** Look up table-level stats by logical Calcite table name. */
  TableStats lookupTable(String schema, String tableName);

  /** Holds table-level statistics. */
  final class TableStats {
    public final String schemaName;
    public final String tableName;
    public final long rowCount;
    public final Map<String, ColumnStats> columnsByName;

    public TableStats(
        String schemaName,
        String tableName,
        long rowCount,
        Map<String, ColumnStats> columnsByName) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.rowCount = rowCount;
      this.columnsByName = columnsByName;
    }

    @Override
    public String toString() {
      return "TableStats{" +
          "schemaName='" + schemaName + '\'' +
          ", tableName='" + tableName + '\'' +
          ", rowCount=" + rowCount +
          ", columns=" + (columnsByName != null ? columnsByName.keySet() : null) +
          '}';
    }
  }

  /** Holds column-level statistics. All fields can be null if unknown. */
  final class ColumnStats {
    public final Long distinctCount;   // NDV
    public final Long nullCount;
    public final Double avgSizeBytes;
    public final Long maxSizeBytes;
    public final Double minValue;      // for range predicates (numeric/date)
    public final Double maxValue;
    public final Boolean primaryKey;

    public ColumnStats(
        Long distinctCount,
        Long nullCount,
        Double avgSizeBytes,
        Long maxSizeBytes,
        Double minValue,
        Double maxValue,
        Boolean primaryKey) {
      this.distinctCount = distinctCount;
      this.nullCount = nullCount;
      this.avgSizeBytes = avgSizeBytes;
      this.maxSizeBytes = maxSizeBytes;
      this.minValue = minValue;
      this.maxValue = maxValue;
      this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
      return "ColumnStats{" +
          "distinctCount=" + distinctCount +
          ", nullCount=" + nullCount +
          ", avgSizeBytes=" + avgSizeBytes +
          ", maxSizeBytes=" + maxSizeBytes +
          ", minValue=" + minValue +
          ", maxValue=" + maxValue +
          ", primaryKey=" + primaryKey +
          '}';
    }
  }
}
