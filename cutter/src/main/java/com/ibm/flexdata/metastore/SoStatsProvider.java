package com.ibm.flexdata.metastore;

import com.ibm.flexdata.metadata.ColStats;
import com.ibm.flexdata.metadata.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;

public class SoStatsProvider implements StatsProvider {

  private final SoStatsCatalog catalog;

  public SoStatsProvider(final SoStatsCatalog catalog) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
  }

  // ------------------------------------------------------------------------
  // Row count
  // ------------------------------------------------------------------------

  @Override
  public double getRowCount(final TableScan scan) {
    final SoStatsCatalog.TableStats ts = resolveTable(scan);
    if (ts == null) {
      // No stats for this table – avoid divide-by-zero in selectivity formulas.
      return 1d;
    }
    return (double) ts.rowCount;
  }

  // ------------------------------------------------------------------------
  // Column stats
  // ------------------------------------------------------------------------

  @Override
  public List<ColStats> getColStats(final TableScan scan) {
    return getColStats(scan, scan.getRowType().getFieldList());
  }

  @Override
  public List<ColStats> getColStats(
      final TableScan scan, final List<RelDataTypeField> fields) {

    final List<ColStats> result = new ArrayList<>(fields.size());
    final SoStatsCatalog.TableStats ts = resolveTable(scan);

    for (RelDataTypeField field : fields) {
      result.add(buildColStats(ts, field));
    }
    return result;
  }

  @Override
  public ColStats getColStats(final TableScan scan, final RelDataTypeField field) {
    final SoStatsCatalog.TableStats ts = resolveTable(scan);
    return buildColStats(ts, field);
  }

  // ------------------------------------------------------------------------
  // Distinct row count (NDV)
  // ------------------------------------------------------------------------

  @Override
  public double getDistinctRowCount(
      final TableScan scan, final List<RelDataTypeField> fields) {

    final SoStatsCatalog.TableStats ts = resolveTable(scan);
    if (ts == null || fields.isEmpty()) {
      // Let metadata fall back (RelMdDistinctRowCountCustom returns 0 -> treated as unknown)
      return 0d;
    }

    // For multi-column groups, NDV cannot exceed any single column's NDV.
    long minNdv = Long.MAX_VALUE;
    boolean seen = false;

    for (RelDataTypeField field : fields) {
      final SoStatsCatalog.ColumnStats col = lookupColumn(ts, field);
      if (col != null && col.distinctCount != null && col.distinctCount > 0) {
        seen = true;
        minNdv = Math.min(minNdv, col.distinctCount);
      }
    }

    if (!seen) {
      return 0d;
    }
    return (double) minNdv;
  }

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------

  private SoStatsCatalog.TableStats resolveTable(final TableScan scan) {
    // Typical qualified name: [catalog?, schema, table] or [schema, table] or [table]
    final List<String> names = scan.getTable().getQualifiedName();
    if (names.isEmpty()) {
      return null;
    }

    final String tableName;
    final String schemaName;

    if (names.size() == 1) {
      schemaName = "default";
      tableName = names.get(0);
    } else {
      tableName = names.get(names.size() - 1);
      schemaName = names.get(names.size() - 2);
    }

    return catalog.lookupTable(schemaName, tableName);
  }

  private ColStats buildColStats(
      final SoStatsCatalog.TableStats ts, final RelDataTypeField field) {

    // Always start from type defaults (TypeSizes) …
    ColStats.Builder b =
        new ColStats.Builder()
            .name(field.getName())
            .type(field.getType());

    // then override with JSON-based stats if present.
    if (ts != null) {
      final SoStatsCatalog.ColumnStats columnStats = lookupColumn(ts, field);

      if (columnStats != null) {
        if (columnStats.distinctCount != null) {
          b = b.numDistinctValues(columnStats.distinctCount);
        }
        if (columnStats.nullCount != null) {
          b = b.numNulls(columnStats.nullCount);
        }
        if (columnStats.avgSizeBytes != null) {
          b = b.avgColLen(columnStats.avgSizeBytes);
        }
        if (columnStats.maxSizeBytes != null) {
          b = b.maxColLen(columnStats.maxSizeBytes);
        }
        if (columnStats.primaryKey != null) {
          b = b.primaryKey(columnStats.primaryKey);
        }
        if (columnStats.minValue != null && columnStats.maxValue != null) {
          b = b.range(columnStats.minValue, columnStats.maxValue);
        }
      }

      if (field.getName().equalsIgnoreCase("CreationDate")) {
      System.out.println("[SoStatsProvider] lookupColumn for columnStats: key=" + columnStats);
    }
    }

    

    return b.build();
  }

  
  private SoStatsCatalog.ColumnStats lookupColumn(
      final SoStatsCatalog.TableStats ts, final RelDataTypeField field) {

    if (ts == null || ts.columnsByName == null) {
      return null;
    }
    // Column keys in catalog are lowercase; make lookup case-insensitive.
    String key = field.getName().toLowerCase(Locale.ROOT);

    SoStatsCatalog.ColumnStats col = ts.columnsByName.get(key);

    // if (ts.tableName.equalsIgnoreCase("users") &&
    //     field.getName().equalsIgnoreCase("reputation")) {
    //   System.out.println("[SoStatsProvider] lookupColumn for users.reputation: key=" + key +
    //                     ", result=" + col);
    // }

    return ts.columnsByName.get(key);
  }
}
