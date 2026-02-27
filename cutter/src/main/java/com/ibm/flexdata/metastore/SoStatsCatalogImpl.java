package com.ibm.flexdata.metastore;

import com.ibm.flexdata.splitter.SoStats;
import com.ibm.flexdata.splitter.SoStatsRegistry;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;


/** Adapter: SoStatsRegistry (JSON) -> SoStatsCatalog (metadata view). */
public class SoStatsCatalogImpl implements SoStatsCatalog {


  // Accepts "yyyy-MM-dd HH:mm:ss" with optional .SSSSSS
  private static final DateTimeFormatter SO_TS_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
          .optionalEnd()
          .toFormatter();


  private final SoStatsRegistry registry;



  public SoStatsCatalogImpl(final SoStatsRegistry registry) {
    this.registry = registry;
  }



  @Override
  public TableStats lookupTable(final String schema, final String tableName) {
    if (tableName == null) {
      return null;
    }

    // Registry is keyed just by table name (lowercase)
    final SoStats.TableStats raw =
        registry.getTableStats(tableName.toLowerCase(Locale.ROOT));
    if (raw == null) {
        // System.out.println("[SoStatsCatalogImpl] No JSON stats for table " + tableName);
        return null;
    }

    final Map<String, ColumnStats> cols = new HashMap<>();
    if (raw.columns != null) {
      for (Map.Entry<String, SoStats.ColumnStats> e : raw.columns.entrySet()) {
        final String colName = e.getKey(); // already lowercase in registry
        final SoStats.ColumnStats c = e.getValue();

        final Long distinctCount = c.distinct_count;
        final Long nullCount = c.null_count;

        // Use column_size_bytes as the average per-value size
        final Double avgSizeBytes = c.column_size_bytes;

        // We usually don't have a true max size; approximate with avg or null.
        Long maxSizeBytes = null;
        if (c.total_uncompressed_bytes > 0 && c.number_of_rows > 0) {
            long avgUncompressedSize = (long) Math.ceil(
                (double) c.total_uncompressed_bytes / c.number_of_rows
            );
            long avgColumnSize = (long) Math.ceil(avgSizeBytes);

            long base = Math.max(avgUncompressedSize, avgColumnSize);
            maxSizeBytes = (long) Math.ceil(base * 1.2);

        }

        // Convert JSON min/max into a numeric domain compatible with FilterSelectivityEstimator Including dates/timestamps
        final Double minValue = toRangeValue(colName, c.arrow_type, c.min);
        final Double maxValue = toRangeValue(colName, c.arrow_type, c.max);

        // System.out.println(
        //     "[SoStatsCatalogImpl] " + tableName + "." + colName
        //         + " rawMin=" + c.min + ", rawMax=" + c.max
        //         + " -> minValue=" + minValue + ", maxValue=" + maxValue);


        final Boolean primaryKey = null; // don't have PK info in JSON

        final ColumnStats catalogCol =
            new ColumnStats(
                distinctCount,
                nullCount,
                avgSizeBytes,
                maxSizeBytes,
                minValue,
                maxValue,
                primaryKey);

        
        // if ("users".equalsIgnoreCase(tableName) &&
        //         "reputation".equalsIgnoreCase(colName)) {
        //         System.out.println("[SoStatsCatalogImpl] Catalog stats for users.reputation = " + catalogCol);
        //     }


        cols.put(colName, catalogCol);
      }
    }

    final String schemaLower = (schema == null) ? null : schema.toLowerCase(Locale.ROOT);
    final String tableLower =
        (raw.table != null) ? raw.table.toLowerCase(Locale.ROOT) : tableName.toLowerCase(Locale.ROOT);

    return new TableStats(schemaLower, tableLower, raw.getRowCount(), cols);
  }




  private static boolean isTimestampArrowType(String arrowType) {
    return arrowType != null && arrowType.toLowerCase(Locale.ROOT).startsWith("timestamp");
  }

  private static boolean isNumericArrowType(String arrowType) {
    if (arrowType == null) {
      return false;
    }
    String t = arrowType.toLowerCase(Locale.ROOT);
    return t.startsWith("int")
        || t.startsWith("uint")
        || t.startsWith("float")
        || t.startsWith("double");
  }

  /** Convert raw JSON min/max to a numeric value comparable with FilterSelectivityEstimator. */
  private static Double toRangeValue(String colName, String arrowType, Object raw) {
    if (raw == null) {
      return null;
    }

    // Already numeric → just use it.
    if (raw instanceof Number n) {
      return n.doubleValue();
    }

    // Booleans (rarely used in ranges, but make them 0/1 if present)
    if (raw instanceof Boolean b) {
      return b ? 1.0 : 0.0;
    }

    if (raw instanceof String s) {
      // TIMESTAMP: parse to epoch seconds, to match extractLiteral() for TIMESTAMP
      if (isTimestampArrowType(arrowType)) {
        try {
          LocalDateTime ldt = LocalDateTime.parse(s, SO_TS_FORMATTER);
          // Uses UTC
          Instant instant = ldt.toInstant(ZoneOffset.UTC);
          long seconds = instant.getEpochSecond();
          return (double) seconds;
        } catch (DateTimeParseException ex) {
          System.err.println(
              "[SoStatsCatalogImpl] Failed to parse timestamp min/max for column "
                  + colName + ": '" + s + "' (" + arrowType + ")");
          return null;
        }
      }

      // Numeric stored as string (just in case)
      if (isNumericArrowType(arrowType)) {
        try {
          return Double.parseDouble(s);
        } catch (NumberFormatException ex) {
          System.err.println(
              "[SoStatsCatalogImpl] Failed to parse numeric min/max for column "
                  + colName + ": '" + s + "'");
          return null;
        }
      }
    }

    // Strings, complex types, etc. → we don't expose a numeric range.
    return null;
  }

}
