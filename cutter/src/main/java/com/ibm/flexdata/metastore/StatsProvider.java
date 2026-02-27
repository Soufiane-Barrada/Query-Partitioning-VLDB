package com.ibm.flexdata.metastore;

import com.ibm.flexdata.metadata.ColStats;
import java.util.List;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;

/** Interface for providing data-related statistics to metadata handlers. */
public interface StatsProvider {
  /**
   * Get the distinct row count for a list of columns of the provided table.
   *
   * @param table table
   * @param fieldNames columns for which to compute the distinct row count
   * @return number of distinct rows of the given columns
   */
  double getDistinctRowCount(TableScan table, List<RelDataTypeField> fieldNames);

  /**
   * Get the number of rows of a table.
   *
   * @param table table
   * @return number of rows in the specified table
   */
  double getRowCount(TableScan table);

  /**
   * Get statistics on all columns of the provided table.
   *
   * @param table table
   * @return list of ColStats objects for each column contained the table
   */
  List<ColStats> getColStats(TableScan table);

  /**
   * Get statistics on a set of columns of the provided table.
   *
   * @param table table
   * @param fields columns to retrieve statistics for
   * @return list of ColStats objects for each provided column
   */
  List<ColStats> getColStats(TableScan table, List<RelDataTypeField> fields);

  /**
   * Get statistics on a single column of the provided table.
   *
   * @param table table
   * @param field column to retrieve statistics for
   * @return ColStat object for the provided column
   */
  ColStats getColStats(TableScan table, RelDataTypeField field);
}
