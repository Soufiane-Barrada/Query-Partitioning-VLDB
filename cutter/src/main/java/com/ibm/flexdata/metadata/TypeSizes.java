package com.ibm.flexdata.metadata;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Central place for default byte sizes of types.

 */
public final class TypeSizes {

  private TypeSizes() {}

  public static Double getDefaultSize(RelDataType type) {
    SqlTypeName t = type.getSqlTypeName();
    if (t == null) {
      return 8d; // generic fallback
    }
    switch (t) {
      case BOOLEAN:
        return 1d;
      case TINYINT:
        return 1d;
      case SMALLINT:
        return 2d;
      case INTEGER:
        return 4d;
      case BIGINT:
        return 8d;
      case FLOAT:
        return 4d;
      case DOUBLE:
        return 8d;
      case DECIMAL:
        return 16d;
      case DATE:
        return 4d; 
      case TIME:
      case TIMESTAMP:
        return 8d;  // micros/epoch
      case CHAR:
      case VARCHAR:
        //  can refine this
        return 16d;  // default average length
      case BINARY:
      case VARBINARY:
        return 16d;
      default:
        return 8d;
    }
  }
}
