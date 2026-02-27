package com.ibm.flexdata.metadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.calcite.rel.type.RelDataType;


/** Record holding all metadata statistics for a column. */
public record ColStats(
    String colName,
    String colType,
    long numDistinctValues,
    long numNulls,
    double avgColLen,
    long maxColLen,
    long numTrues,
    long numFalses,
    Range range,
    boolean primaryKey,
    boolean estimated,
    byte[] bitVectors) {

  public static final int BYTES_PER_CHARACTER = Character.SIZE / Byte.SIZE;

  /** Builder used for creating a ColStats instance. */
  public static final class Builder {

    private String colName;
    private String colType;
    private long numDistinctValues;
    private long numNulls;
    private double avgColLen = -1;
    private long maxColLen = -1;
    private long numTrues;
    private long numFalses;
    private Range range;
    private boolean primaryKey;
    private boolean estimated;
    private byte[] bitVectors;

    /**
     * Set the name of the column.
     *
     * @param name name
     * @return builder
     */
    public Builder name(final String name) {
      this.colName = name;
      return this;
    }

    /**
     * Set the type of the column.
     *
     * <p>Note, if type has already been set, this is a noop
     *
     * @param colType column type
     * @return builder
     */
    public Builder type(final RelDataType colType) {
      this.colType = colType.toString();
      if (this.avgColLen == -1) {
        this.avgColLen = TypeSizes.getDefaultSize(colType);
      }
      if (this.maxColLen == -1) {
        this.maxColLen = TypeSizes.getDefaultSize(colType).longValue();
      }
      return this;
    }


    /** Set number of distinct values (NDV). */
    public Builder numDistinctValues(final long ndv) {
      this.numDistinctValues = ndv;
      return this;
    }

    /** Set number of nulls. */
    public Builder numNulls(final long numNulls) {
      this.numNulls = numNulls;
      return this;
    }

    /** Override the average column length in bytes. */
    public Builder avgColLen(final double avgColLen) {
      this.avgColLen = avgColLen;
      return this;
    }

    /** Override the maximum column length in bytes. */
    public Builder maxColLen(final long maxColLen) {
      this.maxColLen = maxColLen;
      return this;
    }

    /** Mark whether this column is (heuristically) a primary key. */
    public Builder primaryKey(final boolean primaryKey) {
      this.primaryKey = primaryKey;
      return this;
    }

    /** Set the numeric range for this column. */
    public Builder range(final Number min, final Number max) {
      if (min != null && max != null) {
        this.range = new Range(min, max);
      }
      return this;
    }

    /** Mark that values were estimated rather than exact. */
    public Builder estimated(final boolean estimated) {
      this.estimated = estimated;
      return this;
    }


    /**
     * Build the ColStats object.
     *
     * <p>Note, name and type fields must be set.
     *
     * @return colStats object
     */
    public ColStats build() {

      if (colType == null || colName == null || avgColLen == -1 || maxColLen == -1) {
        throw new IllegalStateException("Name and type must be defined for " + this);
      }

      return new ColStats(
          colName,
          colType,
          numDistinctValues,
          numNulls,
          avgColLen,
          maxColLen,
          numTrues,
          numFalses,
          range,
          primaryKey,
          estimated,
          bitVectors);
    }

    
  }
}
