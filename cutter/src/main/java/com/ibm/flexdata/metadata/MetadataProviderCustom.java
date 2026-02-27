package com.ibm.flexdata.metadata;

import com.ibm.flexdata.metastore.StatsProvider;
import java.util.List;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;

/** Metadata providers that rely on a custom metadata source. */
public final class MetadataProviderCustom {
  private MetadataProviderCustom() {}

  /**
   * Initialize the metadata provider with the provided metadata source.
   *
   * @param statsProvider metadata source
   * @return metadata provider
   */
  public static JaninoRelMetadataProvider getCustomMetadataProvider(
      final StatsProvider statsProvider) {
    return JaninoRelMetadataProvider.of(
        ChainedRelMetadataProvider.of(
            List.of(
                new RelMdRowCountCustom(statsProvider).getRelMetadataProvider(),
                new RelMdSelectivityCustom(statsProvider).getRelMetadataProvider(),
                RelMdPredicates.SOURCE,
                new RelMdDistinctRowCountCustom(statsProvider).getRelMetadataProvider(),
                RelMdUniqueKeys.SOURCE,
                RelMdColumnUniqueness.SOURCE,
                RelMdMaxRowCount.SOURCE,
                new RelMdSizeCustom(statsProvider).getRelMetadataProvider(),
                RelMdColumnOrigins.SOURCE,
                RelMdExplainVisibility.SOURCE,
                RelMdCollation.SOURCE,
                RelMdPercentageOriginalRows.SOURCE)));
  }


}
