package com.ibm.flexdata.splitter;

import com.ibm.flexdata.metadata.MetadataProviderCustom;
import com.ibm.flexdata.metastore.SoStatsCatalog;
import com.ibm.flexdata.metastore.SoStatsCatalogImpl;
import com.ibm.flexdata.metastore.SoStatsProvider;
import com.ibm.flexdata.metastore.StatsProvider;
import java.util.List;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;

public final class MetadataProviderFactory {
  private MetadataProviderFactory() {}

  /** Plain Calcite default metadata provider. */
  public static JaninoRelMetadataProvider defaultProvider() {
    return JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE);
  }

  /**
   * Custom provider: uses dataset-specific relations stats via SoStatsRegistry +
   * SoStatsProvider + MetadataProviderCustom.
   */
  public static JaninoRelMetadataProvider customProvider() {
    return customProvider(Dataset.STACKOVERFLOW);
  }

  public static JaninoRelMetadataProvider customProvider(Dataset dataset) {
    // 1) Load JSON stats
    SoStatsRegistry registry = SoStatsRegistry.getInstance(dataset);

    // 2) Wrap them into a SoStatsCatalog view
    SoStatsCatalog catalog = new SoStatsCatalogImpl(registry);

    // 3) Build StatsProvider backed by that catalog
    StatsProvider statsProvider = new SoStatsProvider(catalog);

    // 4) Build Calcite metadata provider that uses our custom RowCount/Selectivity/Size handlers
    return MetadataProviderCustom.getCustomMetadataProvider(statsProvider);
  }
}
