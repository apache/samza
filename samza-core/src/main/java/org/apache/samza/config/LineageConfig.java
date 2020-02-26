package org.apache.samza.config;

import java.util.Optional;


/**
 * Config helper methods related to Samza job lineage
 */
public class LineageConfig extends MapConfig {

  public static final String LINEAGE_FACTORY = "lineage.factory";
  public static final String LINEAGE_REPORTER_FACTORY = "lineage.reporter.factory";
  public static final String LINEAGE_REPORTER_STREAM = "lineage.reporter.stream";

  public LineageConfig(Config config) {
    super(config);
  }

  public Optional<String> getLineageFactoryClassName() {
    return Optional.ofNullable(get(LINEAGE_FACTORY));
  }

  public Optional<String> getLineageReporterFactoryClassName() {
    return Optional.ofNullable(get(LINEAGE_REPORTER_FACTORY));
  }

  public Optional<String> getLineageReporterStreamName() {
    return Optional.ofNullable(get(LINEAGE_REPORTER_STREAM));
  }
}
