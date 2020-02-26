package org.apache.samza.lineage;

import org.apache.samza.config.Config;


/**
 * The LineageReporterFactory class help to build lineage reporter {@link org.apache.samza.lineage.LineageReporter}
 */
public interface LineageReporterFactory<T> {

  /**
   * Build lineage reporter instance with required information from config, e.g. system stream, serde, etc.
   * @param config Samza job config
   * @return lineage reporter instance
   */
  LineageReporter<T> getLineageReporter(Config config);
}
