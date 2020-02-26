package org.apache.samza.lineage;

import org.apache.samza.config.Config;


/**
 * The LineageFactory class helps parse and generate job lineage information from Samza job config.
 */
public interface LineageFactory<T> {

  /**
   * Parse and generate job lineage data model from Samza job config, the data model includes job's inputs, outputs and
   * other metadata information.
   * The config should include full inputs and outputs information, those information are usually populated by job
   * planner and config rewriters.
   * It is the user's responsibility to make sure config contains all required information before call this function.
   *
   * @param config Samza job config
   * @return Samza job lineage data model
   */
  T getLineage(Config config);
}
