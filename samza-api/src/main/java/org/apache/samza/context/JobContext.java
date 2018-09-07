package org.apache.samza.context;

import org.apache.samza.config.Config;


/**
 * Contains information at job granularity, provided by the Samza framework, to be used to instantiate an application at
 * runtime.
 */
public interface JobContext {
  /**
   * @return final configuration for this job
   */
  Config getConfig();
}
