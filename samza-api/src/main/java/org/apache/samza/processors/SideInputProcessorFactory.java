package org.apache.samza.processors;

import java.io.Serializable;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;


/**
 *
 */
public interface SideInputProcessorFactory extends Serializable {
  /**
   *
   * @param config a config object
   * @param metrics a metrics registry
   *
   * @return An instance of {@link SideInputProcessor}
   */
  SideInputProcessor createInstance(Config config, MetricsRegistry metrics);
}
