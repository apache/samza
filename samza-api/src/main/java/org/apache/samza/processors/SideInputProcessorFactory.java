package org.apache.samza.processors;

import java.io.Serializable;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;


/**
 * A factory to build {@link SideInputProcessor}s.
 * Implementation needs to return a new instance for every invocation of {@link #createInstance(Config, MetricsRegistry)}
 */
public interface SideInputProcessorFactory extends Serializable {
  /**
   * Creates a new instance of {@link SideInputProcessor}.
   *
   * @param config a config object
   * @param metrics a metrics registry
   *
   * @return An instance of {@link SideInputProcessor}
   */
  SideInputProcessor createInstance(Config config, MetricsRegistry metrics);
}
