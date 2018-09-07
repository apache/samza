package org.apache.samza.context;

import java.util.Collection;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;


/**
 * Contains information at container granularity, provided by the Samza framework, to be used to instantiate an
 * application at runtime.
 * <p>
 * Note that application-defined container-level context is accessible through
 * {@link ApplicationDefinedContainerContext}.
 */
public interface ContainerContext {
  /**
   * @return processor id for this container
   */
  String getProcessorId();

  /**
   * Returns {@link TaskName}s for all tasks running on this container. There is one {@link TaskName} for each task.
   * @return {@link TaskName}s for all tasks running on this container
   */
  Collection<TaskName> getTaskNames();

  /**
   * Returns the {@link MetricsRegistry} for this container. Metrics built using this registry will be associated with
   * the container.
   * @return {@link MetricsRegistry} for this container
   */
  MetricsRegistry getContainerMetricsRegistry();
}
