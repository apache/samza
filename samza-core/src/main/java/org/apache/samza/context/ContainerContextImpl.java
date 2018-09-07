package org.apache.samza.context;

import java.util.Collection;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;


public class ContainerContextImpl implements ContainerContext {
  private final String processorId;
  private final Collection<TaskName> taskNames;
  private final MetricsRegistry containerMetricsRegistry;

  public ContainerContextImpl(String processorId, Collection<TaskName> taskNames, MetricsRegistry containerMetricsRegistry) {
    this.processorId = processorId;
    this.taskNames = taskNames;
    this.containerMetricsRegistry = containerMetricsRegistry;
  }

  @Override
  public String getProcessorId() {
    return this.processorId;
  }

  @Override
  public Collection<TaskName> getTaskNames() {
    return this.taskNames;
  }

  @Override
  public MetricsRegistry getContainerMetricsRegistry() {
    return this.containerMetricsRegistry;
  }
}
