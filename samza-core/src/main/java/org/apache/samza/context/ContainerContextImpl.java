package org.apache.samza.context;

import java.util.Collection;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;


public class ContainerContextImpl implements ContainerContext {
  private final String _processorId;
  private final Collection<TaskName> _taskNames;
  private final MetricsRegistry _containerMetricsRegistry;

  public ContainerContextImpl(String processorId, Collection<TaskName> taskNames, MetricsRegistry containerMetricsRegistry) {
    _processorId = processorId;
    _taskNames = taskNames;
    _containerMetricsRegistry = containerMetricsRegistry;
  }

  @Override
  public String getProcessorId() {
    return _processorId;
  }

  @Override
  public Collection<TaskName> getTaskNames() {
    return _taskNames;
  }

  @Override
  public MetricsRegistry getContainerMetricsRegistry() {
    return _containerMetricsRegistry;
  }
}
