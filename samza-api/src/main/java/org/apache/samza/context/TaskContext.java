package org.apache.samza.context;

import java.util.Set;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduling.Scheduler;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;


/**
 * Contains information at task granularity, provided by the Samza framework, to be used to instantiate an application
 * at runtime.
 * <p>
 * Note that application-defined task-level context is accessible through {@link ApplicationDefinedTaskContext}.
 */
public interface TaskContext {
  /**
   * @return {@link TaskName} for this task
   */
  TaskName getTaskName();

  /**
   * Returns all of the input {@link SystemStreamPartition}s for this task. This does not include side input
   * {@link SystemStreamPartition}s.
   * @return all of the input {@link SystemStreamPartition}s for this task
   */
  Set<SystemStreamPartition> getSystemStreamPartitions();

  /**
   * Returns the {@link MetricsRegistry} for this task. Metrics built using this registry will be associated with the
   * task.
   * @return {@link MetricsRegistry} for this task
   */
  MetricsRegistry getTaskMetricsRegistry();

  /**
   * @param storeName name of the {@link KeyValueStore} to get
   * @return {@link KeyValueStore} corresponding to the {@code storeName}
   * @throws IllegalArgumentException if there is no store associated with {@code storeName}
   */
  KeyValueStore getStore(String storeName);

  /**
   * @param tableId id of the {@link Table} to get
   * @return {@link Table} corresponding to the {@code tableId}
   * @throws IllegalArgumentException if there is no table associated with {@code tableId}
   */
  Table getTable(String tableId);

  /**
   * @return {@link Scheduler} which can be used to delay execution of some logic
   */
  Scheduler getScheduler();

  /**
   * Set the starting offset for the given {@link SystemStreamPartition}. Offsets can only be set for a
   * {@link SystemStreamPartition} assigned to this task (as returned by {@link #getSystemStreamPartitions()}); trying
   * to set the offset for any other partition will have no effect.
   *
   * NOTE: this feature is experimental, and the API may change in a future release.
   *
   * @param systemStreamPartition {@link org.apache.samza.system.SystemStreamPartition} whose offset should be set
   * @param offset to set for the given {@link org.apache.samza.system.SystemStreamPartition}
   */
  @InterfaceStability.Evolving
  void setStartingOffset(SystemStreamPartition systemStreamPartition, String offset);
}