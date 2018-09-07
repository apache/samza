package org.apache.samza.context;

import java.util.Set;
import java.util.function.Function;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduling.Scheduler;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableManager;


public class TaskContextImpl implements TaskContext {
  private final TaskName _taskName;
  private final Set<SystemStreamPartition> _systemStreamPartitions;
  private final MetricsRegistry _taskMetricsRegistry;
  private final Function<String, KeyValueStore> _keyValueStoreProvider;
  private final TableManager _tableManager;
  private final Scheduler _scheduler;
  private final OffsetManager _offsetManager;

  public TaskContextImpl(TaskName taskName, Set<SystemStreamPartition> systemStreamPartitions,
      MetricsRegistry taskMetricsRegistry, Function<String, KeyValueStore> keyValueStoreProvider,
      TableManager tableManager, Scheduler scheduler, OffsetManager offsetManager) {
    _taskName = taskName;
    _systemStreamPartitions = systemStreamPartitions;
    _taskMetricsRegistry = taskMetricsRegistry;
    _keyValueStoreProvider = keyValueStoreProvider;
    _tableManager = tableManager;
    _scheduler = scheduler;
    _offsetManager = offsetManager;
  }

  @Override
  public TaskName getTaskName() {
    return _taskName;
  }

  @Override
  public Set<SystemStreamPartition> getSystemStreamPartitions() {
    return _systemStreamPartitions;
  }

  @Override
  public MetricsRegistry getTaskMetricsRegistry() {
    return _taskMetricsRegistry;
  }

  @Override
  public KeyValueStore getStore(String storeName) {
    KeyValueStore store = _keyValueStoreProvider.apply(storeName);
    if (store == null) {
      throw new IllegalArgumentException(String.format("No store found for storeName: %s", storeName));
    }
    return store;
  }

  @Override
  public Table getTable(String tableId) {
    return _tableManager.getTable(tableId);
  }

  @Override
  public Scheduler getScheduler() {
    return _scheduler;
  }

  @Override
  public void setStartingOffset(SystemStreamPartition systemStreamPartition, String offset) {
    _offsetManager.setStartingOffset(_taskName, systemStreamPartition, offset);
  }
}
