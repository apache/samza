package org.apache.samza.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduling.Scheduler;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableManager;


public class TaskContextImpl implements TaskContext {
  private final TaskName taskName;
  private final Set<SystemStreamPartition> systemStreamPartitions;
  private final MetricsRegistry taskMetricsRegistry;
  private final Function<String, KeyValueStore> keyValueStoreProvider;
  private final TableManager tableManager;
  private final Scheduler scheduler;
  private final OffsetManager offsetManager;

  private final JobModel jobModel;
  private final StreamMetadataCache streamMetadataCache;
  private final Map<String, Object> objectRegistry = new HashMap<>();

  public TaskContextImpl(TaskName taskName,
      Set<SystemStreamPartition> systemStreamPartitions,
      MetricsRegistry taskMetricsRegistry,
      Function<String, KeyValueStore> keyValueStoreProvider,
      TableManager tableManager,
      Scheduler scheduler,
      OffsetManager offsetManager,
      JobModel jobModel,
      StreamMetadataCache streamMetadataCache) {
    this.taskName = taskName;
    this.systemStreamPartitions = systemStreamPartitions;
    this.taskMetricsRegistry = taskMetricsRegistry;
    this.keyValueStoreProvider = keyValueStoreProvider;
    this.tableManager = tableManager;
    this.scheduler = scheduler;
    this.offsetManager = offsetManager;
    this.jobModel = jobModel;
    this.streamMetadataCache = streamMetadataCache;
  }

  @Override
  public TaskName getTaskName() {
    return this.taskName;
  }

  @Override
  public Set<SystemStreamPartition> getSystemStreamPartitions() {
    return this.systemStreamPartitions;
  }

  @Override
  public MetricsRegistry getTaskMetricsRegistry() {
    return this.taskMetricsRegistry;
  }

  @Override
  public KeyValueStore getStore(String storeName) {
    KeyValueStore store = this.keyValueStoreProvider.apply(storeName);
    if (store == null) {
      throw new IllegalArgumentException(String.format("No store found for storeName: %s", storeName));
    }
    return store;
  }

  @Override
  public Table getTable(String tableId) {
    return this.tableManager.getTable(tableId);
  }

  @Override
  public Scheduler getScheduler() {
    return this.scheduler;
  }

  @Override
  public void setStartingOffset(SystemStreamPartition systemStreamPartition, String offset) {
    this.offsetManager.setStartingOffset(this.taskName, systemStreamPartition, offset);
  }

  // TODO below methods are used by operator code; they should be moved out of this client API to a framework API layer

  public void registerObject(String name, Object value) {
    this.objectRegistry.put(name, value);
  }

  public Object fetchObject(String name) {
    return this.objectRegistry.get(name);
  }

  public JobModel getJobModel() {
    return this.jobModel;
  }

  public StreamMetadataCache getStreamMetadataCache() {
    return this.streamMetadataCache;
  }
}
