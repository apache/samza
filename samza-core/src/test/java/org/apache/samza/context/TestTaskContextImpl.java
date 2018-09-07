package org.apache.samza.context;

import java.util.Set;
import java.util.function.Function;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduling.Scheduler;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.TableManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestTaskContextImpl {
  private static final TaskName TASK_NAME = new TaskName("myTaskName");

  @Mock
  private Set<SystemStreamPartition> _systemStreamPartitions;
  @Mock
  private MetricsRegistry _taskMetricsRegistry;
  @Mock
  private Function<String, KeyValueStore> _keyValueStoreProvider;
  @Mock
  private TableManager _tableManager;
  @Mock
  private Scheduler _scheduler;
  @Mock
  private OffsetManager _offsetManager;

  private TaskContextImpl _taskContext;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    _taskContext = new TaskContextImpl(TASK_NAME, _systemStreamPartitions, _taskMetricsRegistry, _keyValueStoreProvider,
        _tableManager, _scheduler, _offsetManager);
  }

  /**
   * Given that there is a store corresponding to the storeName, getStore should return the store.
   */
  @Test
  public void testGetStore() {
    KeyValueStore store = mock(KeyValueStore.class);
    when(_keyValueStoreProvider.apply("myStore")).thenReturn(store);
    assertEquals(store, _taskContext.getStore("myStore"));
  }

  /**
   * Given that there is not a store corresponding to the storeName, getStore should throw an exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetMissingStore() {
    KeyValueStore store = mock(KeyValueStore.class);
    when(_keyValueStoreProvider.apply("myStore")).thenReturn(null);
    assertEquals(store, _taskContext.getStore("myStore"));
  }

  /**
   * Given an SSP and offset, setStartingOffset should delegate to the offset manager.
   */
  @Test
  public void testSetStartingOffset() {
    SystemStreamPartition ssp = new SystemStreamPartition("mySystem", "myStream", new Partition(0));
    _taskContext.setStartingOffset(ssp, "123");
    verify(_offsetManager).setStartingOffset(TASK_NAME, ssp, "123");
  }
}