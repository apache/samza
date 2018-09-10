package org.apache.samza.context;

import java.util.function.Function;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestTaskContextImpl {
  private static final TaskName TASK_NAME = new TaskName("myTaskName");

  @Mock
  private Function<String, KeyValueStore> keyValueStoreProvider;
  @Mock
  private OffsetManager offsetManager;

  private TaskContextImpl taskContext;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    taskContext =
        new TaskContextImpl(TASK_NAME, null, null, keyValueStoreProvider, null, null, offsetManager, null, null);
  }

  /**
   * Given that there is a store corresponding to the storeName, getStore should return the store.
   */
  @Test
  public void testGetStore() {
    KeyValueStore store = mock(KeyValueStore.class);
    when(keyValueStoreProvider.apply("myStore")).thenReturn(store);
    assertEquals(store, taskContext.getStore("myStore"));
  }

  /**
   * Given that there is not a store corresponding to the storeName, getStore should throw an exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetMissingStore() {
    KeyValueStore store = mock(KeyValueStore.class);
    when(keyValueStoreProvider.apply("myStore")).thenReturn(null);
    assertEquals(store, taskContext.getStore("myStore"));
  }

  /**
   * Given an SSP and offset, setStartingOffset should delegate to the offset manager.
   */
  @Test
  public void testSetStartingOffset() {
    SystemStreamPartition ssp = new SystemStreamPartition("mySystem", "myStream", new Partition(0));
    taskContext.setStartingOffset(ssp, "123");
    verify(offsetManager).setStartingOffset(TASK_NAME, ssp, "123");
  }

  /**
   * Given a registered object, fetchObject should get it. If an object is not registered at a key, then fetchObject
   * should return null.
   */
  @Test
  public void testRegisterAndFetchObject() {
    String value = "hello world";
    taskContext.registerObject("key", value);
    assertEquals(value, taskContext.fetchObject("key"));
    assertNull(taskContext.fetchObject("not a key"));
  }
}