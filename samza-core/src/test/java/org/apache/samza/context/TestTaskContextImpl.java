/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.context;

import java.util.function.Function;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduler.CallbackScheduler;
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
  private TaskModel taskModel;
  @Mock
  private MetricsRegistry taskMetricsRegistry;
  @Mock
  private Function<String, KeyValueStore> keyValueStoreProvider;
  @Mock
  private TableManager tableManager;
  @Mock
  private CallbackScheduler callbackScheduler;
  @Mock
  private OffsetManager offsetManager;

  private TaskContextImpl taskContext;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    taskContext =
        new TaskContextImpl(taskModel, taskMetricsRegistry, keyValueStoreProvider, tableManager, callbackScheduler,
            offsetManager, null, null);
    when(this.taskModel.getTaskName()).thenReturn(TASK_NAME);
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