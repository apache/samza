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

package org.apache.samza.table.remote;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.task.TaskContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import junit.framework.Assert;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestRemoteTable {
  private <K, V, T extends RemoteReadableTable<K, V>> T getTable(String tableId,
      TableReadFunction<K, V> readFn, TableWriteFunction<K, V> writeFn) {
    return getTable(tableId, readFn, writeFn, null);
  }

  private <K, V, T extends RemoteReadableTable<K, V>> T getTable(String tableId,
      TableReadFunction<K, V> readFn, TableWriteFunction<K, V> writeFn, ExecutorService cbExecutor) {
    RemoteReadableTable<K, V> table;

    TableRateLimiter<K, V> readRateLimiter = mock(TableRateLimiter.class);
    TableRateLimiter<K, V> writeRateLimiter = mock(TableRateLimiter.class);
    doReturn(true).when(readRateLimiter).isRateLimited();
    doReturn(true).when(writeRateLimiter).isRateLimited();

    ExecutorService tableExecutor = Executors.newSingleThreadExecutor();

    if (writeFn == null) {
      table = new RemoteReadableTable<K, V>(tableId, readFn, readRateLimiter, tableExecutor, cbExecutor);
    } else {
      table = new RemoteReadWriteTable<K, V>(tableId, readFn, writeFn, readRateLimiter, writeRateLimiter, tableExecutor, cbExecutor);
    }

    TaskContext taskContext = mock(TaskContext.class);
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(mock(Timer.class)).when(metricsRegistry).newTimer(anyString(), anyString());
    doReturn(mock(Counter.class)).when(metricsRegistry).newCounter(anyString(), anyString());
    doReturn(mock(Gauge.class)).when(metricsRegistry).newGauge(anyString(), any());
    doReturn(metricsRegistry).when(taskContext).getMetricsRegistry();

    SamzaContainerContext containerContext = mock(SamzaContainerContext.class);

    table.init(containerContext, taskContext);

    return (T) table;
  }

  private void doTestGet(boolean sync, boolean error) throws Exception {
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    // Sync is backed by async so needs to mock the async method
    CompletableFuture<String> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture("bar");
    }
    doReturn(future).when(readFn).getAsync(anyString());
    RemoteReadableTable<String, String> table = getTable("testGet-" + sync + error, readFn, null);
    Assert.assertEquals("bar", sync ? table.get("foo") : table.getAsync("foo").get());
    verify(table.readRateLimiter, times(1)).throttle(anyString());
  }

  @Test
  public void testGet() throws Exception {
    doTestGet(true, false);
  }

  @Test
  public void testGetAsync() throws Exception {
    doTestGet(false, false);
  }

  @Test(expected = ExecutionException.class)
  public void testGetAsyncError() throws Exception {
    doTestGet(false, true);
  }

  @Test
  public void testGetMultipleTables() {
    TableReadFunction<String, String> readFn1 = mock(TableReadFunction.class);
    TableReadFunction<String, String> readFn2 = mock(TableReadFunction.class);

    // Sync is backed by async so needs to mock the async method
    doReturn(CompletableFuture.completedFuture("bar1")).when(readFn1).getAsync(anyString());
    doReturn(CompletableFuture.completedFuture("bar2")).when(readFn1).getAsync(anyString());

    RemoteReadableTable<String, String> table1 = getTable("testGetMultipleTables-1", readFn1, null);
    RemoteReadableTable<String, String> table2 = getTable("testGetMultipleTables-2", readFn2, null);

    CompletableFuture<String> future1 = table1.getAsync("foo1");
    CompletableFuture<String> future2 = table2.getAsync("foo2");

    CompletableFuture.allOf(future1, future2)
        .thenAccept(u -> {
            Assert.assertEquals(future1.join(), "bar1");
            Assert.assertEquals(future2.join(), "bar1");
          });
  }

  private void doTestPut(boolean sync, boolean error, boolean isDelete) throws Exception {
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    RemoteReadWriteTable<String, String> table = getTable("testPut-" + sync + error + isDelete,
        mock(TableReadFunction.class), writeFn);
    CompletableFuture<Void> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture(null);
    }
    // Sync is backed by async so needs to mock the async method
    if (isDelete) {
      doReturn(future).when(writeFn).deleteAsync(any());
    } else {
      doReturn(future).when(writeFn).putAsync(any(), any());
    }
    if (sync) {
      table.put("foo", isDelete ? null : "bar");
    } else {
      table.putAsync("foo", isDelete ? null : "bar").get();
    }
    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valCaptor = ArgumentCaptor.forClass(String.class);
    if (isDelete) {
      verify(writeFn, times(1)).deleteAsync(keyCaptor.capture());
    } else {
      verify(writeFn, times(1)).putAsync(keyCaptor.capture(), valCaptor.capture());
      Assert.assertEquals("bar", valCaptor.getValue());
    }
    Assert.assertEquals("foo", keyCaptor.getValue());
    if (isDelete) {
      verify(table.writeRateLimiter, times(1)).throttle(anyString());
    } else {
      verify(table.writeRateLimiter, times(1)).throttle(anyString(), anyString());
    }
  }

  @Test
  public void testPut() throws Exception {
    doTestPut(true, false, false);
  }

  @Test
  public void testPutDelete() throws Exception {
    doTestPut(true, false, true);
  }

  @Test
  public void testPutAsync() throws Exception {
    doTestPut(false, false, false);
  }

  @Test
  public void testPutAsyncDelete() throws Exception {
    doTestPut(false, false, true);
  }

  @Test(expected = ExecutionException.class)
  public void testPutAsyncError() throws Exception {
    doTestPut(false, true, false);
  }

  private void doTestDelete(boolean sync, boolean error) throws Exception {
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    RemoteReadWriteTable<String, String> table = getTable("testDelete-" + sync + error,
        mock(TableReadFunction.class), writeFn);
    CompletableFuture<Void> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture(null);
    }
    // Sync is backed by async so needs to mock the async method
    doReturn(future).when(writeFn).deleteAsync(any());
    ArgumentCaptor<String> argCaptor = ArgumentCaptor.forClass(String.class);
    if (sync) {
      table.delete("foo");
    } else {
      table.deleteAsync("foo").get();
    }
    verify(writeFn, times(1)).deleteAsync(argCaptor.capture());
    Assert.assertEquals("foo", argCaptor.getValue());
    verify(table.writeRateLimiter, times(1)).throttle(anyString());
  }

  @Test
  public void testDelete() throws Exception {
    doTestDelete(true, false);
  }

  @Test
  public void testDeleteAsync() throws Exception {
    doTestDelete(false, false);
  }

  @Test(expected = ExecutionException.class)
  public void testDeleteAsyncError() throws Exception {
    doTestDelete(false, true);
  }

  private void doTestGetAll(boolean sync, boolean error, boolean partial) throws Exception {
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    Map<String, String> res = new HashMap<>();
    res.put("foo1", "bar1");
    if (!partial) {
      res.put("foo2", "bar2");
    }
    CompletableFuture<Map<String, String>> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture(res);
    }
    // Sync is backed by async so needs to mock the async method
    doReturn(future).when(readFn).getAllAsync(any());
    RemoteReadableTable<String, String> table = getTable("testGetAll-" + sync + error + partial, readFn, null);
    Assert.assertEquals(res, sync ? table.getAll(Arrays.asList("foo1", "foo2"))
        : table.getAllAsync(Arrays.asList("foo1", "foo2")).get());
    verify(table.readRateLimiter, times(1)).throttle(anyCollection());
  }

  @Test
  public void testGetAll() throws Exception {
    doTestGetAll(true, false, false);
  }

  @Test
  public void testGetAllAsync() throws Exception {
    doTestGetAll(false, false, false);
  }

  @Test(expected = ExecutionException.class)
  public void testGetAllAsyncError() throws Exception {
    doTestGetAll(false, true, false);
  }

  // Partial result is an acceptable scenario
  @Test
  public void testGetAllPartialResult() throws Exception {
    doTestGetAll(false, false, true);
  }

  public void doTestPutAll(boolean sync, boolean error, boolean hasDelete) throws Exception {
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    RemoteReadWriteTable<String, String> table = getTable("testPutAll-" + sync + error + hasDelete,
        mock(TableReadFunction.class), writeFn);
    CompletableFuture<Void> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture(null);
    }
    // Sync is backed by async so needs to mock the async method
    doReturn(future).when(writeFn).putAllAsync(any());
    if (hasDelete) {
      doReturn(future).when(writeFn).deleteAllAsync(any());
    }
    List<Entry<String, String>> entries = Arrays.asList(
        new Entry<>("foo1", "bar1"), new Entry<>("foo2", hasDelete ? null : "bar2"));
    ArgumentCaptor<List> argCaptor = ArgumentCaptor.forClass(List.class);
    if (sync) {
      table.putAll(entries);
    } else {
      table.putAllAsync(entries).get();
    }
    verify(writeFn, times(1)).putAllAsync(argCaptor.capture());
    if (hasDelete) {
      ArgumentCaptor<List> delArgCaptor = ArgumentCaptor.forClass(List.class);
      verify(writeFn, times(1)).deleteAllAsync(delArgCaptor.capture());
      Assert.assertEquals(Arrays.asList("foo2"), delArgCaptor.getValue());
      Assert.assertEquals(1, argCaptor.getValue().size());
      Assert.assertEquals("foo1", ((Entry) argCaptor.getValue().get(0)).getKey());
      verify(table.writeRateLimiter, times(1)).throttle(anyCollection());
    } else {
      Assert.assertEquals(entries, argCaptor.getValue());
    }
    verify(table.writeRateLimiter, times(1)).throttleRecords(anyCollection());
  }

  @Test
  public void testPutAll() throws Exception {
    doTestPutAll(true, false, false);
  }

  @Test
  public void testPutAllHasDelete() throws Exception {
    doTestPutAll(true, false, true);
  }

  @Test
  public void testPutAllAsync() throws Exception {
    doTestPutAll(false, false, false);
  }

  @Test
  public void testPutAllAsyncHasDelete() throws Exception {
    doTestPutAll(false, false, true);
  }

  @Test(expected = ExecutionException.class)
  public void testPutAllAsyncError() throws Exception {
    doTestPutAll(false, true, false);
  }

  public void doTestDeleteAll(boolean sync, boolean error) throws Exception {
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    RemoteReadWriteTable<String, String> table = getTable("testDeleteAll-" + sync + error,
        mock(TableReadFunction.class), writeFn);
    CompletableFuture<Void> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture(null);
    }
    // Sync is backed by async so needs to mock the async method
    doReturn(future).when(writeFn).deleteAllAsync(any());
    List<String> keys = Arrays.asList("foo1", "foo2");
    ArgumentCaptor<List> argCaptor = ArgumentCaptor.forClass(List.class);
    if (sync) {
      table.deleteAll(keys);
    } else {
      table.deleteAllAsync(keys).get();
    }
    verify(writeFn, times(1)).deleteAllAsync(argCaptor.capture());
    Assert.assertEquals(keys, argCaptor.getValue());
    verify(table.writeRateLimiter, times(1)).throttle(anyCollection());
  }

  @Test
  public void testDeleteAll() throws Exception {
    doTestDeleteAll(true, false);
  }

  @Test
  public void testDeleteAllAsync() throws Exception {
    doTestDeleteAll(false, false);
  }

  @Test(expected = ExecutionException.class)
  public void testDeleteAllAsyncError() throws Exception {
    doTestDeleteAll(false, true);
  }

  @Test
  public void testFlush() {
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    RemoteReadWriteTable<String, String> table = getTable("testFlush", mock(TableReadFunction.class), writeFn);
    table.flush();
    verify(writeFn, times(1)).flush();
  }

  @Test
  public void testGetWithCallbackExecutor() throws Exception {
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    // Sync is backed by async so needs to mock the async method
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(anyString());
    RemoteReadableTable<String, String> table = getTable("testGetWithCallbackExecutor", readFn, null,
        Executors.newSingleThreadExecutor());
    Thread testThread = Thread.currentThread();

    table.getAsync("foo").thenAccept(result -> {
        Assert.assertEquals("bar", result);
        // Must be executed on the executor thread
        Assert.assertNotSame(testThread, Thread.currentThread());
      });
  }
}
