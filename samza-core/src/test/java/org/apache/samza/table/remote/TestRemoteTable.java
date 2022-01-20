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

import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.ratelimit.AsyncRateLimitedTable;
import org.apache.samza.table.retry.AsyncRetriableTable;
import org.apache.samza.table.retry.TableRetryPolicy;

import org.apache.samza.testUtils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestRemoteTable {

  public static Context getMockContext() {
    Context context = new MockContext();
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doAnswer(args -> new Timer((String) args.getArguments()[0])).when(metricsRegistry).newTimer(anyString(), anyString());
    doAnswer(args -> new Counter((String) args.getArguments()[0])).when(metricsRegistry).newCounter(anyString(), anyString());
    doAnswer(args -> new Gauge((String) args.getArguments()[0], 0)).when(metricsRegistry).newGauge(anyString(), any());
    doReturn(metricsRegistry).when(context.getContainerContext()).getContainerMetricsRegistry();
    return context;
  }

  private <K, V, U, T extends RemoteTable<K, V, U>> T getTable(String tableId, TableReadFunction<K, V> readFn,
      TableWriteFunction<K, V, U> writeFn, boolean retry) {
    return getTable(tableId, readFn, writeFn, null, retry);
  }

  private <K, V, U, T extends RemoteTable<K, V, U>> T getTable(String tableId, TableReadFunction<K, V> readFn,
      TableWriteFunction<K, V, U> writeFn, ExecutorService cbExecutor, boolean retry) {

    TableRateLimiter<K, V> readRateLimiter = mock(TableRateLimiter.class);
    TableRateLimiter<K, V> writeRateLimiter = mock(TableRateLimiter.class);
    TableRateLimiter<K, U> updateRateLimiter = mock(TableRateLimiter.class);

    TableRetryPolicy readPolicy = retry ? new TableRetryPolicy() : null;
    TableRetryPolicy writePolicy = retry ? new TableRetryPolicy() : null;

    ExecutorService rateLimitingExecutor = Executors.newSingleThreadExecutor();
    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();

    RemoteTable<K, V, U> table = new RemoteTable<>(tableId, readFn, writeFn,
        readRateLimiter, writeRateLimiter, updateRateLimiter, rateLimitingExecutor,
        readPolicy, writePolicy, retryExecutor, null, null, cbExecutor);
    table.init(getMockContext());
    if (readFn != null) {
      verify(readFn, times(1)).init(any(), any());
    }
    if (writeFn != null) {
      verify(writeFn, times(1)).init(any(), any());
    }
    return (T) table;
  }

  private void doTestGet(boolean sync, boolean error, boolean retry) {
    String tableId = "testGet-" + sync + error + retry;
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    // Sync is backed by async so needs to mock the async method
    CompletableFuture<String> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
      if (!retry) {
        doReturn(future).when(readFn).getAsync(anyString());
      } else {
        final int[] times = new int[] {0};
        doAnswer(args -> times[0]++ == 0 ? future : CompletableFuture.completedFuture("bar"))
            .when(readFn).getAsync(anyString());
      }
    } else {
      future = CompletableFuture.completedFuture("bar");
      doReturn(future).when(readFn).getAsync(anyString());
    }
    if (retry) {
      doReturn(true).when(readFn).isRetriable(any());
    }
    RemoteTable<String, String, Void> table = getTable(tableId, readFn, null, retry);
    Assert.assertEquals("bar", sync ? table.get("foo") : table.getAsync("foo").join());
    verify(table.readRateLimiter, times(error && retry ? 2 : 1)).throttle(anyString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnNullReadFnAndWriteFn() {
    getTable("id", null, null, false);
  }

  @Test
  public void testSucceedValidationOnNullReadFn() {
    RemoteTable<String, String, Void> table = getTable("tableId", null, mock(TableWriteFunction.class), false);
    Assert.assertNotNull(table);
  }

  @Test
  public void testInit() {
    String tableId = "testInit";
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    RemoteTable<String, String, String> table = getTable(tableId, readFn, writeFn, true);
    // AsyncRetriableTable
    AsyncReadWriteUpdateTable innerTable = TestUtils.getFieldValue(table, "asyncTable");
    Assert.assertTrue(innerTable instanceof AsyncRetriableTable);
    Assert.assertNotNull(TestUtils.getFieldValue(innerTable, "readRetryMetrics"));
    Assert.assertNotNull(TestUtils.getFieldValue(innerTable, "writeRetryMetrics"));
    // AsyncRateLimitedTable
    innerTable = TestUtils.getFieldValue(innerTable, "table");
    Assert.assertTrue(innerTable instanceof AsyncRateLimitedTable);
    // AsyncRemoteTable
    innerTable = TestUtils.getFieldValue(innerTable, "table");
    Assert.assertTrue(innerTable instanceof AsyncRemoteTable);
    // Verify table functions are initialized
    verify(readFn, times(1)).init(any(), any());
    verify(writeFn, times(1)).init(any(), any());
  }

  @Test
  public void testGet() {
    doTestGet(true, false, false);
  }

  @Test
  public void testGetAsync() {
    doTestGet(false, false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testGetAsyncError() {
    doTestGet(false, true, false);
  }

  @Test
  public void testGetAsyncErrorRetried() {
    doTestGet(false, true, true);
  }

  @Test
  public void testGetMultipleTables() {
    TableReadFunction<String, String> readFn1 = mock(TableReadFunction.class);
    TableReadFunction<String, String> readFn2 = mock(TableReadFunction.class);

    // Sync is backed by async so needs to mock the async method
    doReturn(CompletableFuture.completedFuture("bar1")).when(readFn1).getAsync(anyString());
    doReturn(CompletableFuture.completedFuture("bar2")).when(readFn1).getAsync(anyString());

    RemoteTable<String, String, Void> table1 = getTable("testGetMultipleTables-1", readFn1, null, false);
    RemoteTable<String, String, Void> table2 = getTable("testGetMultipleTables-2", readFn2, null, false);

    CompletableFuture<String> future1 = table1.getAsync("foo1");
    CompletableFuture<String> future2 = table2.getAsync("foo2");

    CompletableFuture.allOf(future1, future2)
        .thenAccept(u -> {
          Assert.assertEquals(future1.join(), "bar1");
          Assert.assertEquals(future2.join(), "bar1");
        });
  }

  public void doTestRead(boolean sync, boolean error) {
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    RemoteTable<String, String, Void> table = getTable("testRead-" + sync + error,
        readFn, mock(TableWriteFunction.class), false);
    CompletableFuture<?> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture(5);
    }
    // Sync is backed by async so needs to mock the async method
    doReturn(future).when(readFn).readAsync(anyInt(), any());

    int readResult = sync
        ? table.read(1, 2)
        : (Integer) table.readAsync(1, 2).join();
    verify(readFn, times(1)).readAsync(anyInt(), any());
    Assert.assertEquals(5, readResult);
    verify(table.readRateLimiter, times(1)).throttle(anyInt(), any());
  }

  @Test
  public void testRead() {
    doTestRead(true, false);
  }

  @Test
  public void testReadAsync() {
    doTestRead(false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testReadAsyncError() {
    doTestRead(false, true);
  }

  private void doTestPut(boolean sync, boolean error, boolean isDelete, boolean retry) {
    String tableId = "testPut-" + sync + error + isDelete + retry;
    TableWriteFunction<String, String, String> mockWriteFn = mock(TableWriteFunction.class);
    TableWriteFunction<String, String, String> writeFn = mockWriteFn;
    CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
    CompletableFuture<Void> failureFuture = new CompletableFuture();
    failureFuture.completeExceptionally(new RuntimeException("Test exception"));
    if (!error) {
      if (isDelete) {
        doReturn(successFuture).when(writeFn).deleteAsync(any());
      } else {
        doReturn(successFuture).when(writeFn).putAsync(any(), any());
      }
    } else if (!retry) {
      if (isDelete) {
        doReturn(failureFuture).when(writeFn).deleteAsync(any());
      } else {
        doReturn(failureFuture).when(writeFn).putAsync(any(), any());
      }
    } else {
      doReturn(true).when(writeFn).isRetriable(any());
      final int[] times = new int[] {0};
      if (isDelete) {
        doAnswer(args -> times[0]++ == 0 ? failureFuture : successFuture).when(writeFn).deleteAsync(any());
      } else {
        doAnswer(args -> times[0]++ == 0 ? failureFuture : successFuture).when(writeFn).putAsync(any(), any());
      }
    }
    RemoteTable<String, String, String> table = getTable(tableId, mock(TableReadFunction.class), writeFn, retry);
    if (sync) {
      table.put("foo", isDelete ? null : "bar");
    } else {
      table.putAsync("foo", isDelete ? null : "bar").join();
    }
    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valCaptor = ArgumentCaptor.forClass(String.class);
    if (isDelete) {
      verify(mockWriteFn, times(1)).deleteAsync(keyCaptor.capture());
    } else {
      verify(mockWriteFn, times(retry ? 2 : 1)).putAsync(keyCaptor.capture(), valCaptor.capture());
      Assert.assertEquals("bar", valCaptor.getValue());
    }
    Assert.assertEquals("foo", keyCaptor.getValue());
    if (isDelete) {
      verify(table.writeRateLimiter, times(error && retry ? 2 : 1)).throttle(anyString());
    } else {
      verify(table.writeRateLimiter, times(error && retry ? 2 : 1)).throttle(anyString(), anyString());
    }
  }

  @Test
  public void testPut() {
    doTestPut(true, false, false, false);
  }

  @Test
  public void testPutDelete() {
    doTestPut(true, false, true, false);
  }

  @Test
  public void testPutAsync() {
    doTestPut(false, false, false, false);
  }

  @Test
  public void testPutAsyncDelete() {
    doTestPut(false, false, true, false);
  }

  @Test(expected = RuntimeException.class)
  public void testPutAsyncError() {
    doTestPut(false, true, false, false);
  }

  @Test
  public void testPutAsyncErrorRetried() {
    doTestPut(false, true, false, true);
  }

  public void doTestUpdate(boolean sync, boolean error) {
    String tableId = "testUpdate-" + sync + error;
    TableWriteFunction<String, String, String> mockWriteFn = mock(TableWriteFunction.class);
    TableWriteFunction<String, String, String> writeFn = mockWriteFn;
    CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
    CompletableFuture<Void> failureFuture = new CompletableFuture();
    failureFuture.completeExceptionally(new RuntimeException("Test exception"));
    if (!error) {
      doReturn(successFuture).when(writeFn).updateAsync(any(), any());
    } else {
      doReturn(failureFuture).when(writeFn).updateAsync(any(), any());
    }

    RemoteTable<String, String, String> table = getTable(tableId, mock(TableReadFunction.class), writeFn, false);
    if (sync) {
      table.update("foo", "bar");
    } else {
      table.updateAsync("foo", "bar").join();
    }
    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> updateCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockWriteFn, times(1))
        .updateAsync(keyCaptor.capture(), updateCaptor.capture());
  }

  @Test
  public void testUpdate() {
    doTestUpdate(true, false);
  }

  @Test
  public void testUpdateAsync() {
    doTestUpdate(false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testUpdateAsyncError() {
    doTestUpdate(false, true);
  }

  private void doTestDelete(boolean sync, boolean error) {
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    RemoteTable<String, String, String> table = getTable("testDelete-" + sync + error,
        mock(TableReadFunction.class), writeFn, false);
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
      table.deleteAsync("foo").join();
    }
    verify(writeFn, times(1)).deleteAsync(argCaptor.capture());
    Assert.assertEquals("foo", argCaptor.getValue());
    verify(table.writeRateLimiter, times(1)).throttle(anyString());
  }

  @Test
  public void testDelete() {
    doTestDelete(true, false);
  }

  @Test
  public void testDeleteAsync() {
    doTestDelete(false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testDeleteAsyncError() {
    doTestDelete(false, true);
  }

  private void doTestGetAll(boolean sync, boolean error, boolean partial) {
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
    RemoteTable<String, String, Void> table = getTable("testGetAll-" + sync + error + partial, readFn, null, false);
    Assert.assertEquals(res, sync ? table.getAll(Arrays.asList("foo1", "foo2"))
        : table.getAllAsync(Arrays.asList("foo1", "foo2")).join());
    verify(table.readRateLimiter, times(1)).throttle(anyCollection());
  }

  @Test
  public void testGetAll() {
    doTestGetAll(true, false, false);
  }

  @Test
  public void testGetAllAsync() {
    doTestGetAll(false, false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testGetAllAsyncError() {
    doTestGetAll(false, true, false);
  }

  // Partial result is an acceptable scenario
  @Test
  public void testGetAllPartialResult() {
    doTestGetAll(false, false, true);
  }

  public void doTestPutAll(boolean sync, boolean error, boolean hasDelete) {
    TableWriteFunction<String, String, Void> writeFn = mock(TableWriteFunction.class);
    RemoteTable<String, String, Void> table = getTable("testPutAll-" + sync + error + hasDelete,
        mock(TableReadFunction.class), writeFn, false);
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
      table.putAllAsync(entries).join();
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
  public void testPutAll() {
    doTestPutAll(true, false, false);
  }

  @Test
  public void testPutAllHasDelete() {
    doTestPutAll(true, false, true);
  }

  @Test
  public void testPutAllAsync() {
    doTestPutAll(false, false, false);
  }

  @Test
  public void testPutAllAsyncHasDelete() {
    doTestPutAll(false, false, true);
  }

  @Test(expected = RuntimeException.class)
  public void testPutAllAsyncError() {
    doTestPutAll(false, true, false);
  }

  public void doTestUpdateAll(boolean sync, boolean error) {
    String tableId = "testUpdateAll-" + sync + error;
    TableWriteFunction<String, String, String> mockWriteFn = mock(TableWriteFunction.class);
    TableWriteFunction<String, String, String> writeFn = mockWriteFn;
    CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
    CompletableFuture<Void> failureFuture = new CompletableFuture();
    failureFuture.completeExceptionally(new RuntimeException("Test exception"));
    if (!error) {
      doReturn(successFuture).when(writeFn).updateAllAsync(anyCollection());
    } else {
      doReturn(failureFuture).when(writeFn).updateAllAsync(anyCollection());
    }

    List<Entry<String, String>> updates = Arrays.asList(new Entry<>("foo1", "bar1"), new Entry<>("foo2", "bar2"));

    RemoteTable<String, String, String> table = getTable(tableId, mock(TableReadFunction.class), writeFn, false);
    if (sync) {
      table.updateAll(updates);
    } else {
      table.updateAllAsync(updates).join();
    }
    ArgumentCaptor<List> argCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockWriteFn, times(1)).updateAllAsync(argCaptor.capture());
  }

  @Test
  public void testUpdateAll() {
    doTestUpdateAll(true, false);
  }

  @Test
  public void testUpdateAllAsync() {
    doTestUpdateAll(false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testUpdateAllAsyncError() {
    doTestUpdateAll(false, true);
  }

  public void doTestDeleteAll(boolean sync, boolean error) {
    TableWriteFunction<String, String, Void> writeFn = mock(TableWriteFunction.class);
    RemoteTable<String, String, Void> table = getTable("testDeleteAll-" + sync + error,
        mock(TableReadFunction.class), writeFn, false);
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
      table.deleteAllAsync(keys).join();
    }
    verify(writeFn, times(1)).deleteAllAsync(argCaptor.capture());
    Assert.assertEquals(keys, argCaptor.getValue());
    verify(table.writeRateLimiter, times(1)).throttle(anyCollection());
  }

  @Test
  public void testDeleteAll() {
    doTestDeleteAll(true, false);
  }

  @Test
  public void testDeleteAllAsync() {
    doTestDeleteAll(false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testDeleteAllAsyncError() {
    doTestDeleteAll(false, true);
  }

  public void doTestWrite(boolean sync, boolean error) {
    TableWriteFunction<String, String, Void> writeFn = mock(TableWriteFunction.class);
    RemoteTable<String, String, Void> table = getTable("testWrite-" + sync + error,
        mock(TableReadFunction.class), writeFn, false);
    CompletableFuture<?> future;
    if (error) {
      future = new CompletableFuture();
      future.completeExceptionally(new RuntimeException("Test exception"));
    } else {
      future = CompletableFuture.completedFuture(5);
    }
    // Sync is backed by async so needs to mock the async method
    doReturn(future).when(writeFn).writeAsync(anyInt(), any());

    int writeResult = sync
        ? table.write(1, 2)
        : (Integer) table.writeAsync(1, 2).join();
    verify(writeFn, times(1)).writeAsync(anyInt(), any());
    Assert.assertEquals(5, writeResult);
    verify(table.writeRateLimiter, times(1)).throttle(anyInt(), any());
  }

  @Test
  public void testWrite() {
    doTestWrite(true, false);
  }

  @Test
  public void testWriteAsync() {
    doTestWrite(false, false);
  }

  @Test(expected = RuntimeException.class)
  public void testWriteAsyncError() {
    doTestWrite(false, true);
  }

  @Test
  public void testFlush() {
    TableWriteFunction<String, String, Void> writeFn = mock(TableWriteFunction.class);
    RemoteTable<String, String, Void> table = getTable("testFlush", mock(TableReadFunction.class), writeFn, false);
    table.flush();
    verify(writeFn, times(1)).flush();
  }

  @Test
  public void testGetWithCallbackExecutor() {
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    // Sync is backed by async so needs to mock the async method
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(anyString());
    RemoteTable<String, String, Void> table = getTable("testGetWithCallbackExecutor", readFn, null,
        Executors.newSingleThreadExecutor(), false);
    Thread testThread = Thread.currentThread();

    table.getAsync("foo").thenAccept(result -> {
      Assert.assertEquals("bar", result);
      // Must be executed on the executor thread
      Assert.assertNotSame(testThread, Thread.currentThread());
    });
  }

  @Test
  public void testGetDelegation() {
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any());
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any(), any());
    Map<String, String> result = new HashMap<>();
    result.put("foo", "bar");
    doReturn(CompletableFuture.completedFuture(result)).when(readFn).getAllAsync(any());
    doReturn(CompletableFuture.completedFuture(result)).when(readFn).getAllAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(5)).when(readFn).readAsync(anyInt(), any());

    RemoteTable<String, String, Void> table = getTable("testGetDelegation", readFn, null,
        Executors.newSingleThreadExecutor(), true);
    verify(readFn, times(1)).init(any(), any());

    // GetAsync
    verify(readFn, times(0)).getAsync(any());
    verify(readFn, times(0)).getAsync(any(), any());
    assertEquals("bar", table.getAsync("foo").join());
    verify(readFn, times(1)).getAsync(any());
    verify(readFn, times(0)).getAsync(any(), any());
    assertEquals("bar", table.getAsync("foo", 1).join());
    verify(readFn, times(1)).getAsync(any());
    verify(readFn, times(1)).getAsync(any(), any());
    // GetAllAsync
    verify(readFn, times(0)).getAllAsync(any());
    verify(readFn, times(0)).getAllAsync(any(), any());
    assertEquals(result, table.getAllAsync(Arrays.asList("foo")).join());
    verify(readFn, times(1)).getAllAsync(any());
    verify(readFn, times(0)).getAllAsync(any(), any());
    assertEquals(result, table.getAllAsync(Arrays.asList("foo"), Arrays.asList(1)).join());
    verify(readFn, times(1)).getAllAsync(any());
    verify(readFn, times(1)).getAllAsync(any(), any());
    // ReadAsync
    verify(readFn, times(0)).readAsync(anyInt(), any());
    assertEquals(5, table.readAsync(1, 2).join());
    verify(readFn, times(1)).readAsync(anyInt(), any());

    table.close();
  }

  @Test
  public void testPutUpdateAndDeleteDelegation() {
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(anyCollection());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(anyCollection(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAllAsync(anyCollection());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(anyCollection());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(anyCollection(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).writeAsync(anyInt(), any());

    RemoteTable<String, String, String> table = getTable("testGetDelegation", readFn, writeFn,
        Executors.newSingleThreadExecutor(), true);
    verify(readFn, times(1)).init(any(), any());

    // PutAsync
    verify(writeFn, times(0)).putAsync(any(), any());
    verify(writeFn, times(0)).putAsync(any(), any(), any());
    table.putAsync("roo", "bar").join();
    verify(writeFn, times(1)).putAsync(any(), any());
    verify(writeFn, times(0)).putAsync(any(), any(), any());
    table.putAsync("foo", "bar", 3).join();
    verify(writeFn, times(1)).putAsync(any(), any());
    verify(writeFn, times(1)).putAsync(any(), any(), any());
    // PutAllAsync
    verify(writeFn, times(0)).putAllAsync(anyCollection());
    verify(writeFn, times(0)).putAllAsync(anyCollection(), any());
    table.putAllAsync(Arrays.asList(new Entry("foo", "bar"))).join();
    verify(writeFn, times(1)).putAllAsync(anyCollection());
    verify(writeFn, times(0)).putAllAsync(anyCollection(), any());
    table.putAllAsync(Arrays.asList(new Entry("foo", "bar")), 2).join();
    verify(writeFn, times(1)).putAllAsync(anyCollection());
    verify(writeFn, times(1)).putAllAsync(anyCollection(), any());

    // UpdateAsync
    verify(writeFn, times(0)).updateAsync(any(), any());
    table.updateAsync("foo", "bar").join();
    verify(writeFn, times(1)).updateAsync(any(), any());
    // UpdateAllAsync
    verify(writeFn, times(0)).updateAllAsync(anyCollection());
    table.updateAllAsync(Arrays.asList(new Entry<>("foo", "bar"))).join();
    verify(writeFn, times(1)).updateAllAsync(anyCollection());

    // DeleteAsync
    verify(writeFn, times(0)).deleteAsync(any());
    verify(writeFn, times(0)).deleteAsync(any(), any());
    table.deleteAsync("foo").join();
    verify(writeFn, times(1)).deleteAsync(any());
    verify(writeFn, times(0)).deleteAsync(any(), any());
    table.deleteAsync("foo", 2).join();
    verify(writeFn, times(1)).deleteAsync(any());
    verify(writeFn, times(1)).deleteAsync(any(), any());
    // DeleteAllAsync
    verify(writeFn, times(0)).deleteAllAsync(anyCollection());
    verify(writeFn, times(0)).deleteAllAsync(anyCollection(), any());
    table.deleteAllAsync(Arrays.asList("foo")).join();
    verify(writeFn, times(1)).deleteAllAsync(anyCollection());
    verify(writeFn, times(0)).deleteAllAsync(anyCollection(), any());
    table.deleteAllAsync(Arrays.asList("foo"), Arrays.asList(2)).join();
    verify(writeFn, times(1)).deleteAllAsync(anyCollection());
    verify(writeFn, times(1)).deleteAllAsync(anyCollection(), any());
    // WriteAsync
    verify(writeFn, times(0)).writeAsync(anyInt(), any());
    table.writeAsync(1, 2).join();
    verify(writeFn, times(1)).writeAsync(anyInt(), any());
  }
}
