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
package org.apache.samza.table.retry;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.remote.AsyncRemoteTable;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.TestRemoteTable;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.*;


public class TestAsyncRetriableTable {

  private final ScheduledExecutorService schedExec = Executors.newSingleThreadScheduledExecutor();

  @Test(expected = NullPointerException.class)
  public void testNotNullTableId() {
    new AsyncRetriableTable(null, mock(AsyncReadWriteUpdateTable.class),
        mock(TableRetryPolicy.class), mock(TableRetryPolicy.class),
        mock(ScheduledExecutorService.class),
        mock(TableReadFunction.class), mock(TableWriteFunction.class));
  }

  @Test(expected = NullPointerException.class)
  public void testNotNullTable() {
    new AsyncRetriableTable("t1", null,
        mock(TableRetryPolicy.class), mock(TableRetryPolicy.class),
        mock(ScheduledExecutorService.class),
        mock(TableReadFunction.class), mock(TableWriteFunction.class));
  }

  @Test(expected = NullPointerException.class)
  public void testNotNullExecutorService() {
    new AsyncRetriableTable("t1", mock(AsyncReadWriteUpdateTable.class),
        mock(TableRetryPolicy.class), mock(TableRetryPolicy.class), null,
        mock(TableReadFunction.class), mock(TableWriteFunction.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotAllRetryPolicyAreNull() {
    new AsyncRetriableTable("t1", mock(AsyncReadWriteUpdateTable.class),
        null, null,
        mock(ScheduledExecutorService.class),
        mock(TableReadFunction.class), mock(TableWriteFunction.class));
  }

  @Test
  public void testGetDelegation() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any());
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any(), any());
    Map<String, String> result = new HashMap<>();
    result.put("foo", "bar");
    doReturn(CompletableFuture.completedFuture(result)).when(readFn).getAllAsync(any());
    doReturn(CompletableFuture.completedFuture(result)).when(readFn).getAllAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(5)).when(readFn).readAsync(anyInt(), any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);

    table.init(TestRemoteTable.getMockContext());
    verify(readFn, times(0)).init(any(), any());

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
  public void testGetWithoutRetry() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any());
    Map<String, String> result = new HashMap<>();
    result.put("foo", "bar");
    doReturn(CompletableFuture.completedFuture(result)).when(readFn).getAllAsync(any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);

    int times = 0;
    table.init(TestRemoteTable.getMockContext());
    verify(readFn, times(0)).init(any(), any());
    assertEquals("bar", table.getAsync("foo").join());
    verify(readFn, times(1)).getAsync(any());
    assertEquals(++times, table.readRetryMetrics.successCount.getCount());
    assertEquals(result, table.getAllAsync(Arrays.asList("foo")).join());
    verify(readFn, times(1)).getAllAsync(any());
    assertEquals(++times, table.readRetryMetrics.successCount.getCount());
    assertEquals(0, table.readRetryMetrics.retryCount.getCount());
    assertEquals(0, table.readRetryMetrics.retryTimer.getSnapshot().getMax());
    assertEquals(0, table.readRetryMetrics.permFailureCount.getCount());
    assertNull(table.writeRetryMetrics);
    table.close();
    verify(readFn, times(1)).close();
  }

  @Test
  public void testGetWithRetryDisabled() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(false).when(readFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAsync(anyString());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.getAsync("foo").join();
      fail();
    } catch (Throwable t) {
    }

    verify(readFn, times(1)).getAsync(any());
    assertEquals(0, table.readRetryMetrics.retryCount.getCount());
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(0, table.readRetryMetrics.permFailureCount.getCount());
    assertEquals(0, table.readRetryMetrics.retryTimer.getSnapshot().getMax());
  }

  @Test
  public void testGetAllWithOneRetry() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());

    AtomicInteger times = new AtomicInteger();
    Map<String, String> map = new HashMap<>();
    map.put("foo1", "bar1");
    map.put("foo2", "bar2");
    doAnswer(invocation -> {
      CompletableFuture<Map<String, String>> future = new CompletableFuture();
      if (times.get() > 0) {
        future.complete(map);
      } else {
        times.incrementAndGet();
        future.completeExceptionally(new RuntimeException("test exception"));
      }
      return future;
    }).when(readFn).getAllAsync(anyCollection());

    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    assertEquals(map, table.getAllAsync(Arrays.asList("foo1", "foo2")).join());
    verify(readFn, times(2)).getAllAsync(any());
    assertEquals(1, table.readRetryMetrics.retryCount.getCount());
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(0, table.readRetryMetrics.permFailureCount.getCount());
    assertTrue(table.readRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testGetWithPermFailureOnTimeout() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAsync(anyString());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.getAsync("foo").join();
      fail();
    } catch (Throwable t) {
    }

    verify(readFn, atLeast(3)).getAsync(any());
    assertTrue(table.readRetryMetrics.retryCount.getCount() >= 3);
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(1, table.readRetryMetrics.permFailureCount.getCount());
    assertTrue(table.readRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testGetWithPermFailureOnMaxCount() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterAttempts(10);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAllAsync(any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.getAsync("foo").join();
      fail();
    } catch (Throwable t) {
    }

    verify(readFn, atLeast(11)).getAsync(any());
    assertEquals(10, table.readRetryMetrics.retryCount.getCount());
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(1, table.readRetryMetrics.permFailureCount.getCount());
    assertTrue(table.readRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testPutUpdateAndDeleteDelegation() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    TableWriteFunction writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).writeAsync(anyInt(), any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);

    // PutAsync
    verify(writeFn, times(0)).putAsync(any(), any());
    verify(writeFn, times(0)).putAsync(any(), any(), any());
    table.putAsync(1, 2).join();
    verify(writeFn, times(1)).putAsync(any(), any());
    verify(writeFn, times(0)).putAsync(any(), any(), any());
    table.putAsync(1, 2, 3).join();
    verify(writeFn, times(1)).putAsync(any(), any());
    verify(writeFn, times(1)).putAsync(any(), any(), any());
    // PutAllAsync
    verify(writeFn, times(0)).putAllAsync(anyCollection());
    verify(writeFn, times(0)).putAllAsync(anyCollection(), any());
    table.putAllAsync(Arrays.asList(1)).join();
    verify(writeFn, times(1)).putAllAsync(anyCollection());
    verify(writeFn, times(0)).putAllAsync(anyCollection(), any());
    table.putAllAsync(Arrays.asList(1), Arrays.asList(1)).join();
    verify(writeFn, times(1)).putAllAsync(anyCollection());
    verify(writeFn, times(1)).putAllAsync(anyCollection(), any());

    // UpdateAsync
    verify(writeFn, times(0)).updateAsync(any(), any());
    table.updateAsync(1, 2).join();
    verify(writeFn, times(1)).updateAsync(any(), any());

    // UpdateAllAsync
    verify(writeFn, times(0)).updateAllAsync(anyCollection());
    table.updateAllAsync(Arrays.asList(new Entry<>(1, 2))).join();
    verify(writeFn, times(1)).updateAllAsync(anyCollection());

    // DeleteAsync
    verify(writeFn, times(0)).deleteAsync(any());
    verify(writeFn, times(0)).deleteAsync(any(), any());
    table.deleteAsync(1).join();
    verify(writeFn, times(1)).deleteAsync(any());
    verify(writeFn, times(0)).deleteAsync(any(), any());
    table.deleteAsync(1, 2).join();
    verify(writeFn, times(1)).deleteAsync(any());
    verify(writeFn, times(1)).deleteAsync(any(), any());
    // DeleteAllAsync
    verify(writeFn, times(0)).deleteAllAsync(anyCollection());
    verify(writeFn, times(0)).deleteAllAsync(anyCollection(), any());
    table.deleteAllAsync(Arrays.asList(1)).join();
    verify(writeFn, times(1)).deleteAllAsync(anyCollection());
    verify(writeFn, times(0)).deleteAllAsync(anyCollection(), any());
    table.deleteAllAsync(Arrays.asList(1), Arrays.asList(2)).join();
    verify(writeFn, times(1)).deleteAllAsync(anyCollection());
    verify(writeFn, times(1)).deleteAllAsync(anyCollection(), any());
    // WriteAsync
    verify(writeFn, times(0)).writeAsync(anyInt(), any());
    table.writeAsync(1, 2).join();
    verify(writeFn, times(1)).writeAsync(anyInt(), any());
  }

  @Test
  public void testPutWithoutRetry() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    TableWriteFunction writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);

    int times = 0;
    table.init(TestRemoteTable.getMockContext());
    verify(readFn, times(0)).init(any(), any());
    verify(writeFn, times(0)).init(any(), any());
    table.putAsync("foo", "bar").join();
    verify(writeFn, times(1)).putAsync(any(), any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    table.putAllAsync(Arrays.asList(new Entry("1", "2"))).join();
    verify(writeFn, times(1)).putAllAsync(any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    table.deleteAsync("1").join();
    verify(writeFn, times(1)).deleteAsync(any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    table.deleteAllAsync(Arrays.asList("1", "2")).join();
    verify(writeFn, times(1)).deleteAllAsync(any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryTimer.getSnapshot().getMax());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertNull(table.readRetryMetrics);
  }

  @Test
  public void testPutWithRetryDisabled() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(false).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).putAsync(any(), any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.putAsync("foo", "bar").join();
      fail();
    } catch (Throwable t) {
    }

    verify(writeFn, times(1)).putAsync(any(), any());
    assertEquals(0, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryTimer.getSnapshot().getMax());
  }

  @Test
  public void testPutAllWithOneRetry() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());

    AtomicInteger times = new AtomicInteger();
    doAnswer(invocation -> {
      CompletableFuture<Map<String, String>> future = new CompletableFuture();
      if (times.get() > 0) {
        future.complete(null);
      } else {
        times.incrementAndGet();
        future.completeExceptionally(new RuntimeException("test exception"));
      }
      return future;
    }).when(writeFn).putAllAsync(any());

    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    table.putAllAsync(Arrays.asList(new Entry(1, 2))).join();
    verify(writeFn, times(2)).putAllAsync(any());
    assertEquals(1, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testPutWithPermFailureOnTimeout() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAsync(anyString());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.putAsync("foo", "bar").join();
      fail();
    } catch (Throwable t) {
    }

    verify(writeFn, atLeast(3)).putAsync(any(), any());
    assertTrue(table.writeRetryMetrics.retryCount.getCount() >= 3);
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(1, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testPutWithPermFailureOnMaxCount() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterAttempts(10);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).putAllAsync(any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.putAllAsync(Arrays.asList(new Entry(1, 2))).join();
      fail();
    } catch (Throwable t) {
    }

    verify(writeFn, atLeast(11)).putAllAsync(any());
    assertEquals(10, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(1, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testUpdateWithoutRetry() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    TableWriteFunction writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAllAsync(any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);

    int times = 0;
    table.init(TestRemoteTable.getMockContext());
    verify(readFn, times(0)).init(any(), any());
    verify(writeFn, times(0)).init(any(), any());
    table.updateAsync("foo", "bar").join();
    verify(writeFn, times(1)).updateAsync(any(), any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    table.updateAllAsync(Arrays.asList(new Entry<>("1", "2"))).join();
    verify(writeFn, times(1)).updateAllAsync(any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryTimer.getSnapshot().getMax());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertNull(table.readRetryMetrics);
  }

  @Test
  public void testUpdateWithRetryDisabled() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(false).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).updateAsync(any(), any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.updateAsync("foo", "bar").join();
      fail();
    } catch (Throwable t) {
    }

    verify(writeFn, times(1)).updateAsync(any(), any());
    assertEquals(0, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryTimer.getSnapshot().getMax());
  }

  @Test
  public void testUpdateWithOneRetry() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());

    AtomicInteger times = new AtomicInteger();
    doAnswer(invocation -> {
      CompletableFuture<Map<String, String>> future = new CompletableFuture();
      if (times.get() > 0) {
        future.complete(null);
      } else {
        times.incrementAndGet();
        future.completeExceptionally(new RuntimeException("test exception"));
      }
      return future;
    }).when(writeFn).updateAsync(any(), any());

    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    table.updateAsync(1, 2).join();
    verify(writeFn, times(2)).updateAsync(any(), any());
    assertEquals(1, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testUpdateAllWithOneRetry() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());

    AtomicInteger times = new AtomicInteger();
    doAnswer(invocation -> {
      CompletableFuture<Map<String, String>> future = new CompletableFuture();
      if (times.get() > 0) {
        future.complete(null);
      } else {
        times.incrementAndGet();
        future.completeExceptionally(new RuntimeException("test exception"));
      }
      return future;
    }).when(writeFn).updateAllAsync(any());

    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    table.updateAllAsync(Arrays.asList(new Entry(1, 2))).join();
    verify(writeFn, times(2)).updateAllAsync(any());
    assertEquals(1, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testUpdateWithPermFailureOnMaxCount() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterAttempts(5);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).updateAsync(any(), any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.updateAsync(1, 2).join();
      fail();
    } catch (Throwable t) {
    }

    verify(writeFn, atLeast(6)).updateAsync(any(), any());
    assertEquals(5, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(1, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testFlushAndClose() {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    TableWriteFunction writeFn = mock(TableWriteFunction.class);
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);

    table.flush();
    verify(writeFn, times(1)).flush();

    table.close();
    verify(readFn, times(1)).close();
    verify(writeFn, times(1)).close();
  }
}
