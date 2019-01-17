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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteTable;
import org.apache.samza.table.remote.AsyncRemoteTable;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.TestRemoteTable;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestAsyncRetriableTable {

  private final ScheduledExecutorService schedExec = Executors.newSingleThreadScheduledExecutor();

  @Test(expected = NullPointerException.class)
  public void testNotNullTableId() {
    new AsyncRetriableTable(null, mock(AsyncReadWriteTable.class),
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
    new AsyncRetriableTable("t1", mock(AsyncReadWriteTable.class),
        mock(TableRetryPolicy.class), mock(TableRetryPolicy.class), null,
        mock(TableReadFunction.class), mock(TableWriteFunction.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotAllRetryPolicyAreNull() {
    new AsyncRetriableTable("t1", mock(AsyncReadWriteTable.class),
        null, null,
        mock(ScheduledExecutorService.class),
        mock(TableReadFunction.class), mock(TableWriteFunction.class));
  }

  @Test
  public void testGetWithoutRetry() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any());
    Map<String, String> result = new HashMap<>();
    result.put("foo", "bar");
    doReturn(CompletableFuture.completedFuture(result)).when(readFn).getAllAsync(any());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);

    int times = 0;
    table.init(TestRemoteTable.getMockContext());
    verify(readFn, times(1)).init(any());
    assertEquals("bar", table.getAsync("foo").get());
    verify(readFn, times(1)).getAsync(any());
    assertEquals(++times, table.readRetryMetrics.successCount.getCount());
    assertEquals(result, table.getAllAsync(Arrays.asList("foo")).get());
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
  public void testGetWithRetryDisabled() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(false).when(readFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAsync(anyString());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.getAsync("foo").get();
      fail();
    } catch (ExecutionException e) {
    }

    verify(readFn, times(1)).getAsync(any());
    assertEquals(0, table.readRetryMetrics.retryCount.getCount());
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(0, table.readRetryMetrics.permFailureCount.getCount());
    assertEquals(0, table.readRetryMetrics.retryTimer.getSnapshot().getMax());
  }

  @Test
  public void testGetAllWithOneRetry() throws Exception {
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
      }).when(readFn).getAllAsync(any());

    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    assertEquals(map, table.getAllAsync(Arrays.asList("foo1", "foo2")).get());
    verify(readFn, times(2)).getAllAsync(any());
    assertEquals(1, table.readRetryMetrics.retryCount.getCount());
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(0, table.readRetryMetrics.permFailureCount.getCount());
    assertTrue(table.readRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testGetWithPermFailureOnTimeout() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAsync(anyString());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.getAsync("foo").get();
      fail();
    } catch (ExecutionException e) {
    }

    verify(readFn, atLeast(3)).getAsync(any());
    assertTrue(table.readRetryMetrics.retryCount.getCount() >= 3);
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(1, table.readRetryMetrics.permFailureCount.getCount());
    assertTrue(table.readRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testGetWithPermFailureOnMaxCount() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterAttempts(10);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAllAsync(any());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, policy, null, schedExec, readFn, null);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.getAsync("foo").get();
      fail();
    } catch (ExecutionException e) {
    }

    verify(readFn, atLeast(11)).getAsync(any());
    assertEquals(10, table.readRetryMetrics.retryCount.getCount());
    assertEquals(0, table.readRetryMetrics.successCount.getCount());
    assertEquals(1, table.readRetryMetrics.permFailureCount.getCount());
    assertTrue(table.readRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testPutWithoutRetry() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction readFn = mock(TableReadFunction.class);
    TableWriteFunction writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);

    int times = 0;
    table.init(TestRemoteTable.getMockContext());
    verify(readFn, times(1)).init(any());
    verify(writeFn, times(1)).init(any());
    table.putAsync("foo", "bar").get();
    verify(writeFn, times(1)).putAsync(any(), any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    table.putAllAsync(Arrays.asList(new Entry("1", "2"))).get();
    verify(writeFn, times(1)).putAllAsync(any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    table.deleteAsync("1").get();
    verify(writeFn, times(1)).deleteAsync(any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    table.deleteAllAsync(Arrays.asList("1", "2")).get();
    verify(writeFn, times(1)).deleteAllAsync(any());
    assertEquals(++times, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryTimer.getSnapshot().getMax());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertNull(table.readRetryMetrics);
  }

  @Test
  public void testPutWithRetryDisabled() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(false).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).putAsync(any(), any());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.putAsync("foo", "bar").get();
      fail();
    } catch (ExecutionException e) {
    }

    verify(writeFn, times(1)).putAsync(any(), any());
    assertEquals(0, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertEquals(0, table.writeRetryMetrics.retryTimer.getSnapshot().getMax());
  }

  @Test
  public void testPutAllWithOneRetry() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
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

    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    table.putAllAsync(Arrays.asList(new Entry(1, 2))).get();
    verify(writeFn, times(2)).putAllAsync(any());
    assertEquals(1, table.writeRetryMetrics.retryCount.getCount());
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(0, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testPutWithPermFailureOnTimeout() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAsync(anyString());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.putAsync("foo", "bar").get();
      fail();
    } catch (ExecutionException e) {
    }

    verify(writeFn, atLeast(3)).putAsync(any(), any());
    assertTrue(table.writeRetryMetrics.retryCount.getCount() >= 3);
    assertEquals(0, table.writeRetryMetrics.successCount.getCount());
    assertEquals(1, table.writeRetryMetrics.permFailureCount.getCount());
    assertTrue(table.writeRetryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testPutWithPermFailureOnMaxCount() throws Exception {
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterAttempts(10);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).putAllAsync(any());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);
    table.init(TestRemoteTable.getMockContext());

    try {
      table.putAllAsync(Arrays.asList(new Entry(1, 2))).get();
      fail();
    } catch (ExecutionException e) {
    }

    verify(writeFn, atLeast(11)).putAllAsync(any());
    assertEquals(10, table.writeRetryMetrics.retryCount.getCount());
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
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRetriableTable table = new AsyncRetriableTable("t1", delegate, null, policy, schedExec, readFn, writeFn);

    table.flush();
    verify(writeFn, times(1)).flush();

    table.close();
    verify(readFn, times(1)).close();
    verify(writeFn, times(1)).close();
  }
}
