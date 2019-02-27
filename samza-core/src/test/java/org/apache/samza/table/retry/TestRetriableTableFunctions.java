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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import junit.framework.Assert;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.Table;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.TestRemoteTable;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestRetriableTableFunctions {

  private final ScheduledExecutorService schedExec = Executors.newSingleThreadScheduledExecutor();

  public TableMetricsUtil getMetricsUtil(String tableId) {
    Table table = mock(Table.class);
    Context context = TestRemoteTable.getMockContext();
    return new TableMetricsUtil(context, table, tableId);
  }

  @Test
  public void testFirstTimeSuccessGet() throws Exception {
    String tableId = "testFirstTimeSuccessGet";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(anyString());
    RetriableReadFunction<String, String> retryIO = new RetriableReadFunction<>(policy, readFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    Assert.assertEquals("bar", retryIO.getAsync("foo").get());
    verify(readFn, times(1)).getAsync(anyString());

    Assert.assertEquals(0, retryIO.retryMetrics.retryCount.getCount());
    Assert.assertEquals(1, retryIO.retryMetrics.successCount.getCount());
    Assert.assertEquals(0, retryIO.retryMetrics.retryTimer.getSnapshot().getMax());
  }

  @Test
  public void testRetryEngagedGet() throws Exception {
    String tableId = "testRetryEngagedGet";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());

    int[] times = {0};
    Map<String, String> map = new HashMap<>();
    map.put("foo1", "bar1");
    map.put("foo2", "bar2");
    doAnswer(invocation -> {
        CompletableFuture<Map<String, String>> future = new CompletableFuture();
        if (times[0] > 0) {
          future.complete(map);
        } else {
          times[0]++;
          future.completeExceptionally(new RuntimeException("test exception"));
        }
        return future;
      }).when(readFn).getAllAsync(any());

    RetriableReadFunction<String, String> retryIO = new RetriableReadFunction<>(policy, readFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    Assert.assertEquals(map, retryIO.getAllAsync(Arrays.asList("foo1", "foo2")).get());
    verify(readFn, times(2)).getAllAsync(any());

    Assert.assertEquals(1, retryIO.retryMetrics.retryCount.getCount());
    Assert.assertEquals(0, retryIO.retryMetrics.successCount.getCount());
    Assert.assertTrue(retryIO.retryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testRetryExhaustedTimeGet() throws Exception {
    String tableId = "testRetryExhaustedTime";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());

    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAsync(anyString());

    RetriableReadFunction<String, String> retryIO = new RetriableReadFunction<>(policy, readFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    try {
      retryIO.getAsync("foo").get();
      Assert.fail();
    } catch (ExecutionException e) {
    }

    // Conservatively: must be at least 3 attempts with 5ms backoff and 100ms maxDelay
    verify(readFn, atLeast(3)).getAsync(anyString());
    Assert.assertTrue(retryIO.retryMetrics.retryCount.getCount() >= 3);
    Assert.assertEquals(0, retryIO.retryMetrics.successCount.getCount());
    Assert.assertTrue(retryIO.retryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testRetryExhaustedAttemptsGet() throws Exception {
    String tableId = "testRetryExhaustedAttempts";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterAttempts(10);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(true).when(readFn).isRetriable(any());

    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(readFn).getAllAsync(any());

    RetriableReadFunction<String, String> retryIO = new RetriableReadFunction<>(policy, readFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    try {
      retryIO.getAllAsync(Arrays.asList("foo1", "foo2")).get();
      Assert.fail();
    } catch (ExecutionException e) {
    }

    // 1 initial try + 10 retries
    verify(readFn, times(11)).getAllAsync(any());
    Assert.assertEquals(10, retryIO.retryMetrics.retryCount.getCount());
    Assert.assertEquals(0, retryIO.retryMetrics.successCount.getCount());
    Assert.assertTrue(retryIO.retryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testFirstTimeSuccessPut() throws Exception {
    String tableId = "testFirstTimeSuccessPut";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());
    doReturn(CompletableFuture.completedFuture("bar")).when(writeFn).putAsync(anyString(), anyString());
    RetriableWriteFunction<String, String> retryIO = new RetriableWriteFunction<>(policy, writeFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    retryIO.putAsync("foo", "bar").get();
    verify(writeFn, times(1)).putAsync(anyString(), anyString());

    Assert.assertEquals(0, retryIO.retryMetrics.retryCount.getCount());
    Assert.assertEquals(1, retryIO.retryMetrics.successCount.getCount());
    Assert.assertEquals(0, retryIO.retryMetrics.retryTimer.getSnapshot().getMax());
  }

  @Test
  public void testRetryEngagedPut() throws Exception {
    String tableId = "testRetryEngagedPut";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(10));
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    doReturn(true).when(writeFn).isRetriable(any());

    int[] times = new int[] {0};
    List<Entry<String, String>> records = new ArrayList<>();
    records.add(new Entry<>("foo1", "bar1"));
    records.add(new Entry<>("foo2", "bar2"));
    doAnswer(invocation -> {
        CompletableFuture<Map<String, String>> future = new CompletableFuture();
        if (times[0] > 0) {
          future.complete(null);
        } else {
          times[0]++;
          future.completeExceptionally(new RuntimeException("test exception"));
        }
        return future;
      }).when(writeFn).putAllAsync(any());

    RetriableWriteFunction<String, String> retryIO = new RetriableWriteFunction<>(policy, writeFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    retryIO.putAllAsync(records).get();
    verify(writeFn, times(2)).putAllAsync(any());

    Assert.assertEquals(1, retryIO.retryMetrics.retryCount.getCount());
    Assert.assertEquals(0, retryIO.retryMetrics.successCount.getCount());
    Assert.assertTrue(retryIO.retryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testRetryExhaustedTimePut() throws Exception {
    String tableId = "testRetryExhaustedTimePut";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterDelay(Duration.ofMillis(100));
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());

    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).deleteAsync(anyString());

    RetriableWriteFunction<String, String> retryIO = new RetriableWriteFunction<>(policy, writeFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    try {
      retryIO.deleteAsync("foo").get();
      Assert.fail();
    } catch (ExecutionException e) {
    }

    // Conservatively: must be at least 3 attempts with 5ms backoff and 100ms maxDelay
    verify(writeFn, atLeast(3)).deleteAsync(anyString());
    Assert.assertTrue(retryIO.retryMetrics.retryCount.getCount() >= 3);
    Assert.assertEquals(0, retryIO.retryMetrics.successCount.getCount());
    Assert.assertTrue(retryIO.retryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testRetryExhaustedAttemptsPut() throws Exception {
    String tableId = "testRetryExhaustedAttemptsPut";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(5));
    policy.withStopAfterAttempts(10);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(true).when(writeFn).isRetriable(any());

    CompletableFuture<String> future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException("test exception"));
    doReturn(future).when(writeFn).deleteAllAsync(any());

    RetriableWriteFunction<String, String> retryIO = new RetriableWriteFunction<>(policy, writeFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));
    try {
      retryIO.deleteAllAsync(Arrays.asList("foo1", "foo2")).get();
      Assert.fail();
    } catch (ExecutionException e) {
    }

    // 1 initial try + 10 retries
    verify(writeFn, times(11)).deleteAllAsync(any());
    Assert.assertEquals(10, retryIO.retryMetrics.retryCount.getCount());
    Assert.assertEquals(0, retryIO.retryMetrics.successCount.getCount());
    Assert.assertTrue(retryIO.retryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }

  @Test
  public void testMixedIsRetriablePredicates() throws Exception {
    String tableId = "testMixedIsRetriablePredicates";
    TableRetryPolicy policy = new TableRetryPolicy();
    policy.withFixedBackoff(Duration.ofMillis(100));

    // Retry should be attempted based on the custom classification, ie. retry on NPE
    policy.withRetryPredicate(ex -> ex instanceof NullPointerException);

    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);

    // Table fn classification only retries on IllegalArgumentException
    doAnswer(arg -> arg.getArgumentAt(0, Throwable.class) instanceof IllegalArgumentException).when(readFn).isRetriable(any());

    int[] times = new int[1];
    doAnswer(arg -> {
        if (times[0]++ == 0) {
          CompletableFuture<String> future = new CompletableFuture();
          future.completeExceptionally(new NullPointerException("test exception"));
          return future;
        } else {
          return CompletableFuture.completedFuture("bar");
        }
      }).when(readFn).getAsync(any());

    RetriableReadFunction<String, String> retryIO = new RetriableReadFunction<>(policy, readFn, schedExec);
    retryIO.setMetrics(getMetricsUtil(tableId));

    Assert.assertEquals("bar", retryIO.getAsync("foo").get());

    verify(readFn, times(2)).getAsync(anyString());
    Assert.assertEquals(1, retryIO.retryMetrics.retryCount.getCount());
    Assert.assertEquals(0, retryIO.retryMetrics.successCount.getCount());
    Assert.assertTrue(retryIO.retryMetrics.retryTimer.getSnapshot().getMax() > 0);
  }
}
