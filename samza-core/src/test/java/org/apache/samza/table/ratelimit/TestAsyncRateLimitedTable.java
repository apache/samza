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
package org.apache.samza.table.ratelimit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.AsyncReadWriteTable;
import org.apache.samza.table.remote.AsyncRemoteTable;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.TestRemoteTable;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class TestAsyncRateLimitedTable {

  private final ScheduledExecutorService schedExec = Executors.newSingleThreadScheduledExecutor();

  @Test(expected = NullPointerException.class)
  public void testNotNullTableId() {
    new AsyncRateLimitedTable(null, mock(AsyncReadWriteTable.class),
        mock(TableRateLimiter.class), mock(TableRateLimiter.class),
        mock(ScheduledExecutorService.class));
  }

  @Test(expected = NullPointerException.class)
  public void testNotNullTable() {
    new AsyncRateLimitedTable("t1", null,
        mock(TableRateLimiter.class), mock(TableRateLimiter.class),
        mock(ScheduledExecutorService.class));
  }

  @Test(expected = NullPointerException.class)
  public void testNotNullRateLimitingExecutor() {
    new AsyncRateLimitedTable("t1", mock(AsyncReadWriteTable.class),
        mock(TableRateLimiter.class), mock(TableRateLimiter.class),
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotNullAtLeastOneRateLimiter() {
    new AsyncRateLimitedTable("t1", mock(AsyncReadWriteTable.class),
        null, null,
        mock(ScheduledExecutorService.class));
  }

  @Test
  public void testGetThrottling() throws Exception {
    TableRateLimiter readRateLimiter = mock(TableRateLimiter.class);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any());
    Map<String, String> result = new HashMap<>();
    result.put("foo", "bar");
    doReturn(CompletableFuture.completedFuture(result)).when(readFn).getAllAsync(any());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, null);
    AsyncRateLimitedTable table = new AsyncRateLimitedTable("t1", delegate,
        readRateLimiter, null, schedExec);
    table.init(TestRemoteTable.getMockContext());

    Assert.assertEquals("bar", table.getAsync("foo").get());
    verify(readFn, times(1)).getAsync(any());
    verify(readRateLimiter, times(1)).throttle(anyString());
    verify(readRateLimiter, times(0)).throttle(anyList());

    Assert.assertEquals(result, table.getAllAsync(Arrays.asList("")).get());
    verify(readFn, times(1)).getAllAsync(any());
    verify(readRateLimiter, times(1)).throttle(anyList());
    verify(readRateLimiter, times(1)).throttle(anyString());
  }

  @Test
  public void testPutThrottling() throws Exception {
    TableRateLimiter readRateLimiter = mock(TableRateLimiter.class);
    TableRateLimiter writeRateLimiter = mock(TableRateLimiter.class);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any());
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRateLimitedTable table = new AsyncRateLimitedTable("t1", delegate,
        readRateLimiter, writeRateLimiter, schedExec);
    table.init(TestRemoteTable.getMockContext());

    table.putAsync("foo", "bar").get();
    verify(writeFn, times(1)).putAsync(any(), any());
    verify(writeRateLimiter, times(1)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttleRecords(anyList());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyList());

    table.putAllAsync(Arrays.asList(new Entry("1", "2"))).get();
    verify(writeFn, times(1)).putAllAsync(any());
    verify(writeRateLimiter, times(1)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(1)).throttleRecords(anyList());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyList());

    table.deleteAsync("foo").get();
    verify(writeFn, times(1)).deleteAsync(anyString());
    verify(writeRateLimiter, times(1)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(1)).throttleRecords(anyList());
    verify(writeRateLimiter, times(1)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyList());

    table.deleteAllAsync(Arrays.asList("1", "2")).get();
    verify(writeFn, times(1)).deleteAllAsync(any());
    verify(writeRateLimiter, times(1)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(1)).throttleRecords(anyList());
    verify(writeRateLimiter, times(1)).throttle(anyString());
    verify(writeRateLimiter, times(1)).throttle(anyList());
  }

  @Test
  public void testFlushAndClose() {
    TableRateLimiter readRateLimiter = mock(TableRateLimiter.class);
    TableRateLimiter writeRateLimiter = mock(TableRateLimiter.class);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writeFn = mock(TableWriteFunction.class);
    AsyncReadWriteTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRateLimitedTable table = new AsyncRateLimitedTable("t1", delegate,
        readRateLimiter, writeRateLimiter, schedExec);
    table.init(TestRemoteTable.getMockContext());

    table.flush();
    verify(writeFn, times(1)).flush();

    table.close();
    verify(readFn, times(1)).close();
    verify(writeFn, times(1)).close();
  }

}
