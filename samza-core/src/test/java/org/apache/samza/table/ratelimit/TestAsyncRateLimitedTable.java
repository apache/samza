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
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.remote.AsyncRemoteTable;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.TestRemoteTable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class TestAsyncRateLimitedTable {

  private final ScheduledExecutorService schedExec = Executors.newSingleThreadScheduledExecutor();

  private Map<String, String> readMap = new HashMap<>();
  private AsyncReadWriteUpdateTable readTable;
  private TableRateLimiter readRateLimiter;
  private TableReadFunction<String, String> readFn;
  private AsyncReadWriteUpdateTable<String, String, String> writeTable;
  private TableRateLimiter<String, String> writeRateLimiter;
  private TableWriteFunction<String, String, Void> writeFn;

  @Before
  public void prepare() {
    // Read part
    readRateLimiter = mock(TableRateLimiter.class);
    readFn = mock(TableReadFunction.class);
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any());
    doReturn(CompletableFuture.completedFuture("bar")).when(readFn).getAsync(any(), any());
    readMap.put("foo", "bar");
    doReturn(CompletableFuture.completedFuture(readMap)).when(readFn).getAllAsync(any());
    doReturn(CompletableFuture.completedFuture(readMap)).when(readFn).getAllAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(5)).when(readFn).readAsync(anyInt(), any());
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, null);
    readTable = new AsyncRateLimitedTable("t1", delegate, readRateLimiter, null, null, schedExec);
    readTable.init(TestRemoteTable.getMockContext());

    // Write part
    writeRateLimiter = mock(TableRateLimiter.class);
    writeFn = mock(TableWriteFunction.class);
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAsync(any(), any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).putAllAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).deleteAllAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(5)).when(writeFn).writeAsync(anyInt(), any());

    // update part
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAsync(any(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(writeFn).updateAllAsync(any());

    delegate = new AsyncRemoteTable(readFn, writeFn);
    writeTable = new AsyncRateLimitedTable("t1", delegate, readRateLimiter, writeRateLimiter, writeRateLimiter, schedExec);
    writeTable.init(TestRemoteTable.getMockContext());
  }

  @Test(expected = NullPointerException.class)
  public void testNotNullTableId() {
    new AsyncRateLimitedTable(null, mock(AsyncReadWriteUpdateTable.class),
        mock(TableRateLimiter.class), mock(TableRateLimiter.class), mock(TableRateLimiter.class),
        mock(ScheduledExecutorService.class));
  }

  @Test(expected = NullPointerException.class)
  public void testNotNullTable() {
    new AsyncRateLimitedTable("t1", null,
        mock(TableRateLimiter.class), mock(TableRateLimiter.class), mock(TableRateLimiter.class),
        mock(ScheduledExecutorService.class));
  }

  @Test(expected = NullPointerException.class)
  public void testNotNullRateLimitingExecutor() {
    new AsyncRateLimitedTable("t1", mock(AsyncReadWriteUpdateTable.class),
        mock(TableRateLimiter.class), mock(TableRateLimiter.class), mock(TableRateLimiter.class),
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotNullAtLeastOneRateLimiter() {
    new AsyncRateLimitedTable("t1", mock(AsyncReadWriteUpdateTable.class),
        null, null, null,
        mock(ScheduledExecutorService.class));
  }

  @Test
  public void testGetAsync() {
    Assert.assertEquals("bar", readTable.getAsync("foo").join());
    verify(readFn, times(1)).getAsync(any());
    verify(readFn, times(0)).getAsync(any(), any());
    verify(readRateLimiter, times(1)).throttle(anyString());
    verify(readRateLimiter, times(0)).throttle(anyCollection());
    verify(readRateLimiter, times(0)).throttle(any(), any());
    verify(readRateLimiter, times(0)).throttle(anyInt(), any());
    verify(readRateLimiter, times(0)).throttleRecords(anyCollection());
    verifyWritePartNotCalled();
  }

  @Test
  public void testGetAsyncWithArgs() {
    Assert.assertEquals("bar", readTable.getAsync("foo", 1).join());
    verify(readFn, times(0)).getAsync(any());
    verify(readFn, times(1)).getAsync(any(), any());
    verify(readRateLimiter, times(1)).throttle(anyString(), any());
    verify(readRateLimiter, times(0)).throttle(anyCollection());
    verify(readRateLimiter, times(0)).throttle(any(), any());
    verify(readRateLimiter, times(0)).throttle(anyInt(), any());
    verify(readRateLimiter, times(0)).throttleRecords(anyCollection());
    verifyWritePartNotCalled();
  }

  @Test
  public void testGetAllAsync() {
    Assert.assertEquals(readMap, readTable.getAllAsync(Arrays.asList("")).join());
    verify(readFn, times(1)).getAllAsync(any());
    verify(readFn, times(0)).getAllAsync(any(), any());
    verify(readRateLimiter, times(0)).throttle(anyString());
    verify(readRateLimiter, times(1)).throttle(anyCollection());
    verify(readRateLimiter, times(0)).throttle(any(), any());
    verify(readRateLimiter, times(0)).throttle(anyInt(), any());
    verify(readRateLimiter, times(0)).throttleRecords(anyCollection());
    verifyWritePartNotCalled();
  }

  @Test
  public void testGetAllAsyncWithArgs() {
    Assert.assertEquals(readMap, readTable.getAllAsync(Arrays.asList(""), "").join());
    verify(readFn, times(0)).getAllAsync(any());
    verify(readFn, times(1)).getAllAsync(any(), any());
    verify(readRateLimiter, times(0)).throttle(anyString());
    verify(readRateLimiter, times(1)).throttle(anyCollection(), any());
    verify(readRateLimiter, times(0)).throttle(anyString(), any());
    verify(readRateLimiter, times(0)).throttle(anyInt(), any());
    verify(readRateLimiter, times(0)).throttleRecords(anyCollection());
    verifyWritePartNotCalled();
  }

  @Test
  public void testReadAsync() {
    Assert.assertEquals(5, readTable.readAsync(1, 2).join());
    verify(readFn, times(1)).readAsync(anyInt(), any());
    verify(readRateLimiter, times(0)).throttle(anyString());
    verify(readRateLimiter, times(0)).throttle(anyCollection());
    verify(readRateLimiter, times(0)).throttle(any(), any());
    verify(readRateLimiter, times(1)).throttle(anyInt(), any());
    verify(readRateLimiter, times(0)).throttleRecords(anyCollection());
    verifyWritePartNotCalled();
  }

  @Test
  public void testPutAsync() {
    writeTable.putAsync("foo", "bar").join();
    verify(writeFn, times(1)).putAsync(any(), any());
    verify(writeFn, times(0)).putAsync(any(), any(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(1)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testPutAsyncWithArgs() {
    writeTable.putAsync("foo", "bar", 1).join();
    verify(writeFn, times(0)).putAsync(any(), any());
    verify(writeFn, times(1)).putAsync(any(), any(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(1)).throttle(anyString(), anyString(), any());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testPutAllAsync() {
    writeTable.putAllAsync(Arrays.asList(new Entry("1", "2"))).join();
    verify(writeFn, times(1)).putAllAsync(anyCollection());
    verify(writeFn, times(0)).putAllAsync(anyCollection(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(1)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testPutAllAsyncWithArgs() {
    writeTable.putAllAsync(Arrays.asList(new Entry("1", "2")), Arrays.asList(1)).join();
    verify(writeFn, times(0)).putAllAsync(anyCollection());
    verify(writeFn, times(1)).putAllAsync(anyCollection(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(1)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testUpdateAsync() {
    writeTable.updateAsync("foo", "bar").join();
    verify(writeFn, times(1)).updateAsync(any(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(1)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testUpdateAllAsync() {
    writeTable.updateAllAsync(Arrays.asList(new Entry("1", "2"))).join();
    verify(writeFn, times(1)).updateAllAsync(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(1)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testDeleteAsync() {
    writeTable.deleteAsync("foo").join();
    verify(writeFn, times(1)).deleteAsync(any());
    verify(writeFn, times(0)).deleteAsync(any(), any());
    verify(writeRateLimiter, times(1)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testDeleteAsyncWithArgs() {
    writeTable.deleteAsync("foo", 1).join();
    verify(writeFn, times(0)).deleteAsync(any());
    verify(writeFn, times(1)).deleteAsync(any(), any());
    verify(writeRateLimiter, times(1)).throttle(anyString(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testDeleteAllAsync() {
    writeTable.deleteAllAsync(Arrays.asList("1", "2")).join();
    verify(writeFn, times(1)).deleteAllAsync(anyCollection());
    verify(writeFn, times(0)).deleteAllAsync(anyCollection(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(1)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testDeleteAllAsyncWithArgs() {
    writeTable.deleteAllAsync(Arrays.asList("1", "2"), 1).join();
    verify(writeFn, times(0)).deleteAllAsync(anyCollection());
    verify(writeFn, times(1)).deleteAllAsync(anyCollection(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(1)).throttle(anyCollection(), any());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
    verifyReadPartNotCalled();
  }

  @Test
  public void testWriteAsync() {
    Assert.assertEquals(5, writeTable.writeAsync(1, 2).join());
    verify(writeFn, times(1)).writeAsync(anyInt(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(1)).throttle(anyInt(), any());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verifyReadPartNotCalled();
  }

  @Test
  public void testFlushAndClose() {
    TableRateLimiter readRateLimiter = mock(TableRateLimiter.class);
    TableRateLimiter writeRateLimiter = mock(TableRateLimiter.class);
    TableReadFunction<String, String> readFn = mock(TableReadFunction.class);
    TableWriteFunction<String, String, String> writeFn = mock(TableWriteFunction.class);
    AsyncReadWriteUpdateTable delegate = new AsyncRemoteTable(readFn, writeFn);
    AsyncRateLimitedTable table = new AsyncRateLimitedTable("t1", delegate,
        readRateLimiter, writeRateLimiter, writeRateLimiter, schedExec);
    table.init(TestRemoteTable.getMockContext());

    table.flush();
    verify(writeFn, times(1)).flush();

    table.close();
    verify(readFn, times(1)).close();
    verify(writeFn, times(1)).close();
  }

  private void verifyReadPartNotCalled() {
    verify(readFn, times(0)).getAsync(any());
    verify(readFn, times(0)).getAsync(any(), any());
    verify(readFn, times(0)).getAllAsync(any(), any());
    verify(readFn, times(0)).getAllAsync(any(), any(), any());
    verify(readFn, times(0)).readAsync(anyInt(), any());
    verify(readRateLimiter, times(0)).throttle(anyString());
    verify(readRateLimiter, times(0)).throttle(anyCollection());
    verify(readRateLimiter, times(0)).throttle(any(), any());
    verify(readRateLimiter, times(0)).throttle(anyInt(), any());
    verify(readRateLimiter, times(0)).throttleRecords(anyCollection());
  }

  private void verifyWritePartNotCalled() {
    verify(writeFn, times(0)).putAsync(any(), any());
    verify(writeFn, times(0)).putAsync(any(), any(), any());
    verify(writeFn, times(0)).putAllAsync(any());
    verify(writeFn, times(0)).putAllAsync(any(), any());
    verify(writeFn, times(0)).deleteAsync(any());
    verify(writeFn, times(0)).deleteAsync(any(), any());
    verify(writeFn, times(0)).deleteAllAsync(any());
    verify(writeFn, times(0)).deleteAllAsync(any(), any());
    verify(writeFn, times(0)).writeAsync(anyInt(), any());
    verify(writeRateLimiter, times(0)).throttle(anyString());
    verify(writeRateLimiter, times(0)).throttle(anyString(), anyString());
    verify(writeRateLimiter, times(0)).throttle(anyCollection());
    verify(writeRateLimiter, times(0)).throttleRecords(anyCollection());
    verify(writeRateLimiter, times(0)).throttle(anyInt(), any());
  }

}
