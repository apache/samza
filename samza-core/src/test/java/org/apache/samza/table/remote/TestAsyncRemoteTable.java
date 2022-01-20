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

import java.util.List;
import org.apache.samza.storage.kv.Entry;

import org.junit.Before;
import org.junit.Test;

import org.apache.samza.table.BaseReadWriteUpdateTable.Func0;

import static org.junit.Assert.assertTrue;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestAsyncRemoteTable {

  private TableReadFunction<Integer, Integer> readFn;
  private TableWriteFunction<Integer, Integer, Integer> writeFn;
  private AsyncRemoteTable<Integer, Integer, Integer> roTable;
  private AsyncRemoteTable<Integer, Integer, Integer> rwTable;

  @Before
  public void prepare() {
    readFn = mock(TableReadFunction.class);
    writeFn = mock(TableWriteFunction.class);
    roTable = new AsyncRemoteTable<>(readFn, null);
    rwTable = new AsyncRemoteTable<>(readFn, writeFn);
  }

  @Test
  public void testGetAsync() {
    int times = 0;
    roTable.getAsync(1);
    verify(readFn, times(++times)).getAsync(any());
    rwTable.getAsync(1);
    verify(readFn, times(++times)).getAsync(any());
  }

  @Test
  public void testGetAsyncWithArgs() {
    int times = 0;
    roTable.getAsync(1, 1);
    verify(readFn, times(++times)).getAsync(any(), any());
    rwTable.getAsync(1, 1);
    verify(readFn, times(++times)).getAsync(any(), any());
  }

  @Test
  public void testGetAllAsync() {
    int times = 0;
    roTable.getAllAsync(Arrays.asList(1, 2));
    verify(readFn, times(++times)).getAllAsync(any());
    rwTable.getAllAsync(Arrays.asList(1, 2));
    verify(readFn, times(++times)).getAllAsync(any());
  }

  @Test
  public void testGetAllAsyncWithArgs() {
    int times = 0;
    roTable.getAllAsync(Arrays.asList(1, 2), Arrays.asList(0, 0));
    verify(readFn, times(++times)).getAllAsync(any(), any());
    rwTable.getAllAsync(Arrays.asList(1, 2), Arrays.asList(0, 0));
    verify(readFn, times(++times)).getAllAsync(any(), any());
  }

  @Test
  public void testReadAsync() {
    int times = 0;
    roTable.readAsync(1, 2, 3);
    verify(readFn, times(++times)).readAsync(anyInt(), any(), any());
    rwTable.readAsync(1, 2, 3);
    verify(readFn, times(++times)).readAsync(anyInt(), any(), any());
  }

  @Test
  public void testPutAsync() {
    verifyFailure(() -> roTable.putAsync(1, 2));
    rwTable.putAsync(1, 2);
    verify(writeFn, times(1)).putAsync(any(), any());
  }

  @Test
  public void testPutAsyncWithArgs() {
    verifyFailure(() -> roTable.putAsync(1, 2, 3));
    rwTable.putAsync(1, 2, 3);
    verify(writeFn, times(1)).putAsync(any(), any(), any());
  }

  @Test
  public void testPutAllAsync() {
    verifyFailure(() -> roTable.putAllAsync(Arrays.asList(new Entry(1, 2))));
    rwTable.putAllAsync(Arrays.asList(new Entry(1, 2)));
    verify(writeFn, times(1)).putAllAsync(any());
  }

  @Test
  public void testPutAllAsyncWithArgs() {
    verifyFailure(() -> roTable.putAllAsync(Arrays.asList(new Entry(1, 2)), Arrays.asList(0, 0)));
    rwTable.putAllAsync(Arrays.asList(new Entry(1, 2)), Arrays.asList(0, 0));
    verify(writeFn, times(1)).putAllAsync(any(), any());
  }

  @Test
  public void testUpdateAsync() {
    verifyFailure(() -> roTable.updateAsync(1, 2));
    rwTable.updateAsync(1, 2);
    verify(writeFn, times(1)).updateAsync(any(), any());
  }

  @Test
  public void testUpdateAllAsync() {
    List<Entry<Integer, Integer>> updates = Arrays.asList(new Entry<>(1, 100), new Entry<>(2, 200));
    verifyFailure(() -> roTable.updateAllAsync(updates));
    rwTable.updateAllAsync(updates);
    verify(writeFn, times(1)).updateAllAsync(any());
  }

  @Test
  public void testDeleteAsync() {
    verifyFailure(() -> roTable.deleteAsync(1));
    rwTable.deleteAsync(1);
    verify(writeFn, times(1)).deleteAsync(any());
  }

  @Test
  public void testDeleteAsyncWithArgs() {
    verifyFailure(() -> roTable.deleteAsync(1, 2));
    rwTable.deleteAsync(1, 2);
    verify(writeFn, times(1)).deleteAsync(any(), any());
  }

  @Test
  public void testDeleteAllAsync() {
    verifyFailure(() -> roTable.deleteAllAsync(Arrays.asList(1)));
    rwTable.deleteAllAsync(Arrays.asList(1, 2));
    verify(writeFn, times(1)).deleteAllAsync(any());
  }

  @Test
  public void testDeleteAllAsyncWithArgs() {
    verifyFailure(() -> roTable.deleteAllAsync(Arrays.asList(1), Arrays.asList(2)));
    rwTable.deleteAllAsync(Arrays.asList(1, 2), Arrays.asList(2));
    verify(writeFn, times(1)).deleteAllAsync(any(), any());
  }

  @Test
  public void testWriteAsync() {
    verifyFailure(() -> roTable.writeAsync(1, 2, 3));
    rwTable.writeAsync(1, 2, 3);
    verify(writeFn, times(1)).writeAsync(anyInt(), any(), any());
  }

  @Test
  public void testClose() {
    roTable.close();
    verify(readFn, times(1)).close();
    verify(writeFn, times(0)).close();
    rwTable.close();
    verify(readFn, times(2)).close();
    verify(writeFn, times(1)).close();
  }

  @Test
  public void testFlush() {
    roTable.flush();
    verify(writeFn, times(0)).flush();
    rwTable.flush();
    verify(writeFn, times(1)).flush();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnNullReadFnAndWriteFn() {
    new AsyncRemoteTable(null, null);
  }

  private void verifyFailure(Func0 func) {
    boolean caughtException = false;
    try {
      func.apply();
    } catch (NullPointerException ex) {
      caughtException = true;
    }
    assertTrue(caughtException);
  }

}
