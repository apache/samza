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

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;

import org.junit.Before;
import org.junit.Test;

import org.apache.samza.table.BaseReadWriteTable.Func0;

import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestAsyncRemoteTable {

  private TableReadFunction<Integer, Integer> readFn;
  private TableWriteFunction<Integer, Integer> writeFn;
  private AsyncRemoteTable<Integer, Integer> roTable;
  private AsyncRemoteTable<Integer, Integer> rwTable;

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
  public void testGetAllAsync() {
    int times = 0;
    roTable.getAllAsync(Arrays.asList(1, 2));
    verify(readFn, times(++times)).getAllAsync(any());
    rwTable.getAllAsync(Arrays.asList(1, 2));
    verify(readFn, times(++times)).getAllAsync(any());
  }

  @Test
  public void testPutAsync() {
    verifyFailure(() -> roTable.putAsync(1, 2));
    rwTable.putAsync(1, 2);
    verify(writeFn, times(1)).putAsync(any(), any());
  }

  @Test
  public void testPutAllAsync() {
    verifyFailure(() -> roTable.putAllAsync(Arrays.asList(new Entry(1, 2))));
    rwTable.putAllAsync(Arrays.asList(new Entry(1, 2)));
    verify(writeFn, times(1)).putAllAsync(any());
  }

  @Test
  public void testDeleteAsync() {
    verifyFailure(() -> roTable.deleteAsync(1));
    rwTable.deleteAsync(1);
    verify(writeFn, times(1)).deleteAsync(any());
  }
  @Test
  public void testDeleteAllAsync() {
    verifyFailure(() -> roTable.deleteAllAsync(Arrays.asList(1)));
    rwTable.deleteAllAsync(Arrays.asList(1, 2));
    verify(writeFn, times(1)).deleteAllAsync(any());
  }

  @Test
  public void testInit() {
    roTable.init(mock(Context.class));
    verify(readFn, times(1)).init(any());
    verify(writeFn, times(0)).init(any());
    rwTable.init(mock(Context.class));
    verify(readFn, times(2)).init(any());
    verify(writeFn, times(1)).init(any());
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

  @Test(expected = NullPointerException.class)
  public void testFailOnNullReadFn() {
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
