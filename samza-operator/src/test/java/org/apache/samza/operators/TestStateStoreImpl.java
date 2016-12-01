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
package org.apache.samza.operators;

import org.apache.samza.operators.windows.StoreFunctions;
import org.apache.samza.operators.windows.WindowState;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestStateStoreImpl {
  @Test
  public void testStateStoreImpl() {
    StoreFunctions<TestMessageEnvelope, String, WindowState> mockStoreFunctions = mock(StoreFunctions.class);
    // test constructor
    StateStoreImpl<TestMessageEnvelope, String, WindowState> storeImpl = new StateStoreImpl<>(mockStoreFunctions, "myStoreName");
    TaskContext mockContext = mock(TaskContext.class);
    KeyValueStore<String, WindowState> mockKvStore = mock(KeyValueStore.class);
    when(mockContext.getStore("myStoreName")).thenReturn(mockKvStore);
    // test init()
    storeImpl.init(mockContext);
    verify(mockContext, times(1)).getStore("myStoreName");
    Function<TestMessageEnvelope, String> wndKeyFn = mock(Function.class);
    when(mockStoreFunctions.getStoreKeyFn()).thenReturn(wndKeyFn);
    TestMessageEnvelope mockMsg = mock(TestMessageEnvelope.class);
    when(wndKeyFn.apply(mockMsg)).thenReturn("myKey");
    WindowState mockState = mock(WindowState.class);
    when(mockKvStore.get("myKey")).thenReturn(mockState);
    // test getState()
    Entry<String, WindowState> storeEntry = storeImpl.getState(mockMsg);
    assertEquals(storeEntry.getKey(), "myKey");
    assertEquals(storeEntry.getValue(), mockState);
    verify(wndKeyFn, times(1)).apply(mockMsg);
    verify(mockKvStore, times(1)).get("myKey");
    Entry<String, WindowState> oldEntry = new Entry<>("myKey", mockState);
    WindowState mockNewState = mock(WindowState.class);
    BiFunction<TestMessageEnvelope, WindowState, WindowState> mockUpdaterFn = mock(BiFunction.class);
    when(mockStoreFunctions.getStateUpdaterFn()).thenReturn(mockUpdaterFn);
    when(mockUpdaterFn.apply(mockMsg, mockState)).thenReturn(mockNewState);
    // test updateState()
    Entry<String, WindowState> newEntry = storeImpl.updateState(mockMsg, oldEntry);
    assertEquals(newEntry.getKey(), "myKey");
    assertEquals(newEntry.getValue(), mockNewState);
  }
}
