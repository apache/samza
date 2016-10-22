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
package org.apache.samza.operators.impl.window;

import org.apache.samza.operators.api.MessageStream;
import org.apache.samza.operators.api.TestMessage;
import org.apache.samza.operators.api.WindowState;
import org.apache.samza.operators.api.internal.Operators.StoreFunctions;
import org.apache.samza.operators.api.internal.Operators.WindowOperator;
import org.apache.samza.operators.api.internal.WindowOutput;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.lang.reflect.Field;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;


public class TestSessionWindowImpl {
  Field wndStoreField = null;
  Field sessWndField = null;

  @Before public void prep() throws NoSuchFieldException {
    wndStoreField = SessionWindowImpl.class.getDeclaredField("wndStore");
    sessWndField = SessionWindowImpl.class.getDeclaredField("sessWnd");
    wndStoreField.setAccessible(true);
    sessWndField.setAccessible(true);
  }

  @Test public void testConstructor() throws IllegalAccessException, NoSuchFieldException {
    // test constructing a SessionWindowImpl w/ expected mock functions
    WindowOperator<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> wndOp = mock(WindowOperator.class);
    SessionWindowImpl<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> sessWnd = new SessionWindowImpl<>(wndOp);
    assertEquals(wndOp, sessWndField.get(sessWnd));
  }

  @Test public void testInitAndProcess() throws IllegalAccessException {
    WindowOperator<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> wndOp = mock(WindowOperator.class);
    BiFunction<TestMessage, Entry<String, WindowState<Integer>>, WindowOutput<String, Integer>> mockTxfmFn = mock(BiFunction.class);
    SessionWindowImpl<TestMessage, String, WindowState<Integer>, WindowOutput<String, Integer>> sessWnd = new SessionWindowImpl<>(wndOp);

    // construct and init the SessionWindowImpl object
    MessageStream<TestMessage> mockInputStrm = mock(MessageStream.class);
    StoreFunctions<TestMessage, String, WindowState<Integer>> mockStoreFns = mock(StoreFunctions.class);
    Function<TestMessage, String> wndKeyFn = m -> "test-msg-key";
    when(mockStoreFns.getStoreKeyFinder()).thenReturn(wndKeyFn);
    when(wndOp.getStoreFunctions()).thenReturn(mockStoreFns);
    when(wndOp.getStoreName(mockInputStrm)).thenReturn("test-wnd-store");
    when(wndOp.getFunction()).thenReturn(mockTxfmFn);
    TaskContext mockContext = mock(TaskContext.class);
    KeyValueStore<String, WindowState<Integer>> mockKvStore = mock(KeyValueStore.class);
    when(mockContext.getStore("test-wnd-store")).thenReturn(mockKvStore);
    sessWnd.init(mockInputStrm, mockContext);

    // test onNext() method. Make sure the transformation function and the state update functions are invoked.
    TestMessage mockMsg = mock(TestMessage.class);
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    BiFunction<TestMessage, WindowState<Integer>, WindowState<Integer>> stateUpdaterFn = mock(BiFunction.class);
    when(mockStoreFns.getStateUpdater()).thenReturn(stateUpdaterFn);
    WindowState<Integer> mockNewState = mock(WindowState.class);
    WindowState<Integer> oldState = mock(WindowState.class);
    when(mockKvStore.get("test-msg-key")).thenReturn(oldState);
    when(stateUpdaterFn.apply(mockMsg, oldState)).thenReturn(mockNewState);
    sessWnd.onNext(mockMsg, mockCollector, mockCoordinator);
    verify(mockTxfmFn, times(1)).apply(argThat(new ArgumentMatcher<TestMessage>() {
      @Override public boolean matches(Object argument) {
        TestMessage xIn = (TestMessage) argument;
        return xIn.equals(mockMsg);
      }
    }), argThat(new ArgumentMatcher<Entry<String, WindowState<Integer>>>() {
      @Override public boolean matches(Object argument) {
        Entry<String, WindowState<Integer>> xIn = (Entry<String, WindowState<Integer>>) argument;
        return xIn.getKey().equals("test-msg-key") && xIn.getValue().equals(oldState);
      }
    }));
    verify(stateUpdaterFn, times(1)).apply(mockMsg, oldState);
    verify(mockKvStore, times(1)).put("test-msg-key", mockNewState);
  }
}
