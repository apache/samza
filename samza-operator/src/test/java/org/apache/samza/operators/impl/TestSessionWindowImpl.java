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
package org.apache.samza.operators.impl;

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.TestMessageEnvelope;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.windows.StoreFunctions;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.WindowState;
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
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestSessionWindowImpl {
  Field wndStoreField = null;
  Field sessWndField = null;

  @Before public void prep() throws NoSuchFieldException {
    wndStoreField = SessionWindowOperatorImpl.class.getDeclaredField("stateStore");
    sessWndField = SessionWindowOperatorImpl.class.getDeclaredField("windowSpec");
    wndStoreField.setAccessible(true);
    sessWndField.setAccessible(true);
  }

  @Test
  public void testConstructor() throws IllegalAccessException, NoSuchFieldException {
    // test constructing a SessionWindowOperatorImpl w/ expected mock functions
    WindowOperatorSpec<TestMessageEnvelope, String, WindowState<Integer>, WindowOutput<String, Integer>> wndOp = mock(WindowOperatorSpec.class);
    SessionWindowOperatorImpl<TestMessageEnvelope, String, WindowState<Integer>, WindowOutput<String, Integer>> sessWnd = new SessionWindowOperatorImpl<>(wndOp);
    assertEquals(wndOp, sessWndField.get(sessWnd));
  }

  @Test
  public void testInitAndProcess() throws IllegalAccessException {
    WindowOperatorSpec<TestMessageEnvelope, String, WindowState<Integer>, WindowOutput<String, Integer>> wndOp = mock(WindowOperatorSpec.class);
    BiFunction<TestMessageEnvelope, Entry<String, WindowState<Integer>>, WindowOutput<String, Integer>> mockTxfmFn = mock(BiFunction.class);
    SessionWindowOperatorImpl<TestMessageEnvelope, String, WindowState<Integer>, WindowOutput<String, Integer>> sessWnd = new SessionWindowOperatorImpl<>(wndOp);

    // construct and init the SessionWindowOperatorImpl object
    MessageStreamImpl<TestMessageEnvelope> mockInputStrm = mock(MessageStreamImpl.class);
    StoreFunctions<TestMessageEnvelope, String, WindowState<Integer>> mockStoreFns = mock(StoreFunctions.class);
    Function<TestMessageEnvelope, String> wndKeyFn = m -> "test-msg-key";
    when(mockStoreFns.getStoreKeyFn()).thenReturn(wndKeyFn);
    when(wndOp.getStoreFns()).thenReturn(mockStoreFns);
    when(wndOp.getStoreName(mockInputStrm)).thenReturn("test-wnd-store");
    when(wndOp.getTransformFn()).thenReturn(mockTxfmFn);
    TaskContext mockContext = mock(TaskContext.class);
    KeyValueStore<String, WindowState<Integer>> mockKvStore = mock(KeyValueStore.class);
    when(mockContext.getStore("test-wnd-store")).thenReturn(mockKvStore);
    sessWnd.init(mockInputStrm, mockContext);

    // test onNext() method. Make sure the transformation function and the state update functions are invoked.
    TestMessageEnvelope mockMsg = mock(TestMessageEnvelope.class);
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);
    BiFunction<TestMessageEnvelope, WindowState<Integer>, WindowState<Integer>> stateUpdaterFn = mock(BiFunction.class);
    when(mockStoreFns.getStateUpdaterFn()).thenReturn(stateUpdaterFn);
    WindowState<Integer> mockNewState = mock(WindowState.class);
    WindowState<Integer> oldState = mock(WindowState.class);
    when(mockKvStore.get("test-msg-key")).thenReturn(oldState);
    when(stateUpdaterFn.apply(mockMsg, oldState)).thenReturn(mockNewState);
    sessWnd.onNext(mockMsg, mockCollector, mockCoordinator);
    verify(mockTxfmFn, times(1)).apply(argThat(new ArgumentMatcher<TestMessageEnvelope>() {
      @Override public boolean matches(Object argument) {
        TestMessageEnvelope xIn = (TestMessageEnvelope) argument;
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
