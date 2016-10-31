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

import junit.framework.Assert;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.TestMessage;
import org.apache.samza.operators.TriggerBuilder;
import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.Windows;
import org.apache.samza.operators.impl.ProcessorContext;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.internal.Operators.WindowOperator;
import org.apache.samza.operators.internal.WindowFn;
import org.apache.samza.operators.internal.WindowOutput;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

  @Test
  public void testSessionWindowLogic() throws Exception {
    Function<TestMessage, String> keyFunction = (m) -> m.getKey();
    MessageCollector mockCollector = mock(MessageCollector.class);
    TaskCoordinator mockCoordinator = mock(TaskCoordinator.class);

    final Windows.Window<TestMessage, String, Integer, WindowOutput<String, Integer>> window = Windows.
        intoSessionCounter(keyFunction)
        .setTriggers(TriggerBuilder.earlyTriggerWhenExceedWndLen(2));

    WindowFn internalWindowFn = Windows.getInternalWindowFn(window);
    MessageStream stream = new MessageStream();

    WindowOperator sessionWindow = Operators.getWindowOperator(internalWindowFn);
    SessionWindowImpl windowImpl = new SessionWindowImpl(sessionWindow);
    final AtomicInteger windowTriggerCount = new AtomicInteger(0);

    windowImpl.subscribe(new Subscriber<ProcessorContext>() {
      @Override
      public void onSubscribe(Subscription s) {

      }

      @Override
      public void onNext(ProcessorContext processorContext) {
        windowTriggerCount.incrementAndGet();
        WindowOutput<String, Integer> output = (WindowOutput) processorContext.getMessage();
        Assert.assertEquals(output.getKey(), "key1");
        Integer value = output.getMessage();
        Assert.assertEquals(value.intValue(), 3);
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onComplete() {

      }
    });

    String jsonString = "{\"userId\":3, \"urlId\":\"google\", \"region\":\"india\"}";
    ObjectMapper mapper = new ObjectMapper();
    Map<String,Object> map = mapper.readValue(jsonString, Map.class);
    System.out.println(map);
  }


}
