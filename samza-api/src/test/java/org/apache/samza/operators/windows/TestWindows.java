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
package org.apache.samza.operators.windows;

import org.apache.samza.operators.TestMessageEnvelope;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestWindows {

  @Test
  public void testSessionWindows() throws NoSuchFieldException, IllegalAccessException {
    // test constructing the default session window
    Window<TestMessageEnvelope, String, Collection<TestMessageEnvelope>, WindowOutput<String, Collection<TestMessageEnvelope>>> testWnd = Windows
        .intoSessions(
        TestMessageEnvelope::getKey);
    assertTrue(testWnd instanceof SessionWindow);
    Field wndKeyFuncField = SessionWindow.class.getDeclaredField("wndKeyFunction");
    Field aggregatorField = SessionWindow.class.getDeclaredField("aggregator");
    wndKeyFuncField.setAccessible(true);
    aggregatorField.setAccessible(true);
    Function<TestMessageEnvelope, String> wndKeyFunc = (Function<TestMessageEnvelope, String>) wndKeyFuncField.get(testWnd);
    assertEquals(wndKeyFunc.apply(new TestMessageEnvelope("test-key", "test-value", 0)), "test-key");
    BiFunction<TestMessageEnvelope, Collection<TestMessageEnvelope>, Collection<TestMessageEnvelope>> aggrFunc =
        (BiFunction<TestMessageEnvelope, Collection<TestMessageEnvelope>, Collection<TestMessageEnvelope>>) aggregatorField.get(testWnd);
    TestMessageEnvelope mockMsg = mock(TestMessageEnvelope.class);
    Collection<TestMessageEnvelope> collection = aggrFunc.apply(mockMsg, new ArrayList<>());
    assertTrue(collection.size() == 1);
    assertTrue(collection.contains(mockMsg));

    // test constructing the session window w/ customized session info
    Window<TestMessageEnvelope, String, Collection<Character>, WindowOutput<String, Collection<Character>>> testWnd2 = Windows.intoSessions(
        m -> String.format("key-%d", m.getMessage().getEventTime()), m -> m.getMessage().getValue().charAt(0));
    assertTrue(testWnd2 instanceof SessionWindow);
    wndKeyFunc = (Function<TestMessageEnvelope, String>) wndKeyFuncField.get(testWnd2);
    aggrFunc = (BiFunction<TestMessageEnvelope, Collection<TestMessageEnvelope>, Collection<TestMessageEnvelope>>) aggregatorField.get(testWnd2);
    assertEquals(wndKeyFunc.apply(new TestMessageEnvelope("test-key", "test-value", 0)), "key-0");
    TestMessageEnvelope.MessageType mockInnerMessage = mock(TestMessageEnvelope.MessageType.class);
    when(mockMsg.getMessage()).thenReturn(mockInnerMessage);
    when(mockInnerMessage.getValue()).thenReturn("x-001");
    collection = aggrFunc.apply(mockMsg, new ArrayList<>());
    assertTrue(collection.size() == 1);
    assertTrue(collection.contains('x'));

    // test constructing session window w/ a default counter
    Window<TestMessageEnvelope, String, Integer, WindowOutput<String, Integer>> testCounter = Windows.intoSessionCounter(
        m -> String.format("key-%d", m.getMessage().getEventTime()));
    assertTrue(testCounter instanceof SessionWindow);
    wndKeyFunc = (Function<TestMessageEnvelope, String>) wndKeyFuncField.get(testCounter);
    BiFunction<TestMessageEnvelope, Integer, Integer> counterFn = (BiFunction<TestMessageEnvelope, Integer, Integer>) aggregatorField.get(testCounter);
    when(mockMsg.getMessage().getEventTime()).thenReturn(12345L);
    assertEquals(wndKeyFunc.apply(mockMsg), "key-12345");
    assertEquals(counterFn.apply(mockMsg, 1), Integer.valueOf(2));
  }

  @Test
  public void testSetTriggers() throws NoSuchFieldException, IllegalAccessException {
    Window<TestMessageEnvelope, String, Integer, WindowOutput<String, Integer>> testCounter = Windows.intoSessionCounter(
        m -> String.format("key-%d", m.getMessage().getEventTime()));
    // test session window w/ a trigger
    TriggerBuilder<TestMessageEnvelope, Integer> triggerBuilder = TriggerBuilder.earlyTriggerWhenExceedWndLen(1000L);
    testCounter.setTriggers(triggerBuilder);
    Trigger<TestMessageEnvelope, WindowState<Integer>> expectedTrigger = triggerBuilder.build();
    Trigger<TestMessageEnvelope, WindowState<Integer>> actualTrigger = Windows.getInternalWindowFn(testCounter).getTrigger();
    // examine all trigger fields are expected
    Field earlyTriggerField = Trigger.class.getDeclaredField("earlyTrigger");
    Field lateTriggerField = Trigger.class.getDeclaredField("lateTrigger");
    Field timerTriggerField = Trigger.class.getDeclaredField("timerTrigger");
    Field earlyTriggerUpdater = Trigger.class.getDeclaredField("earlyTriggerUpdater");
    Field lateTriggerUpdater = Trigger.class.getDeclaredField("lateTriggerUpdater");
    earlyTriggerField.setAccessible(true);
    lateTriggerField.setAccessible(true);
    timerTriggerField.setAccessible(true);
    earlyTriggerUpdater.setAccessible(true);
    lateTriggerUpdater.setAccessible(true);
    assertEquals(earlyTriggerField.get(expectedTrigger), earlyTriggerField.get(actualTrigger));
    assertEquals(lateTriggerField.get(expectedTrigger), lateTriggerField.get(actualTrigger));
    assertEquals(timerTriggerField.get(expectedTrigger), timerTriggerField.get(actualTrigger));
    assertEquals(earlyTriggerUpdater.get(expectedTrigger), earlyTriggerUpdater.get(actualTrigger));
    assertEquals(lateTriggerUpdater.get(expectedTrigger), lateTriggerUpdater.get(actualTrigger));
  }
}
