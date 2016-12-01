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
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestTriggerBuilder {
  private Field earlyTriggerField;
  private Field lateTriggerField;
  private Field timerTriggerField;
  private Field earlyTriggerUpdater;
  private Field lateTriggerUpdater;

  @Before
  public void testPrep() throws Exception {
    this.earlyTriggerField = TriggerBuilder.class.getDeclaredField("earlyTrigger");
    this.lateTriggerField = TriggerBuilder.class.getDeclaredField("lateTrigger");
    this.timerTriggerField = TriggerBuilder.class.getDeclaredField("timerTrigger");
    this.earlyTriggerUpdater = TriggerBuilder.class.getDeclaredField("earlyTriggerUpdater");
    this.lateTriggerUpdater = TriggerBuilder.class.getDeclaredField("lateTriggerUpdater");

    this.earlyTriggerField.setAccessible(true);
    this.lateTriggerField.setAccessible(true);
    this.timerTriggerField.setAccessible(true);
    this.earlyTriggerUpdater.setAccessible(true);
    this.lateTriggerUpdater.setAccessible(true);
  }

  @Test
  public void testStaticCreators() throws NoSuchFieldException, IllegalAccessException {
    TriggerBuilder<TestMessageEnvelope, Collection<TestMessageEnvelope>> builder = TriggerBuilder.earlyTriggerWhenExceedWndLen(1000);
    BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean> triggerField =
        (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    WindowState<Collection<TestMessageEnvelope>> mockState = mock(WindowState.class);
    when(mockState.getNumberMessages()).thenReturn(200L);
    assertFalse(triggerField.apply(null, mockState));
    when(mockState.getNumberMessages()).thenReturn(2000L);
    assertTrue(triggerField.apply(null, mockState));

    Function<TestMessageEnvelope, Boolean> tokenFunc = m -> true;
    builder = TriggerBuilder.earlyTriggerOnTokenMsg(tokenFunc);
    triggerField = (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    TestMessageEnvelope m = mock(TestMessageEnvelope.class);
    assertTrue(triggerField.apply(m, mockState));

    builder = TriggerBuilder.earlyTriggerOnEventTime(tm -> tm.getMessage().getEventTime(), 30000L);
    triggerField = (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    when(mockState.getEarliestEventTimeNs()).thenReturn(1000000000L);
    when(mockState.getLatestEventTimeNs()).thenReturn(20000000000L);
    TestMessageEnvelope.MessageType mockInnerMessage;
    mockInnerMessage = mock(TestMessageEnvelope.MessageType.class);
    when(mockInnerMessage.getEventTime()).thenReturn(19999000000L);
    when(m.getMessage()).thenReturn(mockInnerMessage);
    assertFalse(triggerField.apply(m, mockState));
    mockInnerMessage = mock(TestMessageEnvelope.MessageType.class);
    when(mockInnerMessage.getEventTime()).thenReturn(32000000000L);
    when(m.getMessage()).thenReturn(mockInnerMessage);
    assertTrue(triggerField.apply(m, mockState));
    mockInnerMessage = mock(TestMessageEnvelope.MessageType.class);
    when(m.getMessage()).thenReturn(mockInnerMessage);
    when(mockInnerMessage.getEventTime()).thenReturn(1001000000L);
    when(mockState.getLatestEventTimeNs()).thenReturn(32000000000L);
    assertTrue(triggerField.apply(m, mockState));

    BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean> mockFunc = mock(BiFunction.class);
    builder = TriggerBuilder.earlyTrigger(mockFunc);
    triggerField = (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    assertEquals(triggerField, mockFunc);

    builder = TriggerBuilder.timeoutSinceFirstMessage(10000L);
    Function<WindowState<Collection<TestMessageEnvelope>>, Boolean> timerTrigger =
        (Function<WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.timerTriggerField.get(builder);
    when(mockState.getFirstMessageTimeNs()).thenReturn(0L);
    assertTrue(timerTrigger.apply(mockState));
    // set the firstMessageTimeNs to 9 second earlier, giving the test 1 second to fire up the timerTrigger before assertion
    when(mockState.getFirstMessageTimeNs()).thenReturn(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - 9000L));
    assertFalse(timerTrigger.apply(mockState));

    builder = TriggerBuilder.timeoutSinceLastMessage(10000L);
    timerTrigger = (Function<WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.timerTriggerField.get(builder);
    when(mockState.getLastMessageTimeNs()).thenReturn(0L);
    assertTrue(timerTrigger.apply(mockState));
    // set the lastMessageTimeNs to 9 second earlier, giving the test 1 second to fire up the timerTrigger before assertion
    when(mockState.getLastMessageTimeNs()).thenReturn(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - 9000));
    assertFalse(timerTrigger.apply(mockState));
  }

  @Test
  public void testAddTimerTriggers() throws IllegalAccessException {
    TriggerBuilder<TestMessageEnvelope, Collection<TestMessageEnvelope>> builder = TriggerBuilder.earlyTriggerWhenExceedWndLen(1000);
    builder.addTimeoutSinceFirstMessage(10000L);
    // exam that both earlyTrigger and timer triggers are set up
    BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean> triggerField =
        (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    WindowState<Collection<TestMessageEnvelope>> mockState = mock(WindowState.class);
    when(mockState.getNumberMessages()).thenReturn(200L);
    assertFalse(triggerField.apply(null, mockState));
    // check the timer trigger
    Function<WindowState<Collection<TestMessageEnvelope>>, Boolean> timerTrigger =
        (Function<WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.timerTriggerField.get(builder);
    when(mockState.getFirstMessageTimeNs()).thenReturn(0L);
    assertTrue(timerTrigger.apply(mockState));
    // set the firstMessageTimeNs to 9 second earlier, giving the test 1 second to fire up the timerTrigger before assertion
    when(mockState.getFirstMessageTimeNs()).thenReturn(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - 9000L));
    assertFalse(timerTrigger.apply(mockState));

    // exam that both early trigger and timer triggers are set up
    builder = TriggerBuilder.earlyTriggerWhenExceedWndLen(1000);
    triggerField = (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    mockState = mock(WindowState.class);
    when(mockState.getNumberMessages()).thenReturn(200L);
    assertFalse(triggerField.apply(null, mockState));
    builder.addTimeoutSinceLastMessage(20000L);
    // check the timer trigger
    timerTrigger = (Function<WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.timerTriggerField.get(builder);
    when(mockState.getLastMessageTimeNs()).thenReturn(0L);
    assertTrue(timerTrigger.apply(mockState));
    // set the firstMessageTimeNs to 9 second earlier, giving the test 1 second to fire up the timerTrigger before assertion
    when(mockState.getLastMessageTimeNs()).thenReturn(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - 9000L));
    assertFalse(timerTrigger.apply(mockState));
  }

  @Test
  public void testAddLateTriggers() throws IllegalAccessException {
    TriggerBuilder<TestMessageEnvelope, Collection<TestMessageEnvelope>> builder = TriggerBuilder.earlyTriggerWhenExceedWndLen(1000);
    builder.addLateTriggerOnSizeLimit(10000L);
    // exam that both earlyTrigger and lateTriggers are set up
    BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean> earlyTrigger =
        (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    WindowState<Collection<TestMessageEnvelope>> mockState = mock(WindowState.class);
    when(mockState.getNumberMessages()).thenReturn(200L);
    assertFalse(earlyTrigger.apply(null, mockState));
    // check the late trigger
    BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean> lateTrigger =
        (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.lateTriggerField.get(builder);
    assertFalse(lateTrigger.apply(null, mockState));
    // set the number of messages to 10001 to trigger the late trigger
    when(mockState.getNumberMessages()).thenReturn(10001L);
    assertTrue(lateTrigger.apply(null, mockState));

    builder = TriggerBuilder.earlyTriggerWhenExceedWndLen(1000);
    builder.addLateTrigger((m, s) -> s.getOutputValue().size() > 0);
    // exam that both earlyTrigger and lateTriggers are set up
    earlyTrigger = (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.earlyTriggerField.get(builder);
    mockState = mock(WindowState.class);
    when(mockState.getNumberMessages()).thenReturn(200L);
    assertFalse(earlyTrigger.apply(null, mockState));
    // exam the lateTrigger
    when(mockState.getOutputValue()).thenReturn(new ArrayList<>());
    lateTrigger = (BiFunction<TestMessageEnvelope, WindowState<Collection<TestMessageEnvelope>>, Boolean>) this.lateTriggerField.get(builder);
    assertFalse(lateTrigger.apply(null, mockState));
    List<TestMessageEnvelope> mockList = mock(ArrayList.class);
    when(mockList.size()).thenReturn(200);
    when(mockState.getOutputValue()).thenReturn(mockList);
    assertTrue(lateTrigger.apply(null, mockState));
  }

  @Test
  public void testAddTriggerUpdater() throws IllegalAccessException {
    TriggerBuilder<TestMessageEnvelope, Collection<TestMessageEnvelope>> builder = TriggerBuilder.earlyTriggerWhenExceedWndLen(1000);
    builder.onEarlyTrigger(c -> {
        c.clear();
        return c;
      });
    List<TestMessageEnvelope> collection = new ArrayList<TestMessageEnvelope>() { {
        for (int i = 0; i < 10; i++) {
          this.add(new TestMessageEnvelope(String.format("key-%d", i), "string-value", System.nanoTime()));
        }
      } };
    // exam that earlyTriggerUpdater is set up
    Function<WindowState<Collection<TestMessageEnvelope>>, WindowState<Collection<TestMessageEnvelope>>> earlyTriggerUpdater =
        (Function<WindowState<Collection<TestMessageEnvelope>>, WindowState<Collection<TestMessageEnvelope>>>) this.earlyTriggerUpdater.get(builder);
    WindowState<Collection<TestMessageEnvelope>> mockState = mock(WindowState.class);
    when(mockState.getOutputValue()).thenReturn(collection);
    earlyTriggerUpdater.apply(mockState);
    assertTrue(collection.isEmpty());

    collection.add(new TestMessageEnvelope("key-to-stay", "string-to-stay", System.nanoTime()));
    collection.add(new TestMessageEnvelope("key-to-remove", "string-to-remove", System.nanoTime()));
    builder.onLateTrigger(c -> {
        c.removeIf(t -> t.getKey().equals("key-to-remove"));
        return c;
      });
    // check the late trigger updater
    Function<WindowState<Collection<TestMessageEnvelope>>, WindowState<Collection<TestMessageEnvelope>>> lateTriggerUpdater =
        (Function<WindowState<Collection<TestMessageEnvelope>>, WindowState<Collection<TestMessageEnvelope>>>) this.lateTriggerUpdater.get(builder);
    when(mockState.getOutputValue()).thenReturn(collection);
    lateTriggerUpdater.apply(mockState);
    assertTrue(collection.size() == 1);
    assertFalse(collection.get(0).isDelete());
    assertEquals(collection.get(0).getKey(), "key-to-stay");
  }
}
