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

package org.apache.samza.operators.spec;

import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.serializers.Serde;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SupplierFunction;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class TestWindowOperatorSpec {

  private Trigger defaultTrigger;
  private Trigger earlyTrigger;
  private Trigger lateTrigger;
  private FoldLeftFunction<Object, Collection> foldFn;
  private SupplierFunction<Collection> supplierFunction;
  private MapFunction<Object, Object> keyFn;
  private MapFunction<Object, Long> timeFn;

  @Before
  public void setup() {

    foldFn = (m, c) -> {
      c.add(m);
      return c;
    };
    supplierFunction = () -> new ArrayList<>();
    keyFn = m -> m.toString();
    timeFn = m -> 123456L;

    defaultTrigger = Triggers.timeSinceFirstMessage(Duration.ofMillis(150));
    earlyTrigger = Triggers.repeat(Triggers.count(5));
    lateTrigger = null;
  }

  @Test
  public void testTriggerIntervalWithNestedTimeTriggers() {
    defaultTrigger = Triggers.timeSinceFirstMessage(Duration.ofMillis(150));
    lateTrigger = Triggers.any(Triggers.count(6), Triggers.timeSinceFirstMessage(Duration.ofMillis(15)));
    earlyTrigger = Triggers.repeat(
      Triggers.any(Triggers.count(23),
          Triggers.timeSinceFirstMessage(Duration.ofMillis(15)),
          Triggers.any(Triggers.any(Triggers.count(6),
              Triggers.timeSinceFirstMessage(Duration.ofMillis(15)),
              Triggers.timeSinceFirstMessage(Duration.ofMillis(25)),
              Triggers.timeSinceLastMessage(Duration.ofMillis(15))))));

    WindowOperatorSpec spec = getWindowOperatorSpec("w0");
    assertEquals(spec.getDefaultTriggerMs(), 5);
  }

  @Test
  public void testTriggerIntervalWithSingleTimeTrigger() {
    WindowOperatorSpec spec = getWindowOperatorSpec("w0");
    assertEquals(spec.getDefaultTriggerMs(), 150);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalTimerFunctionAsInitializer() {
    class TimedSupplierFunction implements SupplierFunction<Collection>, TimerFunction<Object, Collection> {

      @Override
      public Collection get() {
        return new ArrayList<>();
      }

      @Override
      public void registerTimer(TimerRegistry<Object> timerRegistry) {

      }

      @Override
      public Collection<Collection> onTimer(Object key, long timestamp) {
        return null;
      }
    }
    supplierFunction = new TimedSupplierFunction();

    getWindowOperatorSpec("w0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalWatermarkFunctionAsInitializer() {
    class WatermarkSupplierFunction implements SupplierFunction<Collection>, WatermarkFunction<Collection> {

      @Override
      public Collection get() {
        return new ArrayList<>();
      }

      @Override
      public Collection<Collection> processWatermark(long watermark) {
        return null;
      }

      @Override
      public Long getOutputWatermark() {
        return null;
      }
    }
    supplierFunction = new WatermarkSupplierFunction();

    getWindowOperatorSpec("w0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalTimerFunctionAsKeyFn() {
    class TimerMapFunction implements MapFunction<Object, Object>, TimerFunction<Object, Object> {

      @Override
      public Object apply(Object message) {
        return message.toString();
      }

      @Override
      public void registerTimer(TimerRegistry<Object> timerRegistry) {

      }

      @Override
      public Collection<Object> onTimer(Object key, long timestamp) {
        return null;
      }
    }
    keyFn = new TimerMapFunction();

    getWindowOperatorSpec("w0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalWatermarkFunctionAsKeyFn() {
    class WatermarkMapFunction implements MapFunction<Object, Object>, WatermarkFunction<Object> {

      @Override
      public Object apply(Object message) {
        return message.toString();
      }

      @Override
      public Collection<Object> processWatermark(long watermark) {
        return null;
      }

      @Override
      public Long getOutputWatermark() {
        return null;
      }
    }
    keyFn = new WatermarkMapFunction();

    getWindowOperatorSpec("w0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalTimerFunctionAsEventTimeFn() {
    class TimerMapFunction implements MapFunction<Object, Long>, TimerFunction<Object, Object> {

      @Override
      public Long apply(Object message) {
        return 123456L;
      }

      @Override
      public void registerTimer(TimerRegistry<Object> timerRegistry) {

      }

      @Override
      public Collection<Object> onTimer(Object key, long timestamp) {
        return null;
      }
    }
    timeFn = new TimerMapFunction();

    getWindowOperatorSpec("w0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalWatermarkFunctionAsEventTimeFn() {
    class WatermarkMapFunction implements MapFunction<Object, Long>, WatermarkFunction<Object> {

      @Override
      public Long apply(Object message) {
        return 123456L;
      }

      @Override
      public Collection<Object> processWatermark(long watermark) {
        return null;
      }

      @Override
      public Long getOutputWatermark() {
        return null;
      }
    }
    timeFn = new WatermarkMapFunction();

    getWindowOperatorSpec("w0");
  }

  @Test
  public void testTimerFunctionAsFoldLeftFn() {
    class TimerFoldLeftFunction implements FoldLeftFunction<Object, Collection>, TimerFunction<Object, Collection> {

      @Override
      public Collection apply(Object message, Collection oldValue) {
        oldValue.add(message);
        return oldValue;
      }

      @Override
      public void registerTimer(TimerRegistry<Object> timerRegistry) {

      }

      @Override
      public Collection<Collection> onTimer(Object key, long timestamp) {
        return null;
      }
    }

    foldFn = new TimerFoldLeftFunction();
    WindowOperatorSpec<Object, Object, Collection> windowSpec = getWindowOperatorSpec("w0");
    assertEquals(windowSpec.getTimerFn(), foldFn);
    assertNull(windowSpec.getWatermarkFn());
  }

  @Test
  public void testWatermarkFunctionAsFoldLeftFn() {
    class WatermarkFoldLeftFunction implements FoldLeftFunction<Object, Collection>, WatermarkFunction<Object> {

      @Override
      public Collection<Object> processWatermark(long watermark) {
        return null;
      }

      @Override
      public Long getOutputWatermark() {
        return null;
      }

      @Override
      public Collection apply(Object message, Collection oldValue) {
        oldValue.add(message);
        return oldValue;
      }
    }

    foldFn = new WatermarkFoldLeftFunction();
    WindowOperatorSpec<Object, Object, Collection> windowSpec = getWindowOperatorSpec("w0");
    assertEquals(windowSpec.getWatermarkFn(), foldFn);
    assertNull(windowSpec.getTimerFn());
  }

  @Test
  public void testCopy() {
    WindowInternal<Object, Object, Collection> window = new WindowInternal<Object, Object, Collection>(
        defaultTrigger, supplierFunction, foldFn, keyFn, timeFn, WindowType.SESSION, null,
        mock(Serde.class), mock(Serde.class));
    window.setEarlyTrigger(earlyTrigger);

    WindowOperatorSpec<Object, Object, Collection> spec = new WindowOperatorSpec<>(window, "w0");

    WindowOperatorSpec<Object, Object, Collection> copy =
        (WindowOperatorSpec<Object, Object, Collection>) OperatorSpecTestUtils.copyOpSpec(spec);

    Assert.assertNotEquals(spec, copy);
    Assert.assertTrue(spec.isClone(copy));
    Assert.assertNotEquals(spec.getWindow(), copy.getWindow());
    Assert.assertNotEquals(copy.getWindow().getInitializer(), supplierFunction);
    assertEquals(copy.getWindow().getInitializer().get(), supplierFunction.get());
    Assert.assertNotEquals(copy.getWindow().getFoldLeftFunction(), foldFn);
    Object mockMsg = new Object();
    assertEquals(copy.getWindow().getFoldLeftFunction().apply(mockMsg, new ArrayList<>()), foldFn.apply(mockMsg, new ArrayList<>()));
    Assert.assertNotEquals(copy.getWindow().getKeyExtractor(), keyFn);
    assertEquals(copy.getWindow().getKeyExtractor().apply(mockMsg), keyFn.apply(mockMsg));
    Assert.assertNotEquals(copy.getWindow().getEventTimeExtractor(), timeFn);
    assertEquals(copy.getWindow().getEventTimeExtractor().apply(mockMsg), timeFn.apply(mockMsg));
    assertEquals(copy.getDefaultTriggerMs(), 150);
  }

  private WindowOperatorSpec getWindowOperatorSpec(String opId) {
    WindowInternal<Object, Object, Collection> window = new WindowInternal<Object, Object, Collection>(
        defaultTrigger, supplierFunction, foldFn, keyFn, timeFn, WindowType.SESSION, null,
        mock(Serde.class), mock(Serde.class));
    if (earlyTrigger != null) {
      window.setEarlyTrigger(earlyTrigger);
    }
    if (lateTrigger != null) {
      window.setLateTrigger(lateTrigger);
    }
    return new WindowOperatorSpec<>(window, opId);
  }

}
