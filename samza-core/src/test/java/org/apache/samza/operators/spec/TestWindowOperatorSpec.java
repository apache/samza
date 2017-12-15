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

import org.apache.samza.serializers.Serde;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SupplierFunction;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

import static org.mockito.Mockito.mock;

public class TestWindowOperatorSpec {
  @Test
  public void testTriggerIntervalWithNestedTimeTriggers() {
    Trigger defaultTrigger = Triggers.timeSinceFirstMessage(Duration.ofMillis(150));
    Trigger lateTrigger = Triggers.any(Triggers.count(6), Triggers.timeSinceFirstMessage(Duration.ofMillis(15)));
    Trigger earlyTrigger = Triggers.repeat(
        Triggers.any(Triggers.count(23),
            Triggers.timeSinceFirstMessage(Duration.ofMillis(15)),
            Triggers.any(Triggers.any(Triggers.count(6),
                Triggers.timeSinceFirstMessage(Duration.ofMillis(15)),
                Triggers.timeSinceFirstMessage(Duration.ofMillis(25)),
                Triggers.timeSinceLastMessage(Duration.ofMillis(15))))));

    WindowInternal window = new WindowInternal(defaultTrigger, null, null, null,
            null, WindowType.SESSION, null, null, mock(Serde.class));
    window.setEarlyTrigger(earlyTrigger);
    window.setLateTrigger(lateTrigger);

    WindowOperatorSpec spec = new WindowOperatorSpec(window, 0);
    Assert.assertEquals(spec.getDefaultTriggerMs(), 5);
  }

  @Test
  public void testTriggerIntervalWithSingleTimeTrigger() {
    Trigger defaultTrigger = Triggers.timeSinceFirstMessage(Duration.ofMillis(150));
    Trigger earlyTrigger = Triggers.repeat(Triggers.count(5));

    WindowInternal window = new WindowInternal(defaultTrigger, null, null, null,
            null, WindowType.SESSION, null, null, mock(Serde.class));
    window.setEarlyTrigger(earlyTrigger);

    WindowOperatorSpec spec = new WindowOperatorSpec(window, 0);
    Assert.assertEquals(spec.getDefaultTriggerMs(), 150);
  }

  @Test
  public void testCopy() throws IOException, ClassNotFoundException {
    Trigger defaultTrigger = Triggers.timeSinceFirstMessage(Duration.ofMillis(150));
    Trigger earlyTrigger = Triggers.repeat(Triggers.count(5));

    FoldLeftFunction<Object, Collection> foldFn = (m, c) -> {
      c.add(m);
      return c;
    };
    SupplierFunction<Collection> supplierFunction = () -> new ArrayList<>();
    MapFunction<Object, Object> keyFn = m -> m.toString();
    MapFunction<Object, Long> timeFn = m -> 123456L;

    WindowInternal<Object, Object, Collection> window = new WindowInternal<Object, Object, Collection>(
        defaultTrigger,
        supplierFunction,
        foldFn,
        keyFn,
        timeFn,
        WindowType.SESSION,
        null,
        mock(Serde.class),
        mock(Serde.class));
    window.setEarlyTrigger(earlyTrigger);

    WindowOperatorSpec<Object, Object, Collection> spec = new WindowOperatorSpec<>(window, 0);

    WindowOperatorSpec<Object, Object, Collection> copy = spec.copy();

    Assert.assertNotEquals(spec, copy);
    Assert.assertTrue(spec.isClone(copy));
    Assert.assertNotEquals(spec.getWindow(), copy.getWindow());
    Assert.assertNotEquals(copy.getWindow().getInitializer(), supplierFunction);
    Assert.assertEquals(copy.getWindow().getInitializer().get(), supplierFunction.get());
    Assert.assertNotEquals(copy.getWindow().getFoldLeftFunction(), foldFn);
    Object mockMsg = new Object();
    Assert.assertEquals(copy.getWindow().getFoldLeftFunction().apply(mockMsg, new ArrayList<>()), foldFn.apply(mockMsg, new ArrayList<>()));
    Assert.assertNotEquals(copy.getWindow().getKeyExtractor(), keyFn);
    Assert.assertEquals(copy.getWindow().getKeyExtractor().apply(mockMsg), keyFn.apply(mockMsg));
    Assert.assertNotEquals(copy.getWindow().getEventTimeExtractor(), timeFn);
    Assert.assertEquals(copy.getWindow().getEventTimeExtractor().apply(mockMsg), timeFn.apply(mockMsg));
    Assert.assertEquals(copy.getDefaultTriggerMs(), 150);
  }
}
