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
package org.apache.samza.operators.internal;

import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.data.Message;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;


public class TestTrigger {

  @Test public void testConstructor() throws Exception {
    BiFunction<Message<Object, Object>, WindowState<Integer>, Boolean> earlyTrigger = (m, s) -> s.getOutputValue() > 1000;
    BiFunction<Message<Object, Object>, WindowState<Integer>, Boolean> lateTrigger = (m, s) -> s.getOutputValue() > 1000;
    Function<WindowState<Integer>, Boolean> timerTrigger = s -> TimeUnit.NANOSECONDS.toMillis(s.getLastMessageTimeNs()) + 50000 < System.currentTimeMillis();
    Function<WindowState<Integer>, WindowState<Integer>> earlyTriggerUpdater = s -> {
      s.setOutputValue(0);
      return s;
    };
    Function<WindowState<Integer>, WindowState<Integer>> lateTriggerUpdater = s -> {
      s.setOutputValue(1);
      return s;
    };

    Trigger<Message<Object, Object>, WindowState<Integer>> trigger = Trigger.createTrigger(timerTrigger, earlyTrigger, lateTrigger,
        earlyTriggerUpdater, lateTriggerUpdater);

    Field earlyTriggerField = Trigger.class.getDeclaredField("earlyTrigger");
    Field timerTriggerField = Trigger.class.getDeclaredField("timerTrigger");
    Field lateTriggerField = Trigger.class.getDeclaredField("lateTrigger");
    Field earlyTriggerUpdaterField = Trigger.class.getDeclaredField("earlyTriggerUpdater");
    Field lateTriggerUpdaterField = Trigger.class.getDeclaredField("lateTriggerUpdater");
    earlyTriggerField.setAccessible(true);
    lateTriggerField.setAccessible(true);
    timerTriggerField.setAccessible(true);
    earlyTriggerUpdaterField.setAccessible(true);
    lateTriggerUpdaterField.setAccessible(true);

    assertEquals(earlyTrigger, earlyTriggerField.get(trigger));
    assertEquals(timerTrigger, timerTriggerField.get(trigger));
    assertEquals(lateTrigger, lateTriggerField.get(trigger));
    assertEquals(earlyTriggerUpdater, earlyTriggerUpdaterField.get(trigger));
    assertEquals(lateTriggerUpdater, lateTriggerUpdaterField.get(trigger));
  }
}
