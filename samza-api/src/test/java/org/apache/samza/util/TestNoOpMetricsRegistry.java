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

package org.apache.samza.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Timer;

public class TestNoOpMetricsRegistry {
  @Test
  public void testNoOpMetricsHappyPath() {
    NoOpMetricsRegistry registry = new NoOpMetricsRegistry();
    Counter counter1 = registry.newCounter("testc", "a");
    Counter counter2 = registry.newCounter("testc", "b");
    Counter counter3 = registry.newCounter("testc2", "c");
    Gauge<String> gauge1 = registry.newGauge("testg", "a", "1");
    Gauge<String> gauge2 = registry.newGauge("testg", "b", "2");
    Gauge<String> gauge3 = registry.newGauge("testg", "c", "3");
    Gauge<String> gauge4 = registry.newGauge("testg2", "d", "4");
    Timer timer1 = registry.newTimer("testt", "a");
    Timer timer2 = registry.newTimer("testt", "b");
    Timer timer3 = registry.newTimer("testt2", "c");
    counter1.inc();
    counter2.inc(2);
    counter3.inc(4);
    gauge1.set("5");
    gauge2.set("6");
    gauge3.set("7");
    gauge4.set("8");
    timer1.update(1L);
    timer2.update(2L);
    timer3.update(3L);
    assertEquals(counter1.getCount(), 1);
    assertEquals(counter2.getCount(), 2);
    assertEquals(counter3.getCount(), 4);
    assertEquals(gauge1.getValue(), "5");
    assertEquals(gauge2.getValue(), "6");
    assertEquals(gauge3.getValue(), "7");
    assertEquals(gauge4.getValue(), "8");
    assertEquals(timer1.getSnapshot().getAverage(), 1, 0);
    assertEquals(timer2.getSnapshot().getAverage(), 2, 0);
    assertEquals(timer3.getSnapshot().getAverage(), 3, 0);
  }
}
