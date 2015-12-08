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

package org.apache.samza.metrics;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.samza.util.Clock;
import org.junit.Test;

public class TestTimer {

  /*
   * Mock clock
   */
  private final Clock clock = new Clock() {
    long value = 0;

    @Override
    public long currentTimeMillis() {
      return value += 100;
    }
  };

  @Test
  public void testDefaultTimerUpdateAndGetSnapshot() {
    Timer timer = new Timer("test", 300, clock);
    timer.update(1L);
    timer.update(2L);

    Snapshot snapshot = timer.getSnapshot();
    assertTrue(snapshot.getValues().containsAll(Arrays.asList(1L, 2L)));
    assertEquals(2, snapshot.getValues().size());
  }

  @Test
  public void testTimerWithDifferentWindowSize() {
    Timer timer = new Timer("test", 300, clock);
    timer.update(1L);
    timer.update(2L);
    timer.update(3L);

    Snapshot snapshot = timer.getSnapshot();
    assertTrue(snapshot.getValues().containsAll(Arrays.asList(1L, 2L, 3L)));
    assertEquals(3, snapshot.getValues().size());

    // The time is 500 for update(4L) because getSnapshot calls clock once + 3
    // updates that call clock 3 times
    timer.update(4L);
    Snapshot snapshot2 = timer.getSnapshot();
    assertTrue(snapshot2.getValues().containsAll(Arrays.asList(3L, 4L)));
    assertEquals(2, snapshot2.getValues().size());
  }
}
