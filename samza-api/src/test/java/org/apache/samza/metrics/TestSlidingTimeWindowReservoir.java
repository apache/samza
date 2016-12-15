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

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.samza.util.Clock;
import org.junit.Test;

public class TestSlidingTimeWindowReservoir {

  private final Clock clock = mock(Clock.class);

  @Test
  public void testUpdateSizeSnapshot() {
    SlidingTimeWindowReservoir slidingTimeWindowReservoir =
        new SlidingTimeWindowReservoir(300, 8, clock);

    when(clock.currentTimeMillis()).thenReturn(0L);
    slidingTimeWindowReservoir.update(1L);

    when(clock.currentTimeMillis()).thenReturn(1L);
    slidingTimeWindowReservoir.update(2L);

    when(clock.currentTimeMillis()).thenReturn(2L);
    slidingTimeWindowReservoir.update(3L);

    assertEquals(3, slidingTimeWindowReservoir.size());

    Snapshot snapshot = slidingTimeWindowReservoir.getSnapshot();
    assertTrue(snapshot.getValues().containsAll(Arrays.asList(1L, 2L, 3L)));
    assertEquals(3, snapshot.getSize());
  }

  @Test
  public void testDuplicateTime() {
    SlidingTimeWindowReservoir slidingTimeWindowReservoir =
        new SlidingTimeWindowReservoir(300, 2, clock);
    when(clock.currentTimeMillis()).thenReturn(1L);
    slidingTimeWindowReservoir.update(1L);
    slidingTimeWindowReservoir.update(2L);

    Snapshot snapshot = slidingTimeWindowReservoir.getSnapshot();
    assertTrue(snapshot.getValues().containsAll(Arrays.asList(1L, 2L)));
    assertEquals(2, snapshot.getSize());

    // update causes collision, will override the last update
    slidingTimeWindowReservoir.update(3L);
    snapshot = slidingTimeWindowReservoir.getSnapshot();
    assertTrue(snapshot.getValues().containsAll(Arrays.asList(1L, 3L)));
    assertEquals(2, snapshot.getSize());
  }

  @Test
  public void testRemoveExpiredValues() {
    SlidingTimeWindowReservoir slidingTimeWindowReservoir =
        new SlidingTimeWindowReservoir(300, 8, clock);
    when(clock.currentTimeMillis()).thenReturn(0L);
    slidingTimeWindowReservoir.update(1L);

    when(clock.currentTimeMillis()).thenReturn(100L);
    slidingTimeWindowReservoir.update(2L);

    when(clock.currentTimeMillis()).thenReturn(301L);
    slidingTimeWindowReservoir.update(3L);

    when(clock.currentTimeMillis()).thenReturn(500L);
    slidingTimeWindowReservoir.update(4L);

    Snapshot snapshot = slidingTimeWindowReservoir.getSnapshot();
    assertTrue(snapshot.getValues().containsAll(Arrays.asList(3L, 4L)));
    assertEquals(2, snapshot.getSize());
  }

}
