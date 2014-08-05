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

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class TestSnapshot {

  @Test
  public void testGetMaxMinAverageSize() {
    Snapshot snapshot = new Snapshot(Arrays.asList(1L, 2L, 3L, 4L, 5L));
    assertEquals(5, snapshot.getMax());
    assertEquals(1, snapshot.getMin());
    assertEquals(3, snapshot.getAverage(), 0);
    assertEquals(5, snapshot.getSize());

    Snapshot emptySnapshot = new Snapshot(new ArrayList<Long>());
    assertEquals(0, emptySnapshot.getMax());
    assertEquals(0, emptySnapshot.getMin());
    assertEquals(0, emptySnapshot.getAverage(), 0);
    assertEquals(0, emptySnapshot.getSize());
  }
}
