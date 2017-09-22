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

package org.apache.samza.operators.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.WatermarkMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.apache.samza.operators.impl.WatermarkStates.WATERMARK_NOT_EXIST;

public class TestWatermarkStates {

  @Test
  public void testUpdate() {
    SystemStream input = new SystemStream("system", "input");
    SystemStream intermediate = new SystemStream("system", "intermediate");

    Set<SystemStreamPartition> ssps = new HashSet<>();
    SystemStreamPartition inputPartition0 = new SystemStreamPartition(input, new Partition(0));
    SystemStreamPartition intPartition0 = new SystemStreamPartition(intermediate, new Partition(0));
    SystemStreamPartition intPartition1 = new SystemStreamPartition(intermediate, new Partition(1));
    ssps.add(inputPartition0);
    ssps.add(intPartition0);
    ssps.add(intPartition1);

    Map<SystemStream, Integer> producerCounts = new HashMap<>();
    producerCounts.put(intermediate, 2);

    // advance watermark on input to 5
    WatermarkStates watermarkStates = new WatermarkStates(ssps, producerCounts);
    IncomingMessageEnvelope envelope = IncomingMessageEnvelope.buildWatermarkEnvelope(inputPartition0, 5L);
    watermarkStates.update((WatermarkMessage) envelope.getMessage(),
        envelope.getSystemStreamPartition());
    assertEquals(watermarkStates.getWatermark(input), 5L);
    assertEquals(watermarkStates.getWatermark(intermediate), WATERMARK_NOT_EXIST);

    // watermark from task 0 on int p0 to 6
    WatermarkMessage watermarkMessage = new WatermarkMessage(6L, "task 0");
    watermarkStates.update(watermarkMessage, intPartition0);
    assertEquals(watermarkStates.getWatermarkPerSSP(intPartition0), WATERMARK_NOT_EXIST);
    assertEquals(watermarkStates.getWatermark(intermediate), WATERMARK_NOT_EXIST);

    // watermark from task 1 on int p0 to 3
    watermarkMessage = new WatermarkMessage(3L, "task 1");
    watermarkStates.update(watermarkMessage, intPartition0);
    assertEquals(watermarkStates.getWatermarkPerSSP(intPartition0), 3L);
    assertEquals(watermarkStates.getWatermark(intermediate), WATERMARK_NOT_EXIST);

    // watermark from task 0 on int p1 to 10
    watermarkMessage = new WatermarkMessage(10L, "task 0");
    watermarkStates.update(watermarkMessage, intPartition1);
    assertEquals(watermarkStates.getWatermarkPerSSP(intPartition1), WATERMARK_NOT_EXIST);
    assertEquals(watermarkStates.getWatermark(intermediate), WATERMARK_NOT_EXIST);

    // watermark from task 1 on int p1 to 4
    watermarkMessage = new WatermarkMessage(4L, "task 1");
    watermarkStates.update(watermarkMessage, intPartition1);
    assertEquals(watermarkStates.getWatermarkPerSSP(intPartition1), 4L);
    // verify we got a watermark 3 (min) for int stream
    assertEquals(watermarkStates.getWatermark(intermediate), 3L);

    // advance watermark from task 1 on int p0 to 8
    watermarkMessage = new WatermarkMessage(8L, "task 1");
    watermarkStates.update(watermarkMessage, intPartition0);
    assertEquals(watermarkStates.getWatermarkPerSSP(intPartition0), 6L);
    // verify we got a watermark 4 (min) for int stream
    assertEquals(watermarkStates.getWatermark(intermediate), 4L);

    // advance watermark from task 1 on int p1 to 7
    watermarkMessage = new WatermarkMessage(7L, "task 1");
    watermarkStates.update(watermarkMessage, intPartition1);
    assertEquals(watermarkStates.getWatermarkPerSSP(intPartition1), 7L);
    // verify we got a watermark 6 (min) for int stream
    assertEquals(watermarkStates.getWatermark(intermediate), 6L);
  }
}
