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
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestEndOfStreamStates {

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

    EndOfStreamStates endOfStreamStates = new EndOfStreamStates(ssps, producerCounts);
    assertFalse(endOfStreamStates.isEndOfStream(input));
    assertFalse(endOfStreamStates.isEndOfStream(intermediate));
    assertFalse(endOfStreamStates.allEndOfStream());

    IncomingMessageEnvelope envelope = IncomingMessageEnvelope.buildEndOfStreamEnvelope(inputPartition0);
    endOfStreamStates.update((EndOfStreamMessage) envelope.getMessage(), envelope.getSystemStreamPartition());
    assertTrue(endOfStreamStates.isEndOfStream(input));
    assertFalse(endOfStreamStates.isEndOfStream(intermediate));
    assertFalse(endOfStreamStates.allEndOfStream());

    EndOfStreamMessage eos = new EndOfStreamMessage("task 0");
    endOfStreamStates.update(eos, intPartition0);
    endOfStreamStates.update(eos, intPartition1);
    assertFalse(endOfStreamStates.isEndOfStream(intermediate));
    assertFalse(endOfStreamStates.allEndOfStream());

    eos = new EndOfStreamMessage("task 1");
    endOfStreamStates.update(eos, intPartition0);
    endOfStreamStates.update(eos, intPartition1);
    assertTrue(endOfStreamStates.isEndOfStream(intermediate));
    assertTrue(endOfStreamStates.allEndOfStream());
  }
}
