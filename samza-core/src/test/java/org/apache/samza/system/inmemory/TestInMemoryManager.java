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
package org.apache.samza.system.inmemory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestInMemoryManager {
  private static final String SYSTEM = "system";
  private static final String STREAM0 = "stream0";
  private static final String STREAM1 = "stream1";

  private InMemoryManager inMemoryManager;

  @Before
  public void setup() {
    this.inMemoryManager = new InMemoryManager();
  }

  @Test
  public void testGetSystemStreamMetadata() {
    this.inMemoryManager.initializeStream(new StreamSpec(STREAM0, STREAM0, SYSTEM, 1));
    this.inMemoryManager.initializeStream(new StreamSpec(STREAM1, STREAM1, SYSTEM, 1));
    // add some other stream which we won't request metadata for
    this.inMemoryManager.initializeStream(new StreamSpec("otherStream", "otherStream", SYSTEM, 1));

    // empty stream
    SystemStreamMetadata systemStreamMetadata0 = new SystemStreamMetadata(STREAM0,
        ImmutableMap.of(new Partition(0), new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, "0")));
    assertEquals(ImmutableMap.of(STREAM0, systemStreamMetadata0),
        this.inMemoryManager.getSystemStreamMetadata(SYSTEM, ImmutableSet.of(STREAM0)));

    // add a message in
    SystemStreamPartition ssp0 = new SystemStreamPartition(SYSTEM, STREAM0, new Partition(0));
    this.inMemoryManager.put(ssp0, "key00", "message00");
    systemStreamMetadata0 = new SystemStreamMetadata(STREAM0,
        ImmutableMap.of(new Partition(0), new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "0", "1")));
    assertEquals(ImmutableMap.of(STREAM0, systemStreamMetadata0),
        this.inMemoryManager.getSystemStreamMetadata(SYSTEM, ImmutableSet.of(STREAM0)));

    // add a second message to the first stream and add one message to the second stream
    this.inMemoryManager.put(ssp0, "key01", "message01");
    SystemStreamPartition ssp1 = new SystemStreamPartition(SYSTEM, STREAM1, new Partition(0));
    this.inMemoryManager.put(ssp1, "key10", "message10");
    systemStreamMetadata0 = new SystemStreamMetadata(STREAM0,
        ImmutableMap.of(new Partition(0), new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "1", "2")));
    SystemStreamMetadata systemStreamMetadata1 = new SystemStreamMetadata(STREAM1,
        ImmutableMap.of(new Partition(0), new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "0", "1")));
    // also test a batch call for multiple streams here
    assertEquals(ImmutableMap.of(STREAM0, systemStreamMetadata0, STREAM1, systemStreamMetadata1),
        this.inMemoryManager.getSystemStreamMetadata(SYSTEM, ImmutableSet.of(STREAM0, STREAM1)));
  }

  @Test
  public void testPoll() {
    this.inMemoryManager.initializeStream(new StreamSpec(STREAM0, STREAM0, SYSTEM, 1));
    this.inMemoryManager.initializeStream(new StreamSpec(STREAM1, STREAM1, SYSTEM, 1));
    // add some other stream which we won't request metadata for
    this.inMemoryManager.initializeStream(new StreamSpec("otherStream", "otherStream", SYSTEM, 1));

    // empty stream
    SystemStreamPartition ssp0 = new SystemStreamPartition(SYSTEM, STREAM0, new Partition(0));
    assertEquals(ImmutableMap.of(ssp0, ImmutableList.of()),
        this.inMemoryManager.poll(Collections.singletonMap(ssp0, "0")));

    // add a message in
    this.inMemoryManager.put(ssp0, "key00", "message00");
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> polledMessages =
        this.inMemoryManager.poll(Collections.singletonMap(ssp0, "0"));
    assertEquals(1, polledMessages.get(ssp0).size());
    assertIncomingMessageEnvelope("key00", "message00", "0", ssp0, polledMessages.get(ssp0).get(0));

    // add a second message to the first stream
    this.inMemoryManager.put(ssp0, "key01", "message01");
    // verify multiple messages returned
    polledMessages = this.inMemoryManager.poll(ImmutableMap.of(ssp0, "0"));
    assertEquals(2, polledMessages.get(ssp0).size());
    assertIncomingMessageEnvelope("key00", "message00", "0", ssp0, polledMessages.get(ssp0).get(0));
    assertIncomingMessageEnvelope("key01", "message01", "1", ssp0, polledMessages.get(ssp0).get(1));
    // make sure only read messages starting from the offset that is not the oldest offset
    polledMessages = this.inMemoryManager.poll(ImmutableMap.of(ssp0, "1"));
    assertEquals(1, polledMessages.get(ssp0).size());
    assertIncomingMessageEnvelope("key01", "message01", "1", ssp0, polledMessages.get(ssp0).get(0));

    // add a message to the second stream to test a batch call
    SystemStreamPartition ssp1 = new SystemStreamPartition(SYSTEM, STREAM1, new Partition(0));
    this.inMemoryManager.put(ssp1, "key10", "message10");
    polledMessages = this.inMemoryManager.poll(ImmutableMap.of(ssp0, "1", ssp1, "0"));
    assertEquals(1, polledMessages.get(ssp0).size());
    assertIncomingMessageEnvelope("key01", "message01", "1", ssp0, polledMessages.get(ssp0).get(0));
    assertEquals(1, polledMessages.get(ssp1).size());
    assertIncomingMessageEnvelope("key10", "message10", "0", ssp1, polledMessages.get(ssp1).get(0));
  }

  private static void assertIncomingMessageEnvelope(String expectedKey, String expectedMessage, String expectedOffset,
      SystemStreamPartition expectedSystemStreamPartition, IncomingMessageEnvelope actualIncomingMessageEnvelope) {
    assertEquals(expectedKey, actualIncomingMessageEnvelope.getKey());
    assertEquals(expectedMessage, actualIncomingMessageEnvelope.getMessage());
    assertEquals(expectedOffset, actualIncomingMessageEnvelope.getOffset());
    assertEquals(expectedSystemStreamPartition, actualIncomingMessageEnvelope.getSystemStreamPartition());
  }
}