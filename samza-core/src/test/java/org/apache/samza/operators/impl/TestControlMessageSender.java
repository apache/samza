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
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.task.MessageCollector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestControlMessageSender {

  @Test
  public void testSend() {
    SystemStreamMetadata metadata = mock(SystemStreamMetadata.class);
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put(new Partition(0), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(1), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(2), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(3), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    when(metadata.getSystemStreamPartitionMetadata()).thenReturn(partitionMetadata);
    StreamMetadataCache metadataCache = mock(StreamMetadataCache.class);
    when(metadataCache.getSystemStreamMetadata(anyObject(), anyBoolean())).thenReturn(metadata);

    SystemStream systemStream = new SystemStream("test-system", "test-stream");
    Set<Integer> partitions = new HashSet<>();
    MessageCollector collector = mock(MessageCollector.class);
    doAnswer(invocation -> {
        OutgoingMessageEnvelope envelope = (OutgoingMessageEnvelope) invocation.getArguments()[0];
        partitions.add((Integer) envelope.getPartitionKey());
        assertEquals(envelope.getSystemStream(), systemStream);
        return null;
      }).when(collector).send(any());

    ControlMessageSender sender = new ControlMessageSender(metadataCache);
    WatermarkMessage watermark = new WatermarkMessage(System.currentTimeMillis(), "task 0");
    sender.send(watermark, systemStream, collector);
    assertEquals(partitions.size(), 1);
  }

  @Test
  public void testBroadcast() {
    SystemStreamMetadata metadata = mock(SystemStreamMetadata.class);
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put(new Partition(0), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(1), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(2), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(3), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    when(metadata.getSystemStreamPartitionMetadata()).thenReturn(partitionMetadata);
    StreamMetadataCache metadataCache = mock(StreamMetadataCache.class);
    when(metadataCache.getSystemStreamMetadata(anyObject(), anyBoolean())).thenReturn(metadata);

    SystemStream systemStream = new SystemStream("test-system", "test-stream");
    Set<Integer> partitions = new HashSet<>();
    MessageCollector collector = mock(MessageCollector.class);
    doAnswer(invocation -> {
        OutgoingMessageEnvelope envelope = (OutgoingMessageEnvelope) invocation.getArguments()[0];
        partitions.add((Integer) envelope.getPartitionKey());
        assertEquals(envelope.getSystemStream(), systemStream);
        return null;
      }).when(collector).send(any());

    ControlMessageSender sender = new ControlMessageSender(metadataCache);
    WatermarkMessage watermark = new WatermarkMessage(System.currentTimeMillis(), "task 0");
    SystemStreamPartition ssp = new SystemStreamPartition(systemStream, new Partition(0));
    sender.broadcast(watermark, ssp, collector);
    assertEquals(partitions.size(), 3);
  }
}
