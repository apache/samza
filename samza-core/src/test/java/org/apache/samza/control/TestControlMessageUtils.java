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

package org.apache.samza.control;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.message.ControlMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.task.MessageCollector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestControlMessageUtils {

  @Test
  public void testSendControlMessage() {
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

    ControlMessageUtils.sendControlMessage(mock(ControlMessage.class), systemStream, metadataCache, collector);
    assertEquals(partitions.size(), 4);
  }

  @Test
  public void testCalculateUpstreamTaskCounts() {
    SystemStream input1 = new SystemStream("test-system", "input-stream-1");
    SystemStream input2 = new SystemStream("test-system", "input-stream-2");
    SystemStream input3 = new SystemStream("test-system", "input-stream-3");

    Multimap<SystemStream, String> inputToTasks = HashMultimap.create();
    TaskName t0 = new TaskName("task 0"); //consume input1 and input2
    TaskName t1 = new TaskName("task 1"); //consume input1 and input2 and input 3
    TaskName t2 = new TaskName("task 2"); //consume input2 and input 3
    inputToTasks.put(input1, t0.getTaskName());
    inputToTasks.put(input1, t1.getTaskName());
    inputToTasks.put(input2, t0.getTaskName());
    inputToTasks.put(input2, t1.getTaskName());
    inputToTasks.put(input2, t2.getTaskName());
    inputToTasks.put(input3, t1.getTaskName());
    inputToTasks.put(input3, t2.getTaskName());

    StreamSpec inputSpec2 = new StreamSpec("input-stream-2", "input-stream-2", "test-system");
    StreamSpec inputSpec3 = new StreamSpec("input-stream-3", "input-stream-3", "test-system");
    StreamSpec intSpec1 = new StreamSpec("int-stream-1", "int-stream-1", "test-system");
    StreamSpec intSpec2 = new StreamSpec("int-stream-2", "int-stream-2", "test-system");

    List<IOGraph.IONode> nodes = new ArrayList<>();
    IOGraph.IONode node = new IOGraph.IONode(intSpec1, true);
    node.addInput(inputSpec2);
    nodes.add(node);
    node = new IOGraph.IONode(intSpec2, true);
    node.addInput(inputSpec3);
    nodes.add(node);
    IOGraph ioGraph = new IOGraph(nodes);

    Map<SystemStream, Integer> counts = ControlMessageUtils.calculateUpstreamTaskCounts(inputToTasks, ioGraph);
    assertEquals(counts.get(intSpec1.toSystemStream()).intValue(), 3);
    assertEquals(counts.get(intSpec2.toSystemStream()).intValue(), 2);
  }

}