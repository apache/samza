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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.message.WatermarkMessage;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.util.IOGraphUtil;
import org.apache.samza.operators.util.TestIOGraphUtil;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.Watermark;
import org.apache.samza.task.MessageCollector;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestWatermarkManager {

  @Test
  public void testUpdateFromInputSource() {
    SystemStreamPartition ssp = new SystemStreamPartition("test-system", "test-stream", new Partition(0));
    TaskName taskName = new TaskName("Task 0");
    Multimap<SystemStream, String> streamToTasks = HashMultimap.create();
    streamToTasks.put(ssp.getSystemStream(), taskName.getTaskName());
    WatermarkManager manager = new WatermarkManager("Task 0", streamToTasks, Collections.singleton(ssp), null, null);
    long time = System.currentTimeMillis();
    IncomingMessageEnvelope envelope = manager.update(WatermarkManager.buildWatermarkEnvelope(time, ssp));
    assertEquals(envelope.getSystemStreamPartition(), ssp);
    Watermark watermark = (Watermark) envelope.getMessage();
    assertEquals(watermark.getTimestamp(), time);
  }

  @Test
  public void testUpdateFromIntermediateStream() {
    SystemStreamPartition[] ssps = new SystemStreamPartition[3];
    ssps[0] = new SystemStreamPartition("test-system", "test-stream-1", new Partition(0));
    ssps[1] = new SystemStreamPartition("test-system", "test-stream-2", new Partition(0));
    ssps[2] = new SystemStreamPartition("test-system", "test-stream-2", new Partition(1));

    TaskName taskName = new TaskName("Task 0");
    Multimap<SystemStream, String> streamToTasks = HashMultimap.create();
    for (SystemStreamPartition ssp : ssps) {
      streamToTasks.put(ssp.getSystemStream(), taskName.getTaskName());
    }

    WatermarkManager manager = new WatermarkManager("Task 0", streamToTasks, new HashSet<>(Arrays.asList(ssps)), null, null);
    int envelopeCount = 4;
    IncomingMessageEnvelope[] envelopes = new IncomingMessageEnvelope[envelopeCount];

    long[] time = {300L, 200L, 100L, 400L};
    for (int i = 0; i < envelopeCount; i++) {
      envelopes[i] = new IncomingMessageEnvelope(ssps[0], "dummy-offset", "", new WatermarkMessage(time[i], "task " + i, envelopeCount));
    }
    for (int i = 0; i < 3; i++) {
      assertNull(manager.update(envelopes[i]));
    }
    // verify the first three messages won't result in end-of-stream
    assertEquals(manager.getWatermarkTime(ssps[0]), WatermarkManager.WATERMARK_NOT_EXIST);
    // the fourth message will generate a watermark
    IncomingMessageEnvelope envelope = manager.update(envelopes[3]);
    assertNotNull(envelope);
    assertTrue(envelope.getMessage() instanceof Watermark);
    assertEquals(((Watermark) envelope.getMessage()).getTimestamp(), 100);
    assertEquals(manager.getWatermarkTime(ssps[1]), WatermarkManager.WATERMARK_NOT_EXIST);
    assertEquals(manager.getWatermarkTime(ssps[2]), WatermarkManager.WATERMARK_NOT_EXIST);


    // stream2 has two partitions assigned to this task, so it requires a message from each partition to calculate watermarks
    long[] time1 = {300L, 200L, 100L, 400L};
    envelopes = new IncomingMessageEnvelope[envelopeCount];
    for (int i = 0; i < envelopeCount; i++) {
      envelopes[i] = new IncomingMessageEnvelope(ssps[1], "dummy-offset", "", new WatermarkMessage(time1[i], "task " + i, envelopeCount));
    }
    // verify the messages for the partition 0 won't generate watermark
    for (int i = 0; i < 4; i++) {
      assertNull(manager.update(envelopes[i]));
    }
    assertEquals(manager.getWatermarkTime(ssps[1]), 100L);

    long[] time2 = {350L, 150L, 500L, 80L};
    for (int i = 0; i < envelopeCount; i++) {
      envelopes[i] = new IncomingMessageEnvelope(ssps[2], "dummy-offset", "", new WatermarkMessage(time2[i], "task " + i, envelopeCount));
    }
    for (int i = 0; i < 3; i++) {
      assertNull(manager.update(envelopes[i]));
    }
    assertEquals(manager.getWatermarkTime(ssps[2]), WatermarkManager.WATERMARK_NOT_EXIST);
    // the fourth message will generate the watermark
    envelope = manager.update(envelopes[3]);
    assertNotNull(envelope);
    assertEquals(manager.getWatermarkTime(ssps[2]), 80L);
    assertTrue(envelope.getMessage() instanceof Watermark);
    assertEquals(((Watermark) envelope.getMessage()).getTimestamp(), 80L);
  }

  @Test
  public void testSendWatermark() {
    SystemStream ints = new SystemStream("test-system", "int-stream");
    SystemAdmin admin = mock(SystemAdmin.class);
    SystemStreamMetadata metadata = mock(SystemStreamMetadata.class);
    when(admin.getSystemStreamMetadata(any())).thenReturn(Collections.singletonMap("int-stream", metadata));
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put(new Partition(0), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(1), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(2), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(3), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    when(metadata.getSystemStreamPartitionMetadata()).thenReturn(partitionMetadata);
    MessageCollector collector = mock(MessageCollector.class);

    WatermarkManager manager = new WatermarkManager("task 0",
        HashMultimap.create(),
        Collections.EMPTY_SET,
        Collections.singletonMap(ints.getSystem(), admin),
        collector);

    long time = System.currentTimeMillis();
    Set<Integer> partitions = new HashSet<>();
    doAnswer(invocation -> {
        OutgoingMessageEnvelope envelope = (OutgoingMessageEnvelope) invocation.getArguments()[0];
        partitions.add((Integer) envelope.getPartitionKey());
        WatermarkMessage watermarkMessage = (WatermarkMessage) envelope.getMessage();
        assertEquals(watermarkMessage.getTaskName(), "task 0");
        assertEquals(watermarkMessage.getTaskCount(), 8);
        assertEquals(watermarkMessage.getTimestamp(), time);
        return null;
      }).when(collector).send(any());

    manager.sendWatermark(time, ints, 8);
    assertEquals(partitions.size(), 4);
  }

  @Test
  public void testDispatcher() {
    StreamSpec outputSpec = new StreamSpec("int-stream", "int-stream", "test-system");
    OutputStreamImpl outputStream = new OutputStreamImpl(outputSpec, null, null);
    OutputOperatorSpec partitionByOp = OperatorSpecs.createPartitionByOperatorSpec(outputStream, 0);

    List<StreamSpec> inputs = new ArrayList<>();
    inputs.add(new StreamSpec("input-stream-1", "input-stream-1", "test-system"));
    inputs.add(new StreamSpec("input-stream-2", "input-stream-2", "test-system"));
    Collection<IOGraphUtil.IONode> ioGraph = TestIOGraphUtil.buildSimpleIOGraphOfPartitionBy(inputs, partitionByOp);

    StreamGraphImpl streamGraph = mock(StreamGraphImpl.class);
    when(streamGraph.toIOGraph()).thenReturn(ioGraph);
    WatermarkManager.WatermarkDispatcher dispatcher = WatermarkManager.createDispatcher(streamGraph);

    SystemStream input1 = new SystemStream("test-system", "input-stream-1");
    SystemStream input2 = new SystemStream("test-system", "input-stream-2");
    SystemStream ints = new SystemStream("test-system", "int-stream");
    SystemStreamPartition[] ssps0 = new SystemStreamPartition[3];
    ssps0[0] = new SystemStreamPartition(input1, new Partition(0));
    ssps0[1] = new SystemStreamPartition(input2, new Partition(0));
    ssps0[2] = new SystemStreamPartition(ints, new Partition(0));

    SystemStreamPartition[] ssps1 = new SystemStreamPartition[3];
    ssps1[0] = new SystemStreamPartition(input1, new Partition(1));
    ssps1[1] = new SystemStreamPartition(input2, new Partition(1));
    ssps1[2] = new SystemStreamPartition(ints, new Partition(1));

    TaskName t0 = new TaskName("task 0");
    TaskName t1 = new TaskName("task 1");
    Multimap<SystemStream, String> streamToTasks = HashMultimap.create();
    for (SystemStreamPartition ssp : ssps0) {
      streamToTasks.put(ssp.getSystemStream(), t0.getTaskName());
    }
    for (SystemStreamPartition ssp : ssps1) {
      streamToTasks.put(ssp.getSystemStream(), t1.getTaskName());
    }
    WatermarkManager manager = spy(new WatermarkManager(t0.getTaskName(), streamToTasks, new HashSet<>(Arrays.asList(ssps0)), null, null));
    long time = System.currentTimeMillis();
    Watermark watermark = manager.createWatermark(time);
    doNothing().when(manager).sendWatermark(anyLong(), any(), anyInt());
    dispatcher.propagate(watermark, ints);
    ArgumentCaptor<Long> arg1 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<SystemStream> arg2 = ArgumentCaptor.forClass(SystemStream.class);
    ArgumentCaptor<Integer> arg3 = ArgumentCaptor.forClass(Integer.class);
    verify(manager).sendWatermark(arg1.capture(), arg2.capture(), arg3.capture());
    assertEquals(arg1.getValue().longValue(), time);
    assertEquals(arg2.getValue(), ints);
    assertEquals(arg3.getValue().intValue(), 2);
  }
}
