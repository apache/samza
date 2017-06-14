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
import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.util.IOGraphUtil.IONode;
import org.apache.samza.operators.util.TestIOGraphUtil;
import org.apache.samza.system.EndOfStream;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestEndOfStreamManager {

  @Test
  public void testUpdateFromInputSource() {
    SystemStreamPartition ssp = new SystemStreamPartition("test-system", "test-stream", new Partition(0));
    EndOfStreamManager manager = new EndOfStreamManager("Task 0", 1, Collections.singleton(ssp), null, null);
    IncomingMessageEnvelope envelope = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssp));
    assertEquals(envelope.getSystemStreamPartition(), ssp);
    assertEquals(envelope.getOffset(), IncomingMessageEnvelope.END_OF_STREAM_OFFSET);
    assertTrue(envelope.getMessage() instanceof EndOfStream);
  }

  @Test
  public void testUpdateFromIntermediateStream() {
    SystemStreamPartition[] ssps = new SystemStreamPartition[3];
    ssps[0] = new SystemStreamPartition("test-system", "test-stream-1", new Partition(0));
    ssps[1] = new SystemStreamPartition("test-system", "test-stream-2", new Partition(0));
    ssps[2] = new SystemStreamPartition("test-system", "test-stream-2", new Partition(1));
    EndOfStreamManager manager = new EndOfStreamManager("Task 0", 1, new HashSet<>(Arrays.asList(ssps)), null, null);
    int envelopeCount = 4;
    IncomingMessageEnvelope[] envelopes = new IncomingMessageEnvelope[envelopeCount];
    for (int i = 0; i < envelopeCount; i++) {
      envelopes[i] = new IncomingMessageEnvelope(ssps[0], "dummy-offset", "dummy-key", new EndOfStreamMessage("task " + i, envelopeCount));
    }

    // verify the first three messages won't result in end-of-stream
    for (int i = 0; i < 3; i++) {
      assertNull(manager.update(envelopes[i]));
      assertFalse(manager.isEndOfStream(ssps[0].getSystemStream()));
    }
    // the fourth message will end the stream
    IncomingMessageEnvelope envelope = manager.update(envelopes[3]);
    assertNotNull(envelope);
    assertEquals(envelope.getOffset(), IncomingMessageEnvelope.END_OF_STREAM_OFFSET);
    assertTrue(envelope.getMessage() instanceof EndOfStream);
    assertEquals(((EndOfStream) envelope.getMessage()).get(), ssps[0].getSystemStream());
    assertTrue(manager.isEndOfStream(ssps[0].getSystemStream()));
    assertFalse(manager.isEndOfStream(ssps[1].getSystemStream()));

    // stream2 has two partitions assigned to this task, so it requires a message from each partition to end it
    envelopes = new IncomingMessageEnvelope[envelopeCount];
    for (int i = 0; i < envelopeCount; i++) {
      envelopes[i] = new IncomingMessageEnvelope(ssps[1], "dummy-offset", "dummy-key", new EndOfStreamMessage("task " + i, envelopeCount));
    }
    // verify the messages for the partition 0 won't result in end-of-stream
    for (int i = 0; i < 4; i++) {
      assertNull(manager.update(envelopes[i]));
      assertFalse(manager.isEndOfStream(ssps[1].getSystemStream()));
    }
    for (int i = 0; i < envelopeCount; i++) {
      envelopes[i] = new IncomingMessageEnvelope(ssps[2], "dummy-offset", "dummy-key", new EndOfStreamMessage("task " + i, envelopeCount));
    }
    for (int i = 0; i < 3; i++) {
      assertNull(manager.update(envelopes[i]));
      assertFalse(manager.isEndOfStream(ssps[1].getSystemStream()));
    }
    // the fourth message will end the stream
    envelope = manager.update(envelopes[3]);
    assertNotNull(envelope);
    assertEquals(envelope.getOffset(), IncomingMessageEnvelope.END_OF_STREAM_OFFSET);
    assertTrue(envelope.getMessage() instanceof EndOfStream);
    assertEquals(((EndOfStream) envelope.getMessage()).get(), ssps[1].getSystemStream());
    assertTrue(manager.isEndOfStream(ssps[1].getSystemStream()));
  }

  @Test
  public void testSendEndOfStream() {
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
    EndOfStreamManager manager = new EndOfStreamManager("task 0",
        8,
        Collections.EMPTY_SET,
        Collections.singletonMap(ints.getSystem(), admin),
        collector);

    Set<Integer> partitions = new HashSet<>();
    doAnswer(invocation -> {
        OutgoingMessageEnvelope envelope = (OutgoingMessageEnvelope) invocation.getArguments()[0];
        partitions.add((Integer) envelope.getPartitionKey());
        EndOfStreamMessage eosMessage = (EndOfStreamMessage) envelope.getMessage();
        assertEquals(eosMessage.getTaskName(), "task 0");
        assertEquals(eosMessage.getTaskCount(), 8);
        return null;
      }).when(collector).send(any());

    manager.sendEndOfStream(ints);
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
    Collection<IONode> ioGraph = TestIOGraphUtil.buildSimpleIOGraphOfPartitionBy(inputs, partitionByOp);

    StreamGraphImpl streamGraph = mock(StreamGraphImpl.class);
    when(streamGraph.toIOGraph()).thenReturn(ioGraph);
    EndOfStreamManager.EndOfStreamDispatcher dispatcher = EndOfStreamManager.createDispatcher(streamGraph);

    SystemStream input1 = new SystemStream("test-system", "input-stream-1");
    SystemStream input2 = new SystemStream("test-system", "input-stream-2");
    SystemStream ints = new SystemStream("test-system", "int-stream");
    SystemStreamPartition[] ssps = new SystemStreamPartition[3];
    ssps[0] = new SystemStreamPartition(input1, new Partition(0));
    ssps[1] = new SystemStreamPartition(input2, new Partition(0));
    ssps[2] = new SystemStreamPartition(ints, new Partition(0));

    EndOfStreamManager manager = spy(
        new EndOfStreamManager("task 0", 4, new HashSet<>(Arrays.asList(ssps)), null, null));
    TaskCoordinator coordinator = mock(TaskCoordinator.class);

    // ssp1 end-of-stream, wait for ssp2
    IncomingMessageEnvelope envelope = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssps[0]));
    dispatcher.propagate((EndOfStream) envelope.getMessage(), coordinator);
    verify(manager, never()).sendEndOfStream(any());

    // ssp2 end-of-stream, propagate to intermediate
    envelope = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssps[1]));
    doNothing().when(manager).sendEndOfStream(any());
    dispatcher.propagate((EndOfStream) envelope.getMessage(), coordinator);
    ArgumentCaptor<SystemStream> argument = ArgumentCaptor.forClass(SystemStream.class);
    verify(manager).sendEndOfStream(argument.capture());
    assertEquals(ints, argument.getValue());

    // intermediate end-of-stream, shutdown the task
    envelope = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssps[2]));
    doNothing().when(coordinator).shutdown(any());
    dispatcher.propagate((EndOfStream) envelope.getMessage(), coordinator);
    ArgumentCaptor<TaskCoordinator.RequestScope> arg = ArgumentCaptor.forClass(TaskCoordinator.RequestScope.class);
    verify(coordinator).shutdown(arg.capture());
    assertEquals(TaskCoordinator.RequestScope.CURRENT_TASK, arg.getValue());
  }
}
