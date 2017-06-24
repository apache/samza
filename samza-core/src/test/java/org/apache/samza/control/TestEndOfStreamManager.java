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
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.util.IOGraphUtil.IONode;
import org.apache.samza.operators.util.TestIOGraphUtil;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.StreamSpec;
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
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
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
    TaskName taskName = new TaskName("Task 0");
    Multimap<SystemStream, String> streamToTasks = HashMultimap.create();
    streamToTasks.put(ssp.getSystemStream(), taskName.getTaskName());
    EndOfStreamManager manager = new EndOfStreamManager("Task 0", streamToTasks, Collections.singleton(ssp), null, null);
    EndOfStream eos = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssp));
    assertEquals(eos.getSystemStream(), ssp.getSystemStream());
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
    EndOfStreamManager manager = new EndOfStreamManager("Task 0", streamToTasks, new HashSet<>(Arrays.asList(ssps)), null, null);
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
    EndOfStream eos = manager.update(envelopes[3]);
    assertNotNull(eos);
    assertEquals(eos.getSystemStream(), ssps[0].getSystemStream());
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
    eos = manager.update(envelopes[3]);
    assertNotNull(eos);
    assertEquals(eos.getSystemStream(), ssps[1].getSystemStream());
    assertTrue(manager.isEndOfStream(ssps[1].getSystemStream()));
  }

  @Test
  public void testSendEndOfStream() {
    SystemStream ints = new SystemStream("test-system", "int-stream");
    SystemStreamMetadata metadata = mock(SystemStreamMetadata.class);
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put(new Partition(0), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(1), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(2), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    partitionMetadata.put(new Partition(3), mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class));
    when(metadata.getSystemStreamPartitionMetadata()).thenReturn(partitionMetadata);
    StreamMetadataCache metadataCache = mock(StreamMetadataCache.class);
    when(metadataCache.getSystemStreamMetadata(anyObject(), anyBoolean())).thenReturn(metadata);

    MessageCollector collector = mock(MessageCollector.class);

    TaskName taskName = new TaskName("Task 0");
    EndOfStreamManager manager = new EndOfStreamManager("task 0",
        HashMultimap.create(),
        Collections.EMPTY_SET,
        metadataCache,
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

    manager.sendEndOfStream(ints, 8);
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
    EndOfStreamDispatcher dispatcher = new EndOfStreamDispatcher(ioGraph);

    SystemStream input1 = new SystemStream("test-system", "input-stream-1");
    SystemStream input2 = new SystemStream("test-system", "input-stream-2");
    SystemStream ints = new SystemStream("test-system", "int-stream");
    SystemStreamPartition[] ssps = new SystemStreamPartition[3];
    ssps[0] = new SystemStreamPartition(input1, new Partition(0));
    ssps[1] = new SystemStreamPartition(input2, new Partition(0));
    ssps[2] = new SystemStreamPartition(ints, new Partition(0));

    Set<SystemStreamPartition> sspSet = new HashSet<>(Arrays.asList(ssps));
    TaskName taskName = new TaskName("task 0");
    Multimap<SystemStream, String> streamToTasks = HashMultimap.create();
    for (SystemStreamPartition ssp : ssps) {
      streamToTasks.put(ssp.getSystemStream(), taskName.getTaskName());
    }
    EndOfStreamManager manager = spy(
        new EndOfStreamManager("task 0", streamToTasks, sspSet, null, null));
    TaskCoordinator coordinator = mock(TaskCoordinator.class);

    // ssp1 end-of-stream, wait for ssp2
    EndOfStream eos = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssps[0]));
    dispatcher.propagate(eos, coordinator);
    verify(manager, never()).sendEndOfStream(any(), anyInt());

    // ssp2 end-of-stream, propagate to intermediate
    eos = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssps[1]));
    doNothing().when(manager).sendEndOfStream(any(), anyInt());
    dispatcher.propagate(eos, coordinator);
    ArgumentCaptor<SystemStream> argument = ArgumentCaptor.forClass(SystemStream.class);
    verify(manager).sendEndOfStream(argument.capture(), anyInt());
    assertEquals(ints, argument.getValue());

    // intermediate end-of-stream, shutdown the task
    eos = manager.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssps[2]));
    doNothing().when(coordinator).shutdown(any());
    dispatcher.propagate(eos, coordinator);
    ArgumentCaptor<TaskCoordinator.RequestScope> arg = ArgumentCaptor.forClass(TaskCoordinator.RequestScope.class);
    verify(coordinator).shutdown(arg.capture());
    assertEquals(TaskCoordinator.RequestScope.CURRENT_TASK, arg.getValue());
  }

  /**
   * Test the case when the publishing tasks to intermediate stream is a subset of total tasks
   */
  @Test
  public void testDispatcherForTaskCount() {
    StreamSpec outputSpec = new StreamSpec("int-stream", "int-stream", "test-system");
    OutputStreamImpl outputStream = new OutputStreamImpl(outputSpec, null, null);
    OutputOperatorSpec partitionByOp = OperatorSpecs.createPartitionByOperatorSpec(outputStream, 0);

    List<StreamSpec> inputs = new ArrayList<>();
    inputs.add(new StreamSpec("input-stream-1", "input-stream-1", "test-system"));
    Collection<IONode> ioGraph = TestIOGraphUtil.buildSimpleIOGraphOfPartitionBy(inputs, partitionByOp);
    EndOfStreamDispatcher dispatcher = new EndOfStreamDispatcher(ioGraph);

    SystemStream input1 = new SystemStream("test-system", "input-stream-1");
    SystemStream ints = new SystemStream("test-system", "int-stream");
    SystemStreamPartition ssp1 = new SystemStreamPartition(input1, new Partition(0));
    SystemStreamPartition ssp2 = new SystemStreamPartition(ints, new Partition(0));

    Map<TaskName, TaskModel> taskModels = new HashMap<>();
    TaskName t0 = new TaskName("task 0");
    taskModels.put(t0, new TaskModel(t0, Collections.singleton(ssp1), null));
    TaskName t1 = new TaskName("task 1");
    taskModels.put(t1, new TaskModel(t1, Collections.singleton(ssp2), null));


    Multimap<SystemStream, String> streamToTasks = HashMultimap.create();
    streamToTasks.put(ssp1.getSystemStream(), t0.getTaskName());
    streamToTasks.put(ssp2.getSystemStream(), t1.getTaskName());

    EndOfStreamManager manager0 = spy(
        new EndOfStreamManager(t0.getTaskName(), streamToTasks, Collections.singleton(ssp1), null, null));
    EndOfStreamManager manager1 = spy(
        new EndOfStreamManager(t1.getTaskName(), streamToTasks, Collections.singleton(ssp2), null, null));

    TaskCoordinator coordinator1 = mock(TaskCoordinator.class);
    TaskCoordinator coordinator2 = mock(TaskCoordinator.class);

    // ssp1 end-of-stream
    EndOfStream eos = manager0.update(EndOfStreamManager.buildEndOfStreamEnvelope(ssp1));
    doNothing().when(manager0).sendEndOfStream(any(), anyInt());
    doNothing().when(coordinator1).shutdown(any());
    dispatcher.propagate(eos, coordinator1);
    ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(Integer.class);
    verify(manager0).sendEndOfStream(any(), argument.capture());
    //verify task count is 1
    assertTrue(argument.getValue() == 1);
    ArgumentCaptor<TaskCoordinator.RequestScope> arg = ArgumentCaptor.forClass(TaskCoordinator.RequestScope.class);
    verify(coordinator1).shutdown(arg.capture());
    assertEquals(TaskCoordinator.RequestScope.CURRENT_TASK, arg.getValue());

    // int1 end-of-stream
    IncomingMessageEnvelope intEos = new IncomingMessageEnvelope(ssp2, null, null, new EndOfStreamMessage(t0.getTaskName(), 1));
    eos = manager1.update(intEos);
    doNothing().when(coordinator2).shutdown(any());
    dispatcher.propagate(eos, coordinator2);
    verify(manager1, never()).sendEndOfStream(any(), anyInt());
    arg = ArgumentCaptor.forClass(TaskCoordinator.RequestScope.class);
    verify(coordinator2).shutdown(arg.capture());
    assertEquals(TaskCoordinator.RequestScope.CURRENT_TASK, arg.getValue());
  }
}
