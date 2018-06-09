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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.StreamUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestOperatorImplGraphUtil {
  @Test
  public void testGetStreamToConsumerTasks() {
    String system = "test-system";
    String streamId0 = "test-stream-0";
    String streamId1 = "test-stream-1";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "test-app");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");

    Config streamConfigs = StreamUtil.toStreamConfigs(ImmutableList.of(
        ImmutableTriple.of(streamId0, system, streamId0),
        ImmutableTriple.of(streamId1, system, streamId1)
    ));
    configs.putAll(streamConfigs);
    Config config = new MapConfig(configs);

    SystemStreamPartition ssp0 = new SystemStreamPartition(system, streamId0, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(system, streamId0, new Partition(1));
    SystemStreamPartition ssp2 = new SystemStreamPartition(system, streamId1, new Partition(0));

    TaskName task0 = new TaskName("Task 0");
    TaskName task1 = new TaskName("Task 1");
    Set<SystemStreamPartition> ssps = new HashSet<>();
    ssps.add(ssp0);
    ssps.add(ssp2);
    TaskModel tm0 = new TaskModel(task0, ssps, new Partition(0));
    ContainerModel cm0 = new ContainerModel("c0", 0, Collections.singletonMap(task0, tm0));
    TaskModel tm1 = new TaskModel(task1, Collections.singleton(ssp1), new Partition(1));
    ContainerModel cm1 = new ContainerModel("c1", 1, Collections.singletonMap(task1, tm1));

    Map<String, ContainerModel> cms = new HashMap<>();
    cms.put(cm0.getProcessorId(), cm0);
    cms.put(cm1.getProcessorId(), cm1);

    JobModel jobModel = new JobModel(config, cms, null);
    OperatorImplGraph.OperatorImplGraphUtil util = new OperatorImplGraph.OperatorImplGraphUtil(config);
    Multimap<SystemStream, String> streamToTasks = util.getStreamToConsumerTasks(jobModel);
    assertEquals(streamToTasks.get(ssp0.getSystemStream()).size(), 2);
    assertEquals(streamToTasks.get(ssp2.getSystemStream()).size(), 1);
  }

  @Test
  public void testGetOutputToInputStreams() {
    String inputStreamId1 = "input1";
    String inputStreamId2 = "input2";
    String inputStreamId3 = "input3";
    String inputSystem = "input-system";

    String outputStreamId1 = "output1";
    String outputStreamId2 = "output2";
    String outputSystem = "output-system";

    String intStreamId1 = "test-app-1-partition_by-p1";
    String intStreamId2 = "test-app-1-partition_by-p2";
    String intSystem = "test-system";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "test-app");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), intSystem);

    Config streamConfigs = StreamUtil.toStreamConfigs(ImmutableList.of(
        ImmutableTriple.of(inputStreamId1, inputSystem, inputStreamId1),
        ImmutableTriple.of(inputStreamId2, inputSystem, inputStreamId2),
        ImmutableTriple.of(inputStreamId3, inputSystem, inputStreamId3),
        ImmutableTriple.of(outputStreamId1, outputSystem, outputStreamId1),
        ImmutableTriple.of(outputStreamId2, outputSystem, outputStreamId2)
    ));
    configs.putAll(streamConfigs);
    Config config = new MapConfig(configs);

    StreamGraphSpec graphSpec = new StreamGraphSpec(config);
    MessageStream messageStream1 = graphSpec.getInputStream(inputStreamId1).map(m -> m);
    MessageStream messageStream2 = graphSpec.getInputStream(inputStreamId2).filter(m -> true);
    MessageStream messageStream3 =
        graphSpec.getInputStream(inputStreamId3)
            .filter(m -> true)
            .partitionBy(m -> "m", m -> m, "p1")
            .map(m -> m);
    OutputStream<Object> outputStream1 = graphSpec.getOutputStream(outputStreamId1);
    OutputStream<Object> outputStream2 = graphSpec.getOutputStream(outputStreamId2);

    messageStream1
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(2), "j1")
        .partitionBy(m -> "m", m -> m, "p2")
        .sendTo(outputStream1);
    messageStream3
        .join(messageStream2, mock(JoinFunction.class),
            mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
        .sendTo(outputStream2);

    OperatorImplGraph.OperatorImplGraphUtil util = new OperatorImplGraph.OperatorImplGraphUtil(config);
    Multimap<SystemStream, SystemStream> outputToInput =
        util.getIntermediateToInputStreamsMap(graphSpec.getOperatorSpecGraph());
    Collection<SystemStream> inputs = outputToInput.get(new SystemStream(intSystem, intStreamId2));
    assertEquals(inputs.size(), 2);
    assertTrue(inputs.contains(new SystemStream(inputSystem, inputStreamId1)));
    assertTrue(inputs.contains(new SystemStream(inputSystem, inputStreamId2)));

    inputs = outputToInput.get(new SystemStream(intSystem, intStreamId1));
    assertEquals(inputs.size(), 1);
    assertEquals(inputs.iterator().next(), new SystemStream(inputSystem, inputStreamId3));
  }

  @Test
  public void testGetProducerTaskCountForIntermediateStreams() {
    String inputStreamId1 = "input1";
    String inputStreamId2 = "input2";
    String inputStreamId3 = "input3";
    String inputSystem1 = "system1";
    String inputSystem2 = "system2";

    HashMap<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "test-app");
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), inputSystem1);

    Config streamConfigs = StreamUtil.toStreamConfigs(ImmutableList.of(
        ImmutableTriple.of(inputStreamId1, inputSystem1, inputStreamId1),
        ImmutableTriple.of(inputStreamId2, inputSystem2, inputStreamId2),
        ImmutableTriple.of(inputStreamId3, inputSystem2, inputStreamId3)
    ));
    configs.putAll(streamConfigs);
    Config config = new MapConfig(configs);

    SystemStream input1 = new SystemStream("system1", "intput1");
    SystemStream input2 = new SystemStream("system2", "intput2");
    SystemStream input3 = new SystemStream("system2", "intput3");

    SystemStream int1 = new SystemStream("system1", "int1");
    SystemStream int2 = new SystemStream("system1", "int2");


    /**
     * the task assignment looks like the following:
     *
     * input1 -----> task0, task1 -----> int1
     *                                    ^
     * input2 ------> task1, task2--------|
     *                                    v
     * input3 ------> task1 -----------> int2
     *
     */
    String task0 = "Task 0";
    String task1 = "Task 1";
    String task2 = "Task 2";

    Multimap<SystemStream, String> streamToConsumerTasks = HashMultimap.create();
    streamToConsumerTasks.put(input1, task0);
    streamToConsumerTasks.put(input1, task1);
    streamToConsumerTasks.put(input2, task1);
    streamToConsumerTasks.put(input2, task2);
    streamToConsumerTasks.put(input3, task1);
    streamToConsumerTasks.put(int1, task0);
    streamToConsumerTasks.put(int1, task1);
    streamToConsumerTasks.put(int2, task0);

    Multimap<SystemStream, SystemStream> intermediateToInputStreams = HashMultimap.create();
    intermediateToInputStreams.put(int1, input1);
    intermediateToInputStreams.put(int1, input2);

    intermediateToInputStreams.put(int2, input2);
    intermediateToInputStreams.put(int2, input3);

    OperatorImplGraph.OperatorImplGraphUtil util = new OperatorImplGraph.OperatorImplGraphUtil(config);
    Map<SystemStream, Integer> counts = util.getProducerTaskCountForIntermediateStreams(
        streamToConsumerTasks, intermediateToInputStreams);
    assertTrue(counts.get(int1) == 3);
    assertTrue(counts.get(int2) == 2);
  }
}
