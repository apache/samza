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
package org.apache.samza.execution;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericOutputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link IntermediateStreamPartitionPlanner}
 */
public class TestIntermediateStreamPartitionPlanner {

  private JobGraph mockGraph;
  private StreamApplicationDescriptorImpl mockStreamAppDesc;
  private Config mockConfig;
  private JobNode mockJobNode;
  private StreamSpec input1Spec;
  private StreamSpec input2Spec;
  private StreamSpec outputSpec;
  private StreamSpec repartitionSpec;
  private StreamSpec broadcastSpec;
  private KVSerde<String, Object> defaultSerde;
  private GenericSystemDescriptor inputSystemDescriptor;
  private GenericSystemDescriptor outputSystemDescriptor;
  private GenericSystemDescriptor intermediateSystemDescriptor;
  private GenericInputDescriptor<KV<String, Object>> input1Descriptor;
  private GenericInputDescriptor<KV<String, Object>> input2Descriptor;
  private GenericInputDescriptor<KV<String, Object>> intermediateInputDescriptor;
  private GenericInputDescriptor<KV<String, Object>> broadcastInputDesriptor;
  private GenericOutputDescriptor<KV<String, Object>> outputDescriptor;
  private GenericOutputDescriptor<KV<String, Object>> intermediateOutputDescriptor;

  @Before
  public void setUp() {
    input1Spec = new StreamSpec("input1", "input1", "input-system");
    input2Spec = new StreamSpec("input2", "input2", "input-system");
    outputSpec = new StreamSpec("output", "output", "output-system");
    repartitionSpec =
        new StreamSpec("jobName-jobId-partition_by-p1", "partition_by-p1", "intermediate-system");
    broadcastSpec = new StreamSpec("jobName-jobId-broadcast-b1", "broadcast-b1", "intermediate-system");


    defaultSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>());
    inputSystemDescriptor = new GenericSystemDescriptor("input-system", "mockSystemFactoryClassName");
    outputSystemDescriptor = new GenericSystemDescriptor("output-system", "mockSystemFactoryClassName");
    intermediateSystemDescriptor = new GenericSystemDescriptor("intermediate-system", "mockSystemFactoryClassName");
    input1Descriptor = inputSystemDescriptor.getInputDescriptor("input1", defaultSerde);
    input2Descriptor = inputSystemDescriptor.getInputDescriptor("input2", defaultSerde);
    outputDescriptor = outputSystemDescriptor.getOutputDescriptor("output", defaultSerde);
    intermediateInputDescriptor = intermediateSystemDescriptor.getInputDescriptor("jobName-jobId-partition_by-p1", defaultSerde)
        .withPhysicalName("partition_by-p1");
    intermediateOutputDescriptor = intermediateSystemDescriptor.getOutputDescriptor("jobName-jobId-partition_by-p1", defaultSerde)
        .withPhysicalName("partition_by-p1");
    broadcastInputDesriptor = intermediateSystemDescriptor.getInputDescriptor("jobName-jobId-broadcast-b1", defaultSerde);

    Map<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), "jobName");
    configs.put(JobConfig.JOB_ID(), "jobId");
    mockConfig = spy(new MapConfig(configs));

    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionJoinStreamApplication(), mockConfig);

    mockJobNode = mock(JobNode.class);
    StreamEdge input1Edge = new StreamEdge(input1Spec, false, false, mockConfig);
    StreamEdge input2Edge = new StreamEdge(input2Spec, false, false, mockConfig);
    StreamEdge outputEdge = new StreamEdge(outputSpec, false, false, mockConfig);
    StreamEdge repartitionEdge = new StreamEdge(repartitionSpec, true, false, mockConfig);
    List<StreamEdge> inputEdges = new ArrayList<>();
    inputEdges.add(input1Edge);
    inputEdges.add(input2Edge);
    inputEdges.add(repartitionEdge);
    List<StreamEdge> outputEdges = new ArrayList<>();
    outputEdges.add(outputEdge);
    outputEdges.add(repartitionEdge);
    when(mockJobNode.getInEdges()).thenReturn(inputEdges);
    when(mockJobNode.getOutEdges()).thenReturn(outputEdges);
    when(mockJobNode.getConfig()).thenReturn(mockConfig);
    when(mockJobNode.getJobName()).thenReturn("jobName");
    when(mockJobNode.getJobId()).thenReturn("jobId");
    when(mockJobNode.getId()).thenReturn(JobNode.createId("jobName", "jobId"));

    mockGraph = mock(JobGraph.class);
  }

  @Test
  public void testConstructor() {
    StreamApplicationDescriptorImpl mockAppDesc = spy(new StreamApplicationDescriptorImpl(appDesc -> { },
        mock(Config.class)));
    InputOperatorSpec inputOp1 = mock(InputOperatorSpec.class);
    InputOperatorSpec inputOp2 = mock(InputOperatorSpec.class);
    Map<String, InputOperatorSpec> inputOpMaps = new HashMap<>();
    inputOpMaps.put("input-op1", inputOp1);
    inputOpMaps.put("input-op2", inputOp2);
    when(mockAppDesc.getInputOperators()).thenReturn(inputOpMaps);
    IntermediateStreamPartitionPlanner partitionPlanner = new IntermediateStreamPartitionPlanner(mock(Config.class),
        mockAppDesc);
    JobGraph mockGraph = mock(JobGraph.class);
    partitionPlanner.calculatePartitions(mockGraph);
  }

  private StreamApplication getRepartitionJoinStreamApplication() {
    return appDesc -> {
      MessageStream<KV<String, Object>> input1 = appDesc.getInputStream(input1Descriptor);
      MessageStream<KV<String, Object>> input2 = appDesc.getInputStream(input2Descriptor);
      OutputStream<KV<String, Object>> output = appDesc.getOutputStream(outputDescriptor);
      JoinFunction<String, Object, Object, KV<String, Object>> mockJoinFn = mock(JoinFunction.class);
      input1
          .partitionBy(KV::getKey, KV::getValue, defaultSerde, "p1")
          .map(kv -> kv.value)
          .join(input2.map(kv -> kv.value), mockJoinFn,
              new StringSerde(), new JsonSerdeV2<>(Object.class), new JsonSerdeV2<>(Object.class),
              Duration.ofHours(1), "j1")
          .sendTo(output);
    };
  }
}
