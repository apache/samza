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
import java.util.HashMap;
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
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

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
    configs.putAll(input1Descriptor.toConfig());
    configs.putAll(input2Descriptor.toConfig());
    configs.putAll(outputDescriptor.toConfig());
    configs.putAll(inputSystemDescriptor.toConfig());
    configs.putAll(outputSystemDescriptor.toConfig());
    configs.putAll(intermediateSystemDescriptor.toConfig());
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), intermediateSystemDescriptor.getSystemName());
    mockConfig = spy(new MapConfig(configs));

    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionJoinStreamApplication(), mockConfig);
  }

  @Test
  public void testCalculateRepartitionJoinTopicPartitions() {
    IntermediateStreamPartitionPlanner partitionPlanner = new IntermediateStreamPartitionPlanner(mockConfig, mockStreamAppDesc);
    JobGraph mockGraph = new ExecutionPlanner(mockConfig, mock(StreamManager.class)).createJobGraph(mockConfig, mockStreamAppDesc,
        mock(JobGraphJsonGenerator.class), mock(JobNodeConfigureGenerator.class));
    // set the input stream partitions
    mockGraph.getSources().forEach(inEdge -> {
        if (inEdge.getStreamSpec().getId().equals(input1Descriptor.getStreamId())) {
          inEdge.setPartitionCount(6);
        } else if (inEdge.getStreamSpec().getId().equals(input2Descriptor.getStreamId())) {
          inEdge.setPartitionCount(5);
        }
      });
    partitionPlanner.calculatePartitions(mockGraph);
    assertEquals(1, mockGraph.getIntermediateStreamEdges().size());
    assertEquals(5, mockGraph.getIntermediateStreamEdges().stream()
        .filter(inEdge -> inEdge.getStreamSpec().getId().equals(intermediateInputDescriptor.getStreamId()))
        .findFirst().get().getPartitionCount());
  }

  @Test
  public void testCalculateRepartitionIntermediateTopicPartitions() {
    mockStreamAppDesc = new StreamApplicationDescriptorImpl(getRepartitionOnlyStreamApplication(), mockConfig);
    IntermediateStreamPartitionPlanner partitionPlanner = new IntermediateStreamPartitionPlanner(mockConfig, mockStreamAppDesc);
    JobGraph mockGraph = new ExecutionPlanner(mockConfig, mock(StreamManager.class)).createJobGraph(mockConfig, mockStreamAppDesc,
        mock(JobGraphJsonGenerator.class), mock(JobNodeConfigureGenerator.class));
    // set the input stream partitions
    mockGraph.getSources().forEach(inEdge -> inEdge.setPartitionCount(7));
    partitionPlanner.calculatePartitions(mockGraph);
    assertEquals(1, mockGraph.getIntermediateStreamEdges().size());
    assertEquals(7, mockGraph.getIntermediateStreamEdges().stream()
        .filter(inEdge -> inEdge.getStreamSpec().getId().equals(intermediateInputDescriptor.getStreamId()))
        .findFirst().get().getPartitionCount());
  }

  private StreamApplication getRepartitionOnlyStreamApplication() {
    return appDesc -> {
      MessageStream<KV<String, Object>> input1 = appDesc.getInputStream(input1Descriptor);
      OutputStream<KV<String, Object>> output = appDesc.getOutputStream(outputDescriptor);
      JoinFunction<String, Object, Object, KV<String, Object>> mockJoinFn = mock(JoinFunction.class);
      input1.partitionBy(KV::getKey, KV::getValue, defaultSerde, "p1").sendTo(output);
    };
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
