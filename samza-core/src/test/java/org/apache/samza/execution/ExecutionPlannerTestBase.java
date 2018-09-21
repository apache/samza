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
import org.apache.samza.application.ApplicationDescriptorImpl;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptorImpl;
import org.apache.samza.application.TaskApplication;
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
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.task.IdentityStreamTask;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


/**
 * Unit test base class to set up commonly used test application and configuration.
 */
class ExecutionPlannerTestBase {
  protected StreamApplicationDescriptorImpl mockStreamAppDesc;
  protected Config mockConfig;
  protected JobNode mockJobNode;
  protected KVSerde<String, Object> defaultSerde;
  protected GenericSystemDescriptor inputSystemDescriptor;
  protected GenericSystemDescriptor outputSystemDescriptor;
  protected GenericSystemDescriptor intermediateSystemDescriptor;
  protected GenericInputDescriptor<KV<String, Object>> input1Descriptor;
  protected GenericInputDescriptor<KV<String, Object>> input2Descriptor;
  protected GenericInputDescriptor<KV<String, Object>> intermediateInputDescriptor;
  protected GenericInputDescriptor<KV<String, Object>> broadcastInputDesriptor;
  protected GenericOutputDescriptor<KV<String, Object>> outputDescriptor;
  protected GenericOutputDescriptor<KV<String, Object>> intermediateOutputDescriptor;

  @Before
  public void setUp() {
    defaultSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>());
    inputSystemDescriptor = new GenericSystemDescriptor("input-system", "mockSystemFactoryClassName");
    outputSystemDescriptor = new GenericSystemDescriptor("output-system", "mockSystemFactoryClassName");
    intermediateSystemDescriptor = new GenericSystemDescriptor("intermediate-system", "mockSystemFactoryClassName");
    input1Descriptor = inputSystemDescriptor.getInputDescriptor("input1", defaultSerde);
    input2Descriptor = inputSystemDescriptor.getInputDescriptor("input2", defaultSerde);
    outputDescriptor = outputSystemDescriptor.getOutputDescriptor("output", defaultSerde);
    intermediateInputDescriptor = intermediateSystemDescriptor.getInputDescriptor("jobName-jobId-partition_by-p1", defaultSerde)
        .withPhysicalName("jobName-jobId-partition_by-p1");
    intermediateOutputDescriptor = intermediateSystemDescriptor.getOutputDescriptor("jobName-jobId-partition_by-p1", defaultSerde)
        .withPhysicalName("jobName-jobId-partition_by-p1");
    broadcastInputDesriptor = intermediateSystemDescriptor.getInputDescriptor("jobName-jobId-broadcast-b1", defaultSerde)
        .withPhysicalName("jobName-jobId-broadcast-b1");

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

  String getJobNameAndId() {
    return "jobName-jobId";
  }

  void configureJobNode(ApplicationDescriptorImpl mockStreamAppDesc) {
    JobGraph jobGraph = new ExecutionPlanner(mockConfig, mock(StreamManager.class))
        .createJobGraph(mockConfig, mockStreamAppDesc);
    mockJobNode = spy(jobGraph.getJobNodes().get(0));
  }

  StreamApplication getRepartitionOnlyStreamApplication() {
    return appDesc -> {
      MessageStream<KV<String, Object>> input1 = appDesc.getInputStream(input1Descriptor);
      input1.partitionBy(KV::getKey, KV::getValue, "p1");
    };
  }

  StreamApplication getRepartitionJoinStreamApplication() {
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

  TaskApplication getTaskApplication() {
    return appDesc -> {
      appDesc.addInputStream(input1Descriptor);
      appDesc.addInputStream(input2Descriptor);
      appDesc.addInputStream(intermediateInputDescriptor);
      appDesc.addOutputStream(intermediateOutputDescriptor);
      appDesc.addOutputStream(outputDescriptor);
      appDesc.setTaskFactory(() -> new IdentityStreamTask());
    };
  }

  TaskApplication getLegacyTaskApplication() {
    return new LegacyTaskApplication(IdentityStreamTask.class.getName());
  }

  StreamApplication getBroadcastOnlyStreamApplication(Serde serde) {
    return appDesc -> {
      MessageStream<KV<String, Object>> input = appDesc.getInputStream(input1Descriptor);
      if (serde != null) {
        input.broadcast(serde, "b1");
      } else {
        input.broadcast("b1");
      }
    };
  }
}
