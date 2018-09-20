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
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.testUtils.StreamTestUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.apache.samza.execution.TestExecutionPlanner.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestJobGraphJsonGenerator {

  public class PageViewEvent {
    String getCountry() {
      return "";
    }
  }

  @Test
  public void test() throws Exception {

    /**
     * the graph looks like the following.
     * number in parentheses () indicates number of stream partitions.
     * number in parentheses in quotes ("") indicates expected partition count.
     * number in square brackets [] indicates operator ID.
     *
     * input3 (32) -> filter [7] -> partitionBy [8] ("64") -> map [10] -> join [14] -> sendTo(output2) [15] (16)
     *                                                                   |
     *              input2 (16) -> partitionBy [3] ("64") -> filter [5] -| -> sink [13]
     *                                                                   |
     *                                         input1 (64) -> map [1] -> join [11] -> sendTo(output1) [12] (8)
     *
     */

    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");
    StreamTestUtils.addStreamConfigs(configMap, "input1", "system1", "input1");
    StreamTestUtils.addStreamConfigs(configMap, "input2", "system2", "input2");
    StreamTestUtils.addStreamConfigs(configMap, "input3", "system2", "input3");
    StreamTestUtils.addStreamConfigs(configMap, "output1", "system1", "output1");
    StreamTestUtils.addStreamConfigs(configMap, "output2", "system2", "output2");
    Config config = new MapConfig(configMap);

    // set up external partition count
    Map<String, Integer> system1Map = new HashMap<>();
    system1Map.put("input1", 64);
    system1Map.put("output1", 8);
    Map<String, Integer> system2Map = new HashMap<>();
    system2Map.put("input2", 16);
    system2Map.put("input3", 32);
    system2Map.put("output2", 16);

    SystemAdmin systemAdmin1 = createSystemAdmin(system1Map);
    SystemAdmin systemAdmin2 = createSystemAdmin(system2Map);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    when(systemAdmins.getSystemAdmin("system1")).thenReturn(systemAdmin1);
    when(systemAdmins.getSystemAdmin("system2")).thenReturn(systemAdmin2);
    StreamManager streamManager = new StreamManager(systemAdmins);

    StreamApplicationDescriptorImpl graphSpec = new StreamApplicationDescriptorImpl(appDesc -> {
        KVSerde<Object, Object> kvSerde = new KVSerde<>(new NoOpSerde(), new NoOpSerde());
        String mockSystemFactoryClass = "factory.class.name";
        GenericSystemDescriptor system1 = new GenericSystemDescriptor("system1", mockSystemFactoryClass);
        GenericSystemDescriptor system2 = new GenericSystemDescriptor("system2", mockSystemFactoryClass);
        GenericInputDescriptor<KV<Object, Object>> input1Descriptor = system1.getInputDescriptor("input1", kvSerde);
        GenericInputDescriptor<KV<Object, Object>> input2Descriptor = system2.getInputDescriptor("input2", kvSerde);
        GenericInputDescriptor<KV<Object, Object>> input3Descriptor = system2.getInputDescriptor("input3", kvSerde);
        GenericOutputDescriptor<KV<Object, Object>>  output1Descriptor = system1.getOutputDescriptor("output1", kvSerde);
        GenericOutputDescriptor<KV<Object, Object>> output2Descriptor = system2.getOutputDescriptor("output2", kvSerde);

        MessageStream<KV<Object, Object>> messageStream1 =
            appDesc.getInputStream(input1Descriptor)
                .map(m -> m);
        MessageStream<KV<Object, Object>> messageStream2 =
            appDesc.getInputStream(input2Descriptor)
                .partitionBy(m -> m.key, m -> m.value, "p1")
                .filter(m -> true);
        MessageStream<KV<Object, Object>> messageStream3 =
            appDesc.getInputStream(input3Descriptor)
                .filter(m -> true)
                .partitionBy(m -> m.key, m -> m.value, "p2")
                .map(m -> m);
        OutputStream<KV<Object, Object>> outputStream1 = appDesc.getOutputStream(output1Descriptor);
        OutputStream<KV<Object, Object>> outputStream2 = appDesc.getOutputStream(output2Descriptor);

        messageStream1
            .join(messageStream2,
                (JoinFunction<Object, KV<Object, Object>, KV<Object, Object>, KV<Object, Object>>) mock(JoinFunction.class),
                mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(2), "j1")
            .sendTo(outputStream1);
        messageStream2.sink((message, collector, coordinator) -> { });
        messageStream3
            .join(messageStream2,
                (JoinFunction<Object, KV<Object, Object>, KV<Object, Object>, KV<Object, Object>>) mock(JoinFunction.class),
                mock(Serde.class), mock(Serde.class), mock(Serde.class), Duration.ofHours(1), "j2")
            .sendTo(outputStream2);
      }, config);

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    ExecutionPlan plan = planner.plan(graphSpec.getOperatorSpecGraph());
    String json = plan.getPlanAsJson();
    System.out.println(json);

    // deserialize
    ObjectMapper mapper = new ObjectMapper();
    JobGraphJsonGenerator.JobGraphJson nodes = mapper.readValue(json, JobGraphJsonGenerator.JobGraphJson.class);
    assertEquals(5, nodes.jobs.get(0).operatorGraph.inputStreams.size());
    assertEquals(11, nodes.jobs.get(0).operatorGraph.operators.size());
    assertEquals(3, nodes.sourceStreams.size());
    assertEquals(2, nodes.sinkStreams.size());
    assertEquals(2, nodes.intermediateStreams.size());
  }

  @Test
  public void test2() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");
    StreamTestUtils.addStreamConfigs(configMap, "PageView", "hdfs", "hdfs:/user/dummy/PageViewEvent");
    StreamTestUtils.addStreamConfigs(configMap, "PageViewCount", "kafka", "PageViewCount");
    Config config = new MapConfig(configMap);

    // set up external partition count
    Map<String, Integer> system1Map = new HashMap<>();
    system1Map.put("hdfs:/user/dummy/PageViewEvent", 512);
    Map<String, Integer> system2Map = new HashMap<>();
    system2Map.put("PageViewCount", 16);

    SystemAdmin systemAdmin1 = createSystemAdmin(system1Map);
    SystemAdmin systemAdmin2 = createSystemAdmin(system2Map);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    when(systemAdmins.getSystemAdmin("hdfs")).thenReturn(systemAdmin1);
    when(systemAdmins.getSystemAdmin("kafka")).thenReturn(systemAdmin2);
    StreamManager streamManager = new StreamManager(systemAdmins);

    StreamApplicationDescriptorImpl graphSpec = new StreamApplicationDescriptorImpl(appDesc -> {
        KVSerde<String, PageViewEvent> pvSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageViewEvent.class));
        GenericSystemDescriptor isd = new GenericSystemDescriptor("hdfs", "mockSystemFactoryClass");
        GenericInputDescriptor<KV<String, PageViewEvent>> pageView = isd.getInputDescriptor("PageView", pvSerde);

        KVSerde<String, Long> pvcSerde = KVSerde.of(new StringSerde(), new LongSerde());
        GenericSystemDescriptor osd = new GenericSystemDescriptor("kafka", "mockSystemFactoryClass");
        GenericOutputDescriptor<KV<String, Long>> pageViewCount = osd.getOutputDescriptor("PageViewCount", pvcSerde);

        MessageStream<KV<String, PageViewEvent>> inputStream = appDesc.getInputStream(pageView);
        OutputStream<KV<String, Long>> outputStream = appDesc.getOutputStream(pageViewCount);
        inputStream
            .partitionBy(kv -> kv.getValue().getCountry(), kv -> kv.getValue(), "keyed-by-country")
            .window(Windows.keyedTumblingWindow(kv -> kv.getValue().getCountry(),
                Duration.ofSeconds(10L),
                () -> 0L,
                (m, c) -> c + 1L,
                new StringSerde(),
                new LongSerde()), "count-by-country")
            .map(pane -> new KV<>(pane.getKey().getKey(), pane.getMessage()))
            .sendTo(outputStream);
      }, config);

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    ExecutionPlan plan = planner.plan(graphSpec.getOperatorSpecGraph());
    String json = plan.getPlanAsJson();
    System.out.println(json);

    // deserialize
    ObjectMapper mapper = new ObjectMapper();
    JobGraphJsonGenerator.JobGraphJson nodes = mapper.readValue(json, JobGraphJsonGenerator.JobGraphJson.class);
    JobGraphJsonGenerator.OperatorGraphJson operatorGraphJson = nodes.jobs.get(0).operatorGraph;
    assertEquals(2, operatorGraphJson.inputStreams.size());
    assertEquals(4, operatorGraphJson.operators.size());
    assertEquals(1, nodes.sourceStreams.size());
    assertEquals(1, nodes.sinkStreams.size());
    assertEquals(1, nodes.intermediateStreams.size());

    // verify partitionBy op output to the intermdiate stream of the same id
    assertEquals(operatorGraphJson.operators.get("test-app-1-partition_by-keyed-by-country").get("outputStreamId"),
        "test-app-1-partition_by-keyed-by-country");
    assertEquals(operatorGraphJson.operators.get("test-app-1-send_to-5").get("outputStreamId"),
        "PageViewCount");
  }
}
