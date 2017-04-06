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
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.apache.samza.execution.TestExecutionPlanner.createSystemAdmin;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestJobGraphJsonGenerator {

  @Test
  public void test() throws Exception {

    /**
     * the graph looks like the following. number of partitions in parentheses. quotes indicate expected value.
     *
     *                               input1 (64) -> map -> join -> output1 (8)
     *                                                       |
     *          input2 (16) -> partitionBy ("64") -> filter -|
     *                                                       |
     * input3 (32) -> filter -> partitionBy ("64") -> map -> join -> output2 (16)
     *
     */

    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");
    Config config = new MapConfig(configMap);

    StreamSpec input1 = new StreamSpec("input1", "input1", "system1");
    StreamSpec input2 = new StreamSpec("input2", "input2", "system2");
    StreamSpec input3 = new StreamSpec("input3", "input3", "system2");

    StreamSpec output1 = new StreamSpec("output1", "output1", "system1");
    StreamSpec output2 = new StreamSpec("output2", "output2", "system2");

    ApplicationRunner runner = mock(ApplicationRunner.class);
    when(runner.getStreamSpec("input1")).thenReturn(input1);
    when(runner.getStreamSpec("input2")).thenReturn(input2);
    when(runner.getStreamSpec("input3")).thenReturn(input3);
    when(runner.getStreamSpec("output1")).thenReturn(output1);
    when(runner.getStreamSpec("output2")).thenReturn(output2);

    // intermediate streams used in tests
    when(runner.getStreamSpec("test-app-1-partition_by-0"))
        .thenReturn(new StreamSpec("test-app-1-partition_by-0", "test-app-1-partition_by-0", "default-system"));
    when(runner.getStreamSpec("test-app-1-partition_by-1"))
        .thenReturn(new StreamSpec("test-app-1-partition_by-1", "test-app-1-partition_by-1", "default-system"));
    when(runner.getStreamSpec("test-app-1-partition_by-4"))
        .thenReturn(new StreamSpec("test-app-1-partition_by-4", "test-app-1-partition_by-4", "default-system"));

    // set up external partition count
    Map<String, Integer> system1Map = new HashMap<>();
    system1Map.put("input1", 64);
    system1Map.put("output1", 8);
    Map<String, Integer> system2Map = new HashMap<>();
    system2Map.put("input2", 16);
    system2Map.put("input3", 32);
    system2Map.put("output2", 16);

    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    SystemAdmin systemAdmin1 = createSystemAdmin(system1Map);
    SystemAdmin systemAdmin2 = createSystemAdmin(system2Map);
    systemAdmins.put("system1", systemAdmin1);
    systemAdmins.put("system2", systemAdmin2);
    StreamManager streamManager = new StreamManager(systemAdmins);

    StreamGraphImpl streamGraph = new StreamGraphImpl(runner, config);
    MessageStream m1 = streamGraph.getInputStream("input1", null).map(m -> m);
    MessageStream m2 = streamGraph.getInputStream("input2", null).partitionBy(m -> "haha").filter(m -> true);
    MessageStream m3 = streamGraph.getInputStream("input3", null).filter(m -> true).partitionBy(m -> "hehe").map(m -> m);
    OutputStream<Object, Object, Object> outputStream1 = streamGraph.getOutputStream("output1", null, null);
    OutputStream<Object, Object, Object> outputStream2 = streamGraph.getOutputStream("output2", null, null);

    m1.join(m2, mock(JoinFunction.class), Duration.ofHours(2)).sendTo(outputStream1);
    m3.join(m2, mock(JoinFunction.class), Duration.ofHours(1)).sendTo(outputStream2);

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    ExecutionPlan plan = planner.plan(streamGraph);
    String json = plan.getPlanAsJson();
    System.out.println(json);

    // deserialize
    ObjectMapper mapper = new ObjectMapper();
    JobGraphJsonGenerator.JobGraphJson nodes = mapper.readValue(json, JobGraphJsonGenerator.JobGraphJson.class);
    assertTrue(nodes.jobs.get(0).operatorGraph.inputStreams.size() == 5);
    assertTrue(nodes.jobs.get(0).operatorGraph.operators.size() == 12);
    assertTrue(nodes.streams.size() == 7);
  }
}
