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
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.apache.samza.execution.TestExecutionUtils.createJoin;
import static org.apache.samza.execution.TestExecutionUtils.createRunner;
import static org.apache.samza.execution.TestExecutionUtils.createSystemAdmin;
import static org.junit.Assert.assertTrue;


public class TestPlanJsonGenerator {

  @Test
  public void test() throws Exception {
    /** the graph looks like the following
     *
     *                        input1 -> map -> join -> output1
     *                                           |
     *          input2 -> partitionBy -> filter -|
     *                                           |
     * input3 -> filter -> partitionBy -> map -> join -> output2
     *
     */
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_NAME(), "test-app");
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");
    Config config = new MapConfig(configMap);

    ApplicationRunner runner = createRunner(config);

    StreamSpec input1 = new StreamSpec("input1", "input1", "system1");
    StreamSpec input2 = new StreamSpec("input2", "input2", "system2");
    StreamSpec input3 = new StreamSpec("input3", "input3", "system2");

    StreamSpec output1 = new StreamSpec("output1", "output1", "system1");
    StreamSpec output2 = new StreamSpec("output2", "output2", "system2");

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

    StreamGraph streamGraph = new StreamGraphImpl(runner, config);
    MessageStream m1 = streamGraph.createInStream(input1, null, null).map(m -> m);
    MessageStream m2 = streamGraph.createInStream(input2, null, null).partitionBy(m -> "haha").filter(m -> true);
    MessageStream m3 = streamGraph.createInStream(input3, null, null).filter(m -> true).partitionBy(m -> "hehe").map(m -> m);

    m1.join(m2, createJoin(), Duration.ofHours(1)).sendTo(streamGraph.createOutStream(output1, null, null));
    m3.join(m2, createJoin(), Duration.ofHours(2)).sendTo(streamGraph.createOutStream(output2, null, null));

    ExecutionPlanner planner = new ExecutionPlanner(config, streamManager);
    StreamPlan plan = planner.plan(streamGraph);
    String json = plan.getPlanAsJson();
    System.out.println(json);

    // deserialize
    ObjectMapper mapper = new ObjectMapper();
    PlanJsonGenerator.GraphNodes nodes = mapper.readValue(json, PlanJsonGenerator.GraphNodes.class);
    assertTrue(nodes.getJobs().get(0).getOpNodes().size() == 12);
    assertTrue(nodes.getStreams().size() == 7);
  }
}
