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
package org.apache.samza.runtime;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.GenericOutputDescriptor;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.serializers.StringSerde;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestAbstractApplicationRunner {
  @Test
  public void testExecutionPlanContainsSystemStreamDescriptorGeneratedConfigs() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    MockApplicationRunner ar = new MockApplicationRunner(new MapConfig(configMap));
    MockStreamApplication mockStreamApplication = new MockStreamApplication();
    ExecutionPlan executionPlan = ar.getExecutionPlan(mockStreamApplication, mock(StreamManager.class));
    JobConfig jobConfig = executionPlan.getJobConfigs().get(0);

    assertEquals("mock-system", jobConfig.get("streams.mock-input-stream.samza.system"));
    assertEquals("mock-system", jobConfig.get("streams.mock-output-stream.samza.system"));
    assertEquals("mock.factory.class", jobConfig.get("systems.mock-system.samza.factory"));
    assertEquals("mock-system.mock-input-stream-physical-name", jobConfig.get("task.inputs"));
    assertTrue(jobConfig.containsKey("streams.mock-input-stream.samza.key.serde"));
    assertTrue(jobConfig.containsKey("streams.mock-input-stream.samza.msg.serde"));
    assertTrue(jobConfig.containsKey("streams.mock-output-stream.samza.key.serde"));
    assertTrue(jobConfig.containsKey("streams.mock-output-stream.samza.msg.serde"));
  }

  @Test
  public void testSystemStreamDescriptorConfigsOverrideUserConfig() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("streams.mock-input-stream.samza.system", "my-system");
    configMap.put("streams.mock-output-stream.samza.system", "my-system");
    configMap.put("systems.mock-system.samza.factory", "my.system.factory.class");

    MockApplicationRunner ar = new MockApplicationRunner(new MapConfig(configMap));
    MockStreamApplication mockStreamApplication = new MockStreamApplication();
    ExecutionPlan executionPlan = ar.getExecutionPlan(mockStreamApplication, mock(StreamManager.class));

    JobConfig jobConfig = executionPlan.getJobConfigs().get(0);
    assertEquals("mock-system", jobConfig.get("streams.mock-input-stream.samza.system"));
    assertEquals("mock-system", jobConfig.get("streams.mock-output-stream.samza.system"));
    assertEquals("mock.factory.class", jobConfig.get("systems.mock-system.samza.factory"));
    assertEquals("mock-system.mock-input-stream-physical-name", jobConfig.get("task.inputs"));
  }
}

class MockStreamApplication implements StreamApplication {
  @Override
  public void init(StreamGraph graph, Config config) {
    GenericSystemDescriptor<String> sd = new GenericSystemDescriptor<>("mock-system", "mock.factory.class", new StringSerde());
    GenericInputDescriptor<String> isd = sd.getInputDescriptor("mock-input-stream").withPhysicalName("mock-input-stream-physical-name");
    GenericOutputDescriptor<String> osd = sd.getOutputDescriptor("mock-output-stream");
    graph.getInputStream(isd).sendTo(graph.getOutputStream(osd));
  }
}

class MockApplicationRunner extends AbstractApplicationRunner {
  public MockApplicationRunner(Config config) {
    super(config);
  }

  @Override
  public void runTask() { }

  @Override
  public void run(StreamApplication streamApp) { }

  @Override
  public void kill(StreamApplication streamApp) { }

  @Override
  public ApplicationStatus status(StreamApplication streamApp) {
    return null;
  }
}
