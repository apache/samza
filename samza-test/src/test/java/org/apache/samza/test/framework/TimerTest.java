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

package org.apache.samza.test.framework;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.samza.test.framework.TestTimerApp.*;

public class TimerTest extends StreamApplicationIntegrationTestHarness {

  @Before
  public void setup() {
    // create topics
    createTopic(PAGE_VIEWS, 2);

    // create events for the following user activity.
    // userId: (viewId, pageId, (adIds))
    // u1: (v1, p1, (a1)), (v2, p2, (a3))
    // u2: (v3, p1, (a1)), (v4, p3, (a5))
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v1\",\"pageId\":\"p1\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 1, "p2", "{\"viewId\":\"v2\",\"pageId\":\"p2\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v3\",\"pageId\":\"p1\",\"userId\":\"u2\"}");
    produceMessage(PAGE_VIEWS, 1, "p3", "{\"viewId\":\"v4\",\"pageId\":\"p3\",\"userId\":\"u2\"}");

  }

  @Test
  public void testJob() throws InterruptedException {
    Map<String, String> configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
    configs.put("job.systemstreampartition.grouper.factory", "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory");
    configs.put("task.name.grouper.factory", "org.apache.samza.container.grouper.task.SingleContainerGrouperFactory");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, "org.apache.samza.standalone.PassthroughCoordinationUtilsFactory");
    configs.put(JobConfig.PROCESSOR_ID(), "0");

    runApplication(new TestTimerApp(), "TimerTest", configs);
  }
}
