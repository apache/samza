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
import java.util.concurrent.CountDownLatch;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.TaskApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.operator.data.PageView;
import org.junit.Test;

import static org.apache.samza.test.framework.TestTimerApp.*;
import static org.junit.Assert.*;


public class FaultInjectionTest extends StreamApplicationIntegrationTestHarness {
  @Test
  public void testRaceCondition() throws InterruptedException {
    CountDownLatch startUpLatch = new CountDownLatch(1);
    Map<String, String> configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, "org.apache.samza.standalone.PassthroughCoordinationUtilsFactory");
    configs.put(JobConfig.PROCESSOR_ID(), "0");
    configs.put(TaskConfig.GROUPER_FACTORY(), "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");
    configs.put(FaultInjectionStreamApp.INPUT_TOPIC_NAME_PROP, "page-views");
    configs.put(TaskConfig.INPUT_STREAMS(), "kafka.page-views");
    configs.put("task.shutdown.ms", "10");
    configs.put(JobConfig.PROCESSOR_ID(), "0");

    createTopic(PAGE_VIEWS, 2);

    // create events for the following user activity.
    // userId: (viewId, pageId, (adIds))
    // u1: (v1, p1, (a1)), (v2, p2, (a3))
    // u2: (v3, p1, (a1)), (v4, p3, (a5))
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v1\",\"pageId\":\"p1\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 1, "p2", "{\"viewId\":\"v2\",\"pageId\":\"p2\",\"userId\":\"u1\"}");

    FaultInjectionStreamApp app = new FaultInjectionStreamApp();
    FaultInjectionStreamApp.shutdownLatch = startUpLatch;
    RunApplicationContext context =
        runApplication(app, "fault-injection-app", configs);

    startUpLatch.await();
    context.getRunner().kill();
    context.getRunner().waitForFinish();
    assertEquals(context.getRunner().status(), ApplicationStatus.UnsuccessfulFinish);
  }

  private static class FaultInjectionStreamApp implements TaskApplication {
    public static final String SYSTEM = "kafka";
    public static final String INPUT_TOPIC_NAME_PROP = "inputTopicName";
    private static transient CountDownLatch shutdownLatch;

    @Override
    public void describe(TaskApplicationDescriptor appDesc) {
      Config config = appDesc.getConfig();
      String inputTopic = config.get(INPUT_TOPIC_NAME_PROP);

      final JsonSerdeV2<PageView> serde = new JsonSerdeV2<>(PageView.class);
      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(SYSTEM);
      KafkaInputDescriptor<PageView> isd = ksd.getInputDescriptor(inputTopic, serde);
      appDesc.addInputStream(isd);
      appDesc.setTaskFactory((StreamTaskFactory) () -> new FaultInjectionTask(shutdownLatch));
    }

    private static class FaultInjectionTask implements StreamTask, ClosableTask {
      private final transient CountDownLatch shutdownLatch;

      public FaultInjectionTask(CountDownLatch shutdownLatch) {
        this.shutdownLatch = shutdownLatch;
      }

      @Override
      public void close() throws Exception {
        shutdownLatch.countDown();
      }

      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
          throws Exception {
        throw new RuntimeException("Failed");
      }
    }
  }
}
