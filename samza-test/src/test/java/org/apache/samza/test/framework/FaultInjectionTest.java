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
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.operator.data.PageView;
import org.junit.Test;

import static org.junit.Assert.*;


public class FaultInjectionTest extends StreamApplicationIntegrationTestHarness {
  private static final String PAGE_VIEWS = "page-views";

  @Test
  public void testRaceCondition() throws InterruptedException {
    int taskShutdownInMs = (int) (Math.random() * 10000);

    CountDownLatch containerShutdownLatch = new CountDownLatch(1);
    Map<String, String> configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.zk.ZkJobCoordinatorFactory");
    configs.put(JobConfig.PROCESSOR_ID(), "0");
    configs.put(TaskConfig.GROUPER_FACTORY(), "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");
    configs.put(FaultInjectionStreamApp.INPUT_TOPIC_NAME_PROP, "page-views");
    configs.put(TaskConfig.INPUT_STREAMS(), "kafka.page-views");
    configs.put(ZkConfig.ZK_CONNECT, zkConnect());
    configs.put(JobConfig.JOB_DEBOUNCE_TIME_MS(), "5000");

    // we purposefully randomize the task.shutdown.ms to make sure we can consistently verify if status is unsuccessfulFinish
    // even though the reason for failure can either be container exception or container shutdown timing out.
    configs.put("task.shutdown.ms", Integer.toString(taskShutdownInMs));
    configs.put(JobConfig.PROCESSOR_ID(), "0");

    createTopic(PAGE_VIEWS, 2);

    // create events for the following user activity.
    // userId: (viewId, pageId, (adIds))
    // u1: (v1, p1, (a1)), (v2, p2, (a3))
    // u2: (v3, p1, (a1)), (v4, p3, (a5))
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v1\",\"pageId\":\"p1\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 1, "p2", "{\"viewId\":\"v2\",\"pageId\":\"p2\",\"userId\":\"u1\"}");

    FaultInjectionStreamApp app = new FaultInjectionStreamApp();
    FaultInjectionStreamApp.containerShutdownLatch = containerShutdownLatch;
    RunApplicationContext context =
        runApplication(app, "fault-injection-app", configs);

    containerShutdownLatch.await();
    context.getRunner().kill();
    context.getRunner().waitForFinish();
    assertEquals(context.getRunner().status(), ApplicationStatus.UnsuccessfulFinish);
  }

  private static class FaultInjectionStreamApp implements TaskApplication {
    public static final String SYSTEM = "kafka";
    public static final String INPUT_TOPIC_NAME_PROP = "inputTopicName";
    private static transient CountDownLatch containerShutdownLatch;

    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      Config config = appDescriptor.getConfig();
      String inputTopic = config.get(INPUT_TOPIC_NAME_PROP);

      final JsonSerdeV2<PageView> serde = new JsonSerdeV2<>(PageView.class);
      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(SYSTEM);
      KafkaInputDescriptor<PageView> isd = ksd.getInputDescriptor(inputTopic, serde);
      appDescriptor.addInputStream(isd);
      appDescriptor.setTaskFactory((StreamTaskFactory) () -> new FaultInjectionTask(containerShutdownLatch));
    }

    private static class FaultInjectionTask implements StreamTask, ClosableTask {
      private final transient CountDownLatch containerShutdownLatch;

      public FaultInjectionTask(CountDownLatch containerShutdownLatch) {
        this.containerShutdownLatch = containerShutdownLatch;
      }

      @Override
      public void close() throws Exception {
        containerShutdownLatch.countDown();
      }

      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
          throws Exception {
        throw new RuntimeException("Failed");
      }
    }
  }
}
