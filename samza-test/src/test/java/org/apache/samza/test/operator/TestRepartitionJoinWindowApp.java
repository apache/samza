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
package org.apache.samza.test.operator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.Partition;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.kafka.KafkaSystemAdmin;
import org.apache.samza.test.framework.BroadcastAssertApp;
import org.apache.samza.test.framework.StreamApplicationIntegrationTestHarness;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test driver for {@link RepartitionJoinWindowApp}.
 */
public class TestRepartitionJoinWindowApp extends StreamApplicationIntegrationTestHarness {

  void initializeTopics(String input1, String input2, String output) {
    // create topics
    createTopic(input1, 2);
    createTopic(input2, 2);
    createTopic(output, 1);

    // create events for the following user activity.
    // userId: (viewId, pageId, (adIds))
    // u1: (v1, p1, (a1)), (v2, p2, (a3))
    // u2: (v3, p1, (a1)), (v4, p3, (a5))
    produceMessage(input1, 0, "p1", "{\"viewId\":\"v1\",\"pageId\":\"p1\",\"userId\":\"u1\"}");
    produceMessage(input1, 1, "p2", "{\"viewId\":\"v2\",\"pageId\":\"p2\",\"userId\":\"u1\"}");
    produceMessage(input1, 0, "p1", "{\"viewId\":\"v3\",\"pageId\":\"p1\",\"userId\":\"u2\"}");
    produceMessage(input1, 1, "p3", "{\"viewId\":\"v4\",\"pageId\":\"p3\",\"userId\":\"u2\"}");

    produceMessage(input2, 0, "a1", "{\"viewId\":\"v1\",\"adId\":\"a1\"}");
    produceMessage(input2, 0, "a3", "{\"viewId\":\"v2\",\"adId\":\"a3\"}");
    produceMessage(input2, 0, "a1", "{\"viewId\":\"v3\",\"adId\":\"a1\"}");
    produceMessage(input2, 0, "a5", "{\"viewId\":\"v4\",\"adId\":\"a5\"}");
  }

  @Test
  public void testRepartitionJoinWindowAppWithoutDeletionOnCommit() throws Exception {
    String inputTopicName1 = "page-views";
    String inputTopicName2 = "ad-clicks";
    String outputTopicName = "user-ad-click-counts";

    KafkaSystemAdmin.deleteMessagesCalled_$eq(false);

    initializeTopics(inputTopicName1, inputTopicName2, outputTopicName);

    // run the application
    RepartitionJoinWindowApp app = new RepartitionJoinWindowApp();
    String appName = "UserPageAdClickCounter";
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.kafka.samza.delete.committed.messages", "false");
    configs.put(RepartitionJoinWindowApp.INPUT_TOPIC_NAME_1_PROP, inputTopicName1);
    configs.put(RepartitionJoinWindowApp.INPUT_TOPIC_NAME_2_PROP, inputTopicName2);
    configs.put(RepartitionJoinWindowApp.OUTPUT_TOPIC_NAME_PROP, outputTopicName);

    runApplication(app, appName, configs);

    // consume and validate result
    List<ConsumerRecord<String, String>> messages = consumeMessages(Collections.singletonList(outputTopicName), 2);
    Assert.assertEquals(2, messages.size());

    Assert.assertFalse(KafkaSystemAdmin.deleteMessagesCalled());
  }

  @Test
  public void testRepartitionJoinWindowAppAndDeleteMessagesOnCommit() throws Exception {
    String inputTopicName1 = "page-views2";
    String inputTopicName2 = "ad-clicks2";
    String outputTopicName = "user-ad-click-counts2";

    initializeTopics(inputTopicName1, inputTopicName2, outputTopicName);

    // run the application
    RepartitionJoinWindowApp app = new RepartitionJoinWindowApp();
    final String appName = "UserPageAdClickCounter2";
    Map<String, String> configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, "org.apache.samza.standalone.PassthroughCoordinationUtilsFactory");
    configs.put(TaskConfig.GROUPER_FACTORY(), "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");
    configs.put("systems.kafka.samza.delete.committed.messages", "true");
    configs.put(RepartitionJoinWindowApp.INPUT_TOPIC_NAME_1_PROP, inputTopicName1);
    configs.put(RepartitionJoinWindowApp.INPUT_TOPIC_NAME_2_PROP, inputTopicName2);
    configs.put(RepartitionJoinWindowApp.OUTPUT_TOPIC_NAME_PROP, outputTopicName);

    runApplication(app, appName, configs);

    // consume and validate result
    List<ConsumerRecord<String, String>> messages = consumeMessages(Collections.singletonList(outputTopicName), 2);
    Assert.assertEquals(2, messages.size());

    for (ConsumerRecord<String, String> message : messages) {
      String key = message.key();
      String value = message.value();
      Assert.assertTrue(key.equals("u1") || key.equals("u2"));
      Assert.assertEquals("2", value);
    }

    // Verify that messages in the intermediate stream will be deleted in 10 seconds
    long startTimeMs = System.currentTimeMillis();
    for (String streamId: app.getIntermediateStreamIds()) {
      long remainingMessageNum = -1;

      while (remainingMessageNum != 0 && System.currentTimeMillis() - startTimeMs < 10000) {
        remainingMessageNum = 0;
        SystemStreamMetadata metadatas = systemAdmin.getSystemStreamMetadata(
            new HashSet<>(Arrays.asList(streamId)), new ExponentialSleepStrategy.Mock(3)
        ).get(streamId).get();

        for (Map.Entry<Partition, SystemStreamPartitionMetadata> entry : metadatas.getSystemStreamPartitionMetadata().entrySet()) {
          SystemStreamPartitionMetadata metadata = entry.getValue();
          remainingMessageNum += Long.parseLong(metadata.getUpcomingOffset()) - Long.parseLong(metadata.getOldestOffset());
        }
      }
      Assert.assertEquals(0, remainingMessageNum);
    }
  }

  @Test
  public void testBroadcastApp() {
    String inputTopicName1 = "page-views";
    String inputTopicName2 = "ad-clicks";
    String outputTopicName = "user-ad-click-counts";
    Map<String, String> configs = new HashMap<>();
    configs.put(BroadcastAssertApp.INPUT_TOPIC_NAME_PROP, inputTopicName1);

    initializeTopics(inputTopicName1, inputTopicName2, outputTopicName);
    runApplication(new BroadcastAssertApp(), "BroadcastTest", configs);
  }
}
