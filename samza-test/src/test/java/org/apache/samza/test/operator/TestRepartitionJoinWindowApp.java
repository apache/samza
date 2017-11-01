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
import java.util.HashSet;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.Partition;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.samza.test.operator.RepartitionJoinWindowApp.AD_CLICKS;
import static org.apache.samza.test.operator.RepartitionJoinWindowApp.OUTPUT_TOPIC;
import static org.apache.samza.test.operator.RepartitionJoinWindowApp.PAGE_VIEWS;


/**
 * Test driver for {@link RepartitionJoinWindowApp}.
 */
public class TestRepartitionJoinWindowApp extends StreamApplicationIntegrationTestHarness {

  @Before
  public void setup() {
    // create topics
    createTopic(PAGE_VIEWS, 2);
    createTopic(AD_CLICKS, 2);
    createTopic(OUTPUT_TOPIC, 1);

    // create events for the following user activity.
    // userId: (viewId, pageId, (adIds))
    // u1: (v1, p1, (a1)), (v2, p2, (a3))
    // u2: (v3, p1, (a1)), (v4, p3, (a5))
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v1\",\"pageId\":\"p1\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 1, "p2", "{\"viewId\":\"v2\",\"pageId\":\"p2\",\"userId\":\"u1\"}");
    produceMessage(PAGE_VIEWS, 0, "p1", "{\"viewId\":\"v3\",\"pageId\":\"p1\",\"userId\":\"u2\"}");
    produceMessage(PAGE_VIEWS, 1, "p3", "{\"viewId\":\"v4\",\"pageId\":\"p3\",\"userId\":\"u2\"}");

    produceMessage(AD_CLICKS, 0, "a1", "{\"viewId\":\"v1\",\"adId\":\"a1\"}");
    produceMessage(AD_CLICKS, 0, "a3", "{\"viewId\":\"v2\",\"adId\":\"a3\"}");
    produceMessage(AD_CLICKS, 0, "a1", "{\"viewId\":\"v3\",\"adId\":\"a1\"}");
    produceMessage(AD_CLICKS, 0, "a5", "{\"viewId\":\"v4\",\"adId\":\"a5\"}");

  }

  @Test
  public void testRepartitionJoinWindowAppAndDeleteMessagesOnCommit() throws Exception {
    // run the application
    RepartitionJoinWindowApp app = new RepartitionJoinWindowApp();
    final String appName = "UserPageAdClickCounter";
    runApplication(app, appName, null);

    // consume and validate result
    List<ConsumerRecord<String, String>> messages = consumeMessages(Collections.singletonList(OUTPUT_TOPIC), 2);
    Assert.assertEquals(2, messages.size());

    for (ConsumerRecord<String, String> message : messages) {
      String key = message.key();
      String value = message.value();
      Assert.assertTrue(key.equals("u1") || key.equals("u2"));
      Assert.assertEquals("2", value);
    }

    // Verify that messages in the intermediate stream will be deleted in 10 seconds
    long startTimeMs = System.currentTimeMillis();
    for (StreamSpec spec: runner.getIntermediateStreams(app)) {
      long remainingMessageNum = -1;

      while (remainingMessageNum != 0 && System.currentTimeMillis() - startTimeMs < 10000) {
        remainingMessageNum = 0;
        SystemStreamMetadata metadatas = systemAdmin.getSystemStreamMetadata(
            new HashSet<>(Arrays.asList(spec.getPhysicalName())), new ExponentialSleepStrategy.Mock(3)
        ).get(spec.getPhysicalName()).get();

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
    runApplication(new BroadcastAssertApp(), "BroadcastTest", null);
  }
}
