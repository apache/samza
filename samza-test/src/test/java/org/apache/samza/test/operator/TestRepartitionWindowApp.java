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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.test.operator.data.PageView;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.operator.RepartitionWindowApp.*;

/**
 * Test driver for {@link RepartitionWindowApp}.
 */
public class TestRepartitionWindowApp extends StreamApplicationIntegrationTestHarness {

  static final String APP_NAME = "PageViewCounterApp";

  @Test
  public void testRepartitionedSessionWindowCounter() throws Exception {
    // create topics
    createTopic(INPUT_TOPIC, 3);
    createTopic(OUTPUT_TOPIC, 1);

    // produce messages to different partitions.
    ObjectMapper mapper = new ObjectMapper();
    PageView pv = new PageView("india", "5.com", "userId1");
    produceMessage(INPUT_TOPIC, 0, "userId1", mapper.writeValueAsString(pv));
    pv = new PageView("china", "4.com", "userId2");
    produceMessage(INPUT_TOPIC, 1, "userId2", mapper.writeValueAsString(pv));
    pv = new PageView("india", "1.com", "userId1");
    produceMessage(INPUT_TOPIC, 2, "userId1", mapper.writeValueAsString(pv));
    pv = new PageView("india", "2.com", "userId1");
    produceMessage(INPUT_TOPIC, 0, "userId1", mapper.writeValueAsString(pv));
    pv = new PageView("india", "3.com", "userId1");
    produceMessage(INPUT_TOPIC, 1, "userId1", mapper.writeValueAsString(pv));

    Map<String, String> configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, "org.apache.samza.standalone.PassthroughCoordinationUtilsFactory");
    configs.put(TaskConfig.GROUPER_FACTORY(), "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");
    configs.put(String.format("streams.%s.samza.msg.serde", INPUT_TOPIC), "string");
    configs.put(String.format("streams.%s.samza.key.serde", INPUT_TOPIC), "string");

    // run the application
    Thread runThread = runApplication(RepartitionWindowApp.class.getName(), APP_NAME, new MapConfig(configs)).getRunThread();

    // consume and validate result
    List<ConsumerRecord<String, String>> messages = consumeMessages(Collections.singletonList(OUTPUT_TOPIC), 2);
    Assert.assertEquals(messages.size(), 2);

    for (ConsumerRecord<String, String> message : messages) {
      String key = message.key();
      String value = message.value();
      // Assert that there are 4 messages for userId1 and 1 message for userId2.
      Assert.assertTrue(key.equals("userId1") || key.equals("userId2"));
      if ("userId1".equals(key)) {
        Assert.assertEquals(value, "4");
      } else {
        Assert.assertEquals(value, "1");
      }
    }

    runThread.interrupt();
    runThread.join();
  }
}
