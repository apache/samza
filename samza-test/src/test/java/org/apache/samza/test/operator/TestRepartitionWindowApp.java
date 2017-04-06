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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test driver for {@link RepartitionWindowApp}.
 */
public class TestRepartitionWindowApp extends StreamApplicationIntegrationTestHarness {

  static final String INPUT_TOPIC = "page-views";
  static final String OUTPUT_TOPIC = "Result";
  private static final String APP_NAME = "Sessionizer";

  @Test
  public void test() throws Exception {
    // create topics
    createTopic(INPUT_TOPIC, 3);
    createTopic(OUTPUT_TOPIC, 1);


    // produce messages to different partitions.
    produceMessage(INPUT_TOPIC, 1, "userId1", "userId1,india,google.com");
    produceMessage(INPUT_TOPIC, 0, "userId2", "userId2,china,yahoo.com");
    produceMessage(INPUT_TOPIC, 0, "userId1", "userId1,india,hotmail.com");
    produceMessage(INPUT_TOPIC, 2, "userId1", "userId1,india,hotmail.com");
    produceMessage(INPUT_TOPIC, 0, "userId1", "userId1,india,hotmail.com");

    // run the application
    RepartitionWindowApp app = new RepartitionWindowApp();
    runApplication(app, APP_NAME, null);

    // consume and validate result
    List<ConsumerRecord<String, String>> messages = getMessages(Collections.singletonList(OUTPUT_TOPIC), 2);
    Assert.assertEquals(messages.size(), 2);

    for (ConsumerRecord<String, String> message : messages) {
      String key = message.key();
      String value = message.value();
      Assert.assertTrue(key.equals("userId1") || key.equals("userId2"));
      if ("userId1".equals(key)) {
        Assert.assertEquals(value, "4");
      } else {
        Assert.assertEquals(value, "1");
      }
    }
  }
}
