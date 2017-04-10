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

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * Test driver for {@link SessionWindowApp}.
 */
public class TestSessionWindowApp extends StreamApplicationIntegrationTestHarness {

  static final String INPUT_TOPIC = "page-views";
  static final String OUTPUT_TOPIC = "Result";
  static final long WINDOW_GAP = Duration.ofSeconds(3).toMillis();

  private static final String APP_NAME = "SessionWindowDemo";

  @Test
  public void testSessionWindowCounter() throws Exception {
    // create topics
    createTopic(INPUT_TOPIC, 1);
    createTopic(OUTPUT_TOPIC, 1);

    long startTime = System.currentTimeMillis();
    // Produce messages to the same partition. (intersperse them with messages with badKeys to be
    // filtered out.)
    produceMessage(INPUT_TOPIC, 0, "badKey", "badKey,india,google.com");
    produceMessage(INPUT_TOPIC, 0, "userId2", "userId2,china,yahoo.com");
    produceMessage(INPUT_TOPIC, 0, "userId1", "badKey,india,google.com");
    produceMessage(INPUT_TOPIC, 0, "userId1", "userId1,india,hotmail.com");
    produceMessage(INPUT_TOPIC, 0, "badKey", "badKey,india,google.com");
    produceMessage(INPUT_TOPIC, 0, "userId1", "userId1,india,hotmail.com");
    produceMessage(INPUT_TOPIC, 0, "userId3", "userId3,usa,hotmail.com");
    produceMessage(INPUT_TOPIC, 0, "badKey", "badKey,india,google.com");

    // run the application
    SessionWindowApp app = new SessionWindowApp();
    runApplication(app, APP_NAME, null);

    // consume and validate result
    List<ConsumerRecord<String, String>> messages = consumeMessages(Collections.singletonList(OUTPUT_TOPIC), 3);
    Assert.assertTrue(System.currentTimeMillis() - startTime >= WINDOW_GAP);
    Assert.assertEquals(messages.size(), 3);

    for (ConsumerRecord<String, String> message : messages) {
      String key = message.key();
      String value = message.value();
      // Assert that "badKey" messages were actually filtered out
      Assert.assertTrue(key.equals("userId1") || key.equals("userId2") || key.equals("userId3"));

      // Assert that there are 2 messages for userId1, 1 message each for userId2 and userId3
      if ("userId1".equals(key)) {
        Assert.assertEquals(value, "2");
      } else {
        Assert.assertEquals(value, "1");
      }
    }
  }
}
