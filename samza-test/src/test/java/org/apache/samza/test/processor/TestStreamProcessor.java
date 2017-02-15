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

package org.apache.samza.test.processor;

import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.task.AsyncStreamTaskAdapter;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.samza.test.processor.IdentityStreamTask.endLatch;

public class TestStreamProcessor extends StandaloneIntegrationTestHarness {
  /**
   * Testing a basic identity stream task - reads data from a topic and writes it to another topic
   * (without any modifications)
   *
   * The standalone version in this test uses KafkaSystemFactory and it uses a SingleContainerGrouperFactory. Hence,
   * no matter how many tasks are present, it will always be run in a single processor instance. This simplifies testing
   */
  @Test
  public void testStreamProcessor() {
    final String testSystem = "test-system";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 20;

    final Config configs = new MapConfig(createConfigs(testSystem, inputTopic, outputTopic, messageCount));
    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);
    final StreamProcessor processor = new StreamProcessor(1, new MapConfig(configs), null, new HashMap<>());

    produceMessages(inputTopic, messageCount);
    run(processor, endLatch);
    verifyNumMessages(outputTopic, messageCount);
  }

  /**
   * Should be able to create task instances from the provided task factory.
   */
  @Test
  public void testStreamProcessorWithStreamTaskFactory() {
    final String testSystem = "test-system";
    final String inputTopic = "numbers2";
    final String outputTopic = "output2";
    final int messageCount = 20;

    final Config configs = new MapConfig(createConfigs(testSystem, inputTopic, outputTopic, messageCount));
    createTopics(inputTopic, outputTopic);
    final StreamTaskFactory stf = IdentityStreamTask::new;
    final StreamProcessor processor = new StreamProcessor(1, configs, null, new HashMap<>(), stf);

    produceMessages(inputTopic, messageCount);
    run(processor, endLatch);
    verifyNumMessages(outputTopic, messageCount);
  }

  /**
   * Should be able to create task instances from the provided task factory.
   */
  @Test
  public void testStreamProcessorWithAsyncStreamTaskFactory() {
    final String testSystem = "test-system";
    final String inputTopic = "numbers3";
    final String outputTopic = "output3";
    final int messageCount = 20;

    final Config configs = new MapConfig(createConfigs(testSystem, inputTopic, outputTopic, messageCount));
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    createTopics(inputTopic, outputTopic);
    final AsyncStreamTaskFactory stf = () -> new AsyncStreamTaskAdapter(new IdentityStreamTask(), executorService);
    final StreamProcessor processor = new StreamProcessor(1, configs, null, new HashMap<>(), stf);

    produceMessages(inputTopic, messageCount);
    run(processor, endLatch);
    verifyNumMessages(outputTopic, messageCount);
    executorService.shutdownNow();
  }

  /**
   * Should fail to create a SamzaContainer when neither task factory nor task.class are provided.
   */
  @Test(expected = SamzaException.class)
  public void testStreamProcessorWithNoTask() {
    final String testSystem = "test-system";
    final String inputTopic = "numbers4";
    final String outputTopic = "output4";
    final int messageCount = 20;

    final Map<String, String> configMap = createConfigs(testSystem, inputTopic, outputTopic, messageCount);
    configMap.remove("task.class");
    final Config configs = new MapConfig(configMap);

    StreamProcessor processor = new StreamProcessor(1, configs, null, new HashMap<>());
    run(processor, endLatch);
  }

  private void createTopics(String inputTopic, String outputTopic) {
    TestUtils.createTopic(zkUtils(), inputTopic, 1, 1, servers(), new Properties());
    TestUtils.createTopic(zkUtils(), outputTopic, 1, 1, servers(), new Properties());
  }

  private Map<String, String> createConfigs(String testSystem, String inputTopic, String outputTopic, int messageCount) {
    Map<String, String> configs = new HashMap<>();
    configs.putAll(
        StandaloneTestUtils.getStandaloneConfigs(
            "test-job",
            "org.apache.samza.test.processor.IdentityStreamTask"));
    configs.putAll(
        StandaloneTestUtils.getKafkaSystemConfigs(
            testSystem,
            bootstrapServers(),
            zkConnect(),
            null,
            StandaloneTestUtils.SerdeAlias.STRING,
            true));
    configs.put("task.inputs", String.format("%s.%s", testSystem, inputTopic));
    configs.put("app.messageCount", String.valueOf(messageCount));
    configs.put("app.outputTopic", outputTopic);
    configs.put("app.outputSystem", testSystem);
    return configs;
  }

  /**
   * Produces the provided number of messages to the topic.
   */
  private void produceMessages(String topic, int numMessages) {
    KafkaProducer producer = getKafkaProducer();
    for (int i = 0; i < numMessages; i++) {
      try {
        producer.send(new ProducerRecord(topic, String.valueOf(i).getBytes())).get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Runs the provided stream processor by starting it, waiting on the provided latch with a timeout,
   * and then stopping it.
   */
  private void run(StreamProcessor processor, CountDownLatch latch) {
    boolean latchResult = false;
    processor.start();
    try {
      processor.awaitStart(10000);
      latchResult = latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (!latchResult) {
        Assert.fail("StreamTask either failed to process all input or timed out!");
      }
    }
    processor.stop();
  }

  /**
   * Consumes data from the topic until there are no new messages for a while
   * and asserts that the number of consumed messages is as expected.
   */
  private void verifyNumMessages(String topic, int expectedNumMessages) {
    KafkaConsumer consumer = getKafkaConsumer();
    consumer.subscribe(Collections.singletonList(topic));

    int count = 0;
    int emptyPollCount = 0;

    while (count < expectedNumMessages && emptyPollCount < 5) {
      ConsumerRecords records = consumer.poll(5000);
      if (!records.isEmpty()) {
        Iterator<ConsumerRecord> iterator = records.iterator();
        while (iterator.hasNext()) {
          ConsumerRecord record = iterator.next();
          Assert.assertEquals(new String((byte[]) record.value()), String.valueOf(count));
          count++;
        }
      } else {
        emptyPollCount++;
      }
    }

    Assert.assertEquals(count, expectedNumMessages);
  }
}
