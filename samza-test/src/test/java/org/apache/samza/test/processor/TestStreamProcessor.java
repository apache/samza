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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.task.AsyncStreamTaskAdapter;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.junit.Assert;
import org.junit.Test;
import scala.Option$;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;


public class TestStreamProcessor extends StandaloneIntegrationTestHarness {
  /**
   * Testing a basic identity stream task - reads data from a topic and writes it to another topic
   * (without any modifications)
   *
   * <p>
   * The standalone version in this test uses KafkaSystemFactory and it uses a SingleContainerGrouperFactory. Hence,
   * no matter how many tasks are present, it will always be run in a single processor instance. This simplifies testing
   */
  @Test
  public void testStreamProcessor() {
    final String testSystem = "test-system";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 20;

    final Config configs = new MapConfig(createConfigs("1", testSystem, inputTopic, outputTopic, messageCount));
    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);
    final Mocks mocks = new Mocks(configs, IdentityStreamTask::new, bootstrapServers());

    produceMessages(mocks.producer, inputTopic, messageCount);
    run(mocks.processor, mocks.latch);
    verifyNumMessages(mocks.consumer, outputTopic, messageCount);
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

    final Config configs = new MapConfig(createConfigs("1", testSystem, inputTopic, outputTopic, messageCount));
    createTopics(inputTopic, outputTopic);
    final Mocks mocks = new Mocks(configs, IdentityStreamTask::new, bootstrapServers());

    produceMessages(mocks.producer, inputTopic, messageCount);
    run(mocks.processor, mocks.latch);
    verifyNumMessages(mocks.consumer, outputTopic, messageCount);
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

    final Config configs = new MapConfig(createConfigs("1", testSystem, inputTopic, outputTopic, messageCount));
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    createTopics(inputTopic, outputTopic);
    final AsyncStreamTaskFactory stf = () -> new AsyncStreamTaskAdapter(new IdentityStreamTask(), executorService);
    final Mocks mocks = new Mocks(configs, stf, bootstrapServers());

    produceMessages(mocks.producer, inputTopic, messageCount);
    run(mocks.processor, mocks.latch);
    verifyNumMessages(mocks.consumer, outputTopic, messageCount);
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

    final Map<String, String> configMap = createConfigs("1", testSystem, inputTopic, outputTopic, messageCount);
    configMap.remove("task.class");
    final Config configs = new MapConfig(configMap);
    final Mocks mocks = new Mocks(configs, (StreamTaskFactory) null, bootstrapServers());

    run(mocks.processor, mocks.latch);
  }

  private void createTopics(String inputTopic, String outputTopic) {
    TestUtils.createTopic(zkUtils(), inputTopic, 1, 1, servers(), new Properties());
    TestUtils.createTopic(zkUtils(), outputTopic, 1, 1, servers(), new Properties());
  }

  private Map<String, String> createConfigs(String processorId, String testSystem, String inputTopic, String outputTopic, int messageCount) {
    Map<String, String> configs = new HashMap<>();
    configs.putAll(
        StandaloneTestUtils.getStandaloneConfigs("test-job", "org.apache.samza.test.processor.IdentityStreamTask"));
    configs.putAll(StandaloneTestUtils.getKafkaSystemConfigs(testSystem, bootstrapServers(), zkConnect(), null,
            StandaloneTestUtils.SerdeAlias.STRING, true));
    configs.put("task.inputs", String.format("%s.%s", testSystem, inputTopic));
    configs.put("app.messageCount", String.valueOf(messageCount));
    configs.put("app.outputTopic", outputTopic);
    configs.put("app.outputSystem", testSystem);
    configs.put(ZkConfig.ZK_CONNECT, zkConnect());
    configs.put("processor.id", processorId);
    return configs;
  }

  /**
   * Produces the provided number of messages to the topic.
   */
  @SuppressWarnings("unchecked")
  private void produceMessages(KafkaProducer producer, String topic, int numMessages) {
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
  @SuppressWarnings("unchecked")
  private void verifyNumMessages(KafkaConsumer consumer, String topic, int expectedNumMessages) {
    consumer.subscribe(Collections.singletonList(topic));

    int count = 0;
    int emptyPollCount = 0;

    while (count < expectedNumMessages && emptyPollCount < 5) {
      ConsumerRecords records = consumer.poll(5000);
      if (!records.isEmpty()) {
        for (ConsumerRecord record : (Iterable<ConsumerRecord>) records) {
          Assert.assertEquals(new String((byte[]) record.value()), String.valueOf(count));
          count++;
        }
      } else {
        emptyPollCount++;
      }
    }

    Assert.assertEquals(count, expectedNumMessages);
  }

  private static class Mocks {
    CountDownLatch latch;
    KafkaConsumer consumer;
    KafkaProducer producer;
    StreamProcessor processor;
    StreamProcessorLifecycleListener listener;

    private Mocks(String bootstrapServer) {
      latch = new CountDownLatch(1);
      initProcessListener();
      initConsumer(bootstrapServer);
      initProducer(bootstrapServer);
    }

    Mocks(Config config, StreamTaskFactory taskFactory, String bootstrapServer) {
      this(bootstrapServer);
      processor = new StreamProcessor(config, new HashMap<>(), taskFactory, listener);
    }

    Mocks(Config config, AsyncStreamTaskFactory taskFactory, String bootstrapServer) {
      this(bootstrapServer);
      processor = new StreamProcessor(config, new HashMap<>(), taskFactory, listener);
    }

    private void initConsumer(String bootstrapServer) {
      consumer = TestUtils.createNewConsumer(
          bootstrapServer,
          "group",
          "earliest",
          4096L,
          "org.apache.kafka.clients.consumer.RangeAssignor",
          30000,
          SecurityProtocol.PLAINTEXT,
          Option$.MODULE$.empty(),
          Option$.MODULE$.empty(),
          Option$.MODULE$.empty());
    }

    private void initProcessListener() {
      listener = mock(StreamProcessorLifecycleListener.class);
      doNothing().when(listener).onStart();
      doNothing().when(listener).onFailure(anyObject());
      doAnswer(invocation -> {
          latch.countDown();
          return null;
        }).when(listener).onShutdown();
    }

    private void initProducer(String bootstrapServer) {
      producer = TestUtils.createNewProducer(
          bootstrapServer,
          1,
          60 * 1000L,
          1024L * 1024L,
          0,
          0L,
          5 * 1000L,
          SecurityProtocol.PLAINTEXT,
          null,
          Option$.MODULE$.apply(new Properties()),
          new StringSerializer(),
          new ByteArraySerializer(),
          Option$.MODULE$.apply(new Properties()));
    }
  }
}
