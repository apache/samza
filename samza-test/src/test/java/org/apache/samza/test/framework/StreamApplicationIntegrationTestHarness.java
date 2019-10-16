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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.execution.TestStreamManager;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.test.harness.IntegrationTestHarness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;


/**
 * Harness for writing integration tests for {@link SamzaApplication}s.
 *
 * <p> This provides the following features for its sub-classes:
 * <ul>
 *   <li>
 *     Automatic Setup and teardown: Any non-trivial integration test brings up components like Zookeeper
 *     servers, Kafka brokers, Kafka producers and Kafka consumers. This harness initializes each
 *     of these components in {@link #setUp()} and shuts down each of them cleanly in {@link #tearDown()}.
 *     {@link #setUp()} and {@link #tearDown()} are automatically invoked from the Junit runner.
 *   <li>
 *     Interaction with Kafka: The harness provides convenience methods to interact with Kafka brokers, consumers
 *     and producers - for instance, methods to create topics, produce and consume messages.
 *   <li>
 *     Config defaults: Often Samza integration tests have to setup config boiler plate
 *     to perform even simple tasks like producing and consuming messages. This harness provides default string
 *     serdes for producing / consuming messages and a default system-alias named "kafka" that uses the
 *     {@link org.apache.samza.system.kafka.KafkaSystemFactory}.
 *   <li>
 *     Debugging: At times, it is convenient to debug integration tests locally from an IDE. This harness
 *     runs all its components (including Kafka brokers, Zookeeper servers and Samza) locally.
 * </ul>
 *
 * <p> <i> Implementation Notes: </i> <br/>
 * State persistence: {@link #tearDown()} clears all associated state (including topics and metadata) in Kafka and
 * Zookeeper. Hence, the state is not durable across invocations of {@link #tearDown()} <br/>
 *
 * Execution model: {@link SamzaApplication}s are run as their own {@link org.apache.samza.job.local.ThreadJob}s.
 * Similarly, embedded Kafka servers and Zookeeper servers are run as their own threads.
 * {@link #produceMessage(String, int, String, String)} and {@link #consumeMessages(Collection, int)} are blocking calls.
 *
 * <h3>Usage Example</h3>
 * Here is an actual test that publishes a message into Kafka, runs an application, and verifies consumption
 * from the output topic.
 *
 *  <pre> {@code
 * class MyTest extends StreamApplicationIntegrationTestHarness {
 *   private final StreamApplication myApp = new MyStreamApplication();
 *   private final Collection<String> outputTopics = Collections.singletonList("output-topic");
 *   @Test
 *   public void test() {
 *     createTopic("mytopic", 1);
 *     produceMessage("mytopic", 0, "key1", "val1");
 *     runApplication(myApp, "myApp", null);
 *     List<ConsumerRecord<String, String>> messages = consumeMessages(outputTopics)
 *     Assert.assertEquals(messages.size(), 1);
 *   }
 * }}</pre>
 */
public class StreamApplicationIntegrationTestHarness extends IntegrationTestHarness {
  private static final Logger LOG = LoggerFactory.getLogger(StreamApplicationIntegrationTestHarness.class);
  private static final Duration POLL_TIMEOUT_MS = Duration.ofSeconds(20);
  private static final int DEFAULT_REPLICATION_FACTOR = 1;
  private int numEmptyPolls = 3;

  /**
   * Creates a kafka topic with the provided name and the number of partitions
   * @param topicName the name of the topic
   * @param numPartitions the number of partitions in the topic
   */
  public boolean createTopic(String topicName, int numPartitions) {
    return createTopic(topicName, numPartitions, DEFAULT_REPLICATION_FACTOR);
  }

  /**
   * Produces a message to the provided topic partition.
   * @param topicName the topic to produce messages to
   * @param partitionId the topic partition to produce messages to
   * @param key the key in the message
   * @param val the value in the message
   */
  public void produceMessage(String topicName, int partitionId, String key, String val) {
    producer.send(new ProducerRecord<>(topicName, partitionId, key, val));
    producer.flush();
    LOG.info("Sent key: {} val: {} to topic: {} on partition: {}", key, val, topicName, partitionId);
  }

  @Override
  public int clusterSize() {
    return Integer.parseInt(KafkaConfig.TOPIC_DEFAULT_REPLICATION_FACTOR());
  }


  /**
   * Read messages from the provided list of topics until {@param threshold} messages have been read or until
   * {@link #numEmptyPolls} polls return no messages.
   *
   * The default poll time out is determined by {@link #POLL_TIMEOUT_MS} and the number of empty polls are
   * determined by {@link #numEmptyPolls}
   *
   * @param topics the list of topics to consume from
   * @param threshold the number of messages to consume
   * @return the list of {@link ConsumerRecord}s whose size can be atmost {@param threshold}
   */
  public List<ConsumerRecord<String, String>> consumeMessages(Collection<String> topics, int threshold) {
    int emptyPollCount = 0;
    List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
    consumer.subscribe(topics);

    while (emptyPollCount < numEmptyPolls && recordList.size() < threshold) {
      ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT_MS);
      LOG.info("Read {} messages from topics: {}", records.count(), StringUtils.join(topics, ","));
      if (!records.isEmpty()) {
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        while (iterator.hasNext() && recordList.size() < threshold) {
          ConsumerRecord record = iterator.next();
          LOG.info("Read key: {} val: {} from topic: {} on partition: {}",
              record.key(), record.value(), record.topic(), record.partition());
          recordList.add(record);
          emptyPollCount = 0;
        }
      } else {
        emptyPollCount++;
      }
    }
    return recordList;
  }

  /**
   * Executes the provided {@link org.apache.samza.application.StreamApplication} as a {@link org.apache.samza.job.local.ThreadJob}. The
   * {@link org.apache.samza.application.StreamApplication} runs in its own separate thread.
   *
   * @param appName the name of the application
   * @param overriddenConfigs configs to override
   * @return RunApplicationContext which contains objects created within runApplication, to be used for verification
   * if necessary
   */
  protected RunApplicationContext runApplication(SamzaApplication streamApplication,
      String appName,
      Map<String, String> overriddenConfigs) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("app.runner.class", "org.apache.samza.runtime.LocalApplicationRunner");
    configMap.put("app.name", appName);
    configMap.put("app.class", streamApplication.getClass().getCanonicalName());
    configMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
    configMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
    configMap.put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configMap.put("systems.kafka.consumer.zookeeper.connect", zkConnect());
    configMap.put("systems.kafka.producer.bootstrap.servers", bootstrapUrl());
    configMap.put("systems.kafka.samza.key.serde", "string");
    configMap.put("systems.kafka.samza.msg.serde", "string");
    configMap.put("systems.kafka.samza.offset.default", "oldest");
    configMap.put("job.coordinator.system", "kafka");
    configMap.put("job.default.system", "kafka");
    configMap.put("job.coordinator.replication.factor", "1");
    configMap.put("task.window.ms", "1000");
    configMap.put("task.checkpoint.factory", TestStreamManager.MockCheckpointManagerFactory.class.getName());

    // This is to prevent tests from taking a long time to stop after they're done. The issue is that
    // tearDown currently doesn't call runner.kill(app), and shuts down the Kafka and ZK servers immediately.
    // The test process then exits, triggering the SamzaContainer shutdown hook, which in turn tries to flush any
    // store changelogs, which then get stuck trying to produce to the stopped Kafka server.
    // Calling runner.kill doesn't work since RemoteApplicationRunner creates a new ThreadJob instance when
    // kill is called. We can't use LocalApplicationRunner since ZkJobCoordinator doesn't currently create
    // changelog streams. Hence we just force an unclean shutdown here to. This _should be_ OK
    // since the test method has already executed by the time the shutdown hook is called. The side effect is
    // that buffered state (e.g. changelog contents) might not be flushed correctly after the test run.
    configMap.put("task.shutdown.ms", "1");

    if (overriddenConfigs != null) {
      configMap.putAll(overriddenConfigs);
    }

    Config config = new MapConfig(configMap);
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(streamApplication, config);
    executeRun(runner, config);

    MessageStreamAssert.waitForComplete();
    return new RunApplicationContext(runner, config);
  }

  /**
   * Container for any necessary context created during runApplication. Allows tests to access objects created within
   * runApplication in order to do verification.
   */
  protected static class RunApplicationContext {
    private final ApplicationRunner runner;
    private final Config config;

    private RunApplicationContext(ApplicationRunner runner, Config config) {
      this.runner = runner;
      this.config = config;
    }

    public ApplicationRunner getRunner() {
      return this.runner;
    }

    public Config getConfig() {
      return this.config;
    }
  }

  @Override
  protected Properties createProducerConfigs() {
    Properties producerProps = super.createProducerConfigs();
    /*
     * Stream application tests uses string serde for both key and value.
     * The default serde for the test producer in IntegrationTestHarness uses
     * byte serde for value and string for key.
     */
    producerProps.setProperty(KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
    producerProps.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
    return producerProps;
  }

  @Override
  protected Properties createConsumerConfigs() {
    Properties consumerProps = super.createConsumerConfigs();
    /*
     * Stream application tests uses string serde for both key and value.
     * The default serde for the test producer in IntegrationTestHarness uses
     * byte serde for value and string for key.
     */
    consumerProps.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
    consumerProps.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
    return consumerProps;
  }
}
