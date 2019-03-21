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

package org.apache.samza.test.harness;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.KafkaConsumerConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.kafka.KafkaSystemAdmin;
import org.apache.samza.system.kafka.KafkaSystemConsumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;


public class IntegrationTestHarness extends AbstractKafkaServerTestHarness {
  protected static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
  protected static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();
  protected static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();
  protected static final ByteArrayDeserializer BYTE_ARRAY_DESERIALIZER = new ByteArrayDeserializer();


  protected AdminClient adminClient;
  protected KafkaConsumer consumer;
  protected KafkaProducer producer;
  protected KafkaSystemAdmin systemAdmin;


  /**
   * Starts a single kafka broker, and a single embedded zookeeper server in their own threads.
   * Sub-classes should invoke {@link #zkConnect()} and {@link #bootstrapUrl()}s to
   * obtain the urls (and ports) of the started zookeeper and kafka broker.
   */
  @Override
  public void setUp() {
    super.setUp();
    producer = createProducer();
    consumer = createConsumer();

    Properties kafkaConfig = new Properties();
    kafkaConfig.setProperty("bootstrap.servers", bootstrapServers());
    adminClient = AdminClient.create(kafkaConfig);

    systemAdmin = createSystemAdmin("kafka");
    systemAdmin.start();
  }

  /**
   * Shutdown and clear Zookeeper and Kafka broker state.
   */
  @Override
  public void tearDown() {
    systemAdmin.stop();
    // Close joins on AdminClientRunnable thread and at times takes longer than the configured test timeouts resulting
    // in test failures. Forcefully closing the adminClient should notify AdminClientRunnable thread which in turn
    // closes the underlying client quietly
    adminClient.close(10000, TimeUnit.MILLISECONDS);
    consumer.close();
    producer.close();
    super.tearDown();
  }

  /**
   * Returns the bootstrap servers configuration string to be used by clients.
   *
   * @return bootstrap servers string.
   */
  public String bootstrapServers() {
    return bootstrapUrl();
  }

  public KafkaSystemAdmin createSystemAdmin(String system) {
    String kafkaConsumerPropertyPrefix = "systems." + system + ".consumer.";

    Map<String, String> map = new HashMap<>();
    map.put(kafkaConsumerPropertyPrefix + BOOTSTRAP_SERVERS_CONFIG, brokerList());
    map.put(JobConfig.JOB_NAME(), "test.job");
    map.put(kafkaConsumerPropertyPrefix + KafkaConsumerConfig.ZOOKEEPER_CONNECT, zkConnect());

    Config config = new MapConfig(map);
    HashMap<String, Object> consumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, system, KafkaConsumerConfig.createClientId("kafka-admin-consumer", config));

    return new KafkaSystemAdmin(system, new MapConfig(map), KafkaSystemConsumer.createKafkaConsumerImpl(system, consumerConfig));
  }

  protected KafkaProducer createProducer() {
    Properties kafkaConfig = new Properties();
    kafkaConfig.setProperty("bootstrap.servers", bootstrapServers());
    return new KafkaProducer<>(kafkaConfig, STRING_SERIALIZER, BYTE_ARRAY_SERIALIZER);
  }

  protected KafkaConsumer createConsumer() {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", bootstrapServers());
    consumerProps.setProperty("group.id", "group");
    consumerProps.setProperty("auto.offset.reset", "earliest");
    return new KafkaConsumer<>(consumerProps, STRING_DESERIALIZER, BYTE_ARRAY_DESERIALIZER);
  }

  protected void executeRun(ApplicationRunner applicationRunner, Config config) {
    applicationRunner.run(buildExternalContext(config).orElse(null));
  }

  protected boolean createTopic(String topicName, int numPartitions, int replicationFactor) {
    return createTopics(Collections.singleton(new NewTopic(topicName, numPartitions, (short) replicationFactor)));
  }

  protected boolean createTopics(Collection<NewTopic> newTopics) {
    boolean createStatus = true;

    try {
      CreateTopicsResult resultFuture =
          adminClient.createTopics(newTopics);
      // wait for 10 seconds to make sure the topic is created if not
      resultFuture.all().get(10000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      createStatus = false;
    }

    return createStatus;
  }

  protected boolean deleteTopics(Collection<String> topics) {
    boolean deleteStatus = true;

    try {
      DeleteTopicsResult resultFutures = adminClient.deleteTopics(topics);
      resultFutures.all().get(10000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      deleteStatus = false;
    }

    return deleteStatus;
  }

  protected CreatePartitionsResult increasePartitionsTo(String topicName, int numPartitions) {
    return adminClient.createPartitions(ImmutableMap.of(topicName, NewPartitions.increaseTo(numPartitions)));
  }

  private Optional<ExternalContext> buildExternalContext(Config config) {
    /*
     * By default, use an empty ExternalContext here. In a custom fork of Samza, this can be implemented to pass
     * a non-empty ExternalContext. Only config should be used to build the external context. In the future, components
     * like the application descriptor may not be available.
     */
    return Optional.empty();
  }
}
