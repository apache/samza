/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestKafkaConsumerConfig {
  public static final String JOB_NAME = "jobName";
  public static final String JOB_ID = "jobId";
  private final Map<String, String> props = new HashMap<>();
  public final static String SYSTEM_NAME = "testSystem";
  public final static String KAFKA_PRODUCER_PROPERTY_PREFIX = "systems." + SYSTEM_NAME + ".producer.";
  public final static String KAFKA_CONSUMER_PROPERTY_PREFIX = "systems." + SYSTEM_NAME + ".consumer.";
  private final static String ID_PREFIX = "ID_PREFIX";

  @Before
  public void setProps() {
    props.put(JobConfig.JOB_NAME(), JOB_NAME);
    props.put(JobConfig.JOB_ID(), JOB_ID);
  }

  @Test
  public void testDefaultsAndOverrides() {

    Map<String, String> overrides = new HashMap<>();
    overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // should be ignored
    overrides.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "Ignore"); // should be ignored
    overrides.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100"); // should NOT be ignored

    // if KAFKA_CONSUMER_PROPERTY_PREFIX is set, then PRODUCER should be ignored
    props.put(KAFKA_PRODUCER_PROPERTY_PREFIX + "bootstrap.servers", "ignroeThis:9092");
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + "bootstrap.servers", "useThis:9092");

    // should be overridden
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "true"); //ignore
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000"); // ignore


    // should be overridden
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "200");

    Config config = new MapConfig(props);
    KafkaConsumerConfig kafkaConsumerConfig = KafkaConsumerConfig.getKafkaSystemConsumerConfig(
        config, SYSTEM_NAME, ID_PREFIX, overrides);

    Assert.assertEquals(kafkaConsumerConfig.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), false);

    Assert.assertEquals(
        kafkaConsumerConfig.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
        Integer.valueOf(KafkaConsumerConfig.DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS));

    Assert.assertEquals(
        kafkaConsumerConfig.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG).get(0),
        RangeAssignor.class.getName());

    Assert.assertEquals(
        kafkaConsumerConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).get(0),
        "useThis:9092");
    Assert.assertEquals(
        kafkaConsumerConfig.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG).longValue(),
        100);

    Assert.assertEquals(
        kafkaConsumerConfig.getClass(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG),
        ByteArrayDeserializer.class);

    Assert.assertEquals(
        kafkaConsumerConfig.getClass(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG),
        ByteArrayDeserializer.class);

    String expectedClientId = String.format("%s-%s-%s", ID_PREFIX, JOB_NAME, JOB_ID);
    Assert.assertEquals(
        expectedClientId,
        kafkaConsumerConfig.getString(ConsumerConfig.CLIENT_ID_CONFIG));

    String expectedGroupId = String.format("%s-%s", JOB_NAME, JOB_ID);
    Assert.assertEquals(
        expectedGroupId,
        kafkaConsumerConfig.getString(ConsumerConfig.GROUP_ID_CONFIG));
  }

  @Test
  // test stuff that should not be overridden
  public void testNotOverride() {

    // if KAFKA_CONSUMER_PROPERTY_PREFIX is not set, then PRODUCER should be used
    props.put(KAFKA_PRODUCER_PROPERTY_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "useThis:9092");
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, TestKafkaConsumerConfig.class.getName());
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TestKafkaConsumerConfig.class.getName());


    Config config = new MapConfig(props);
    KafkaConsumerConfig kafkaConsumerConfig = KafkaConsumerConfig.getKafkaSystemConsumerConfig(
        config, SYSTEM_NAME, ID_PREFIX, Collections.emptyMap());

    Assert.assertEquals(
        kafkaConsumerConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).get(0),
        "useThis:9092");

    Assert.assertEquals(
        kafkaConsumerConfig.getClass(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG),
        TestKafkaConsumerConfig.class);

    Assert.assertEquals(
        kafkaConsumerConfig.getClass(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG),
        TestKafkaConsumerConfig.class);
  }



  @Test(expected = SamzaException.class)
  public void testNoBootstrapServers() {
    KafkaConsumerConfig kafkaConsumerConfig = KafkaConsumerConfig.getKafkaSystemConsumerConfig(
        new MapConfig(Collections.emptyMap()), SYSTEM_NAME, "clientId", Collections.emptyMap());

    Assert.fail("didn't get exception for the missing config:" + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
  }
}
