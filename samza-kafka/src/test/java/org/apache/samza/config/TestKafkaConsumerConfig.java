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
package org.apache.samza.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.samza.SamzaException;
import org.junit.Assert;
import org.junit.Test;


public class TestKafkaConsumerConfig {

  public final static String SYSTEM_NAME = "testSystem";
  public final static String JOB_NAME = "jobName";
  public final static String JOB_ID = "jobId";
  public final static String KAFKA_PRODUCER_PROPERTY_PREFIX = "systems." + SYSTEM_NAME + ".producer.";
  public final static String KAFKA_CONSUMER_PROPERTY_PREFIX = "systems." + SYSTEM_NAME + ".consumer.";
  private final static String CLIENT_ID_PREFIX = "consumer-client";

  @Test
  public void testDefaults() {
    Map<String, String> props = new HashMap<>();

    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // should be ignored
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        "Ignore"); // should be ignored
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        "100"); // should NOT be ignored

    props.put(JobConfig.JOB_NAME(), JOB_NAME);

    // if KAFKA_CONSUMER_PROPERTY_PREFIX is set, then PRODUCER should be ignored
    props.put(KAFKA_PRODUCER_PROPERTY_PREFIX + "bootstrap.servers", "ignroeThis:9092");
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + "bootstrap.servers", "useThis:9092");

    Config config = new MapConfig(props);
    String clientId = KafkaConsumerConfig.createClientId(CLIENT_ID_PREFIX, config);
    KafkaConsumerConfig kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, SYSTEM_NAME, clientId);

    Assert.assertEquals("false", kafkaConsumerConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

    Assert.assertEquals(KafkaConsumerConfig.DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS,
        kafkaConsumerConfig.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));

    Assert.assertEquals(RangeAssignor.class.getName(),
        kafkaConsumerConfig.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));

    Assert.assertEquals("useThis:9092", kafkaConsumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals("100", kafkaConsumerConfig.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));

    Assert.assertEquals(ByteArrayDeserializer.class.getName(),
        kafkaConsumerConfig.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));

    Assert.assertEquals(ByteArrayDeserializer.class.getName(),
        kafkaConsumerConfig.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

    // validate group and client id generation
    Assert.assertEquals(CLIENT_ID_PREFIX.replace("-", "_") + "-" + JOB_NAME + "-" + "1",
        kafkaConsumerConfig.get(ConsumerConfig.CLIENT_ID_CONFIG));

    Assert.assertEquals(CLIENT_ID_PREFIX.replace("-", "_") + "-jobName-1",
        KafkaConsumerConfig.createClientId(CLIENT_ID_PREFIX, config));

    Assert.assertEquals("jobName-1", KafkaConsumerConfig.createConsumerGroupId(config));

    // validate setting of group and client id
    Assert.assertEquals(KafkaConsumerConfig.createConsumerGroupId(config),
        kafkaConsumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG));

    Assert.assertEquals(KafkaConsumerConfig.createConsumerGroupId(config),
        kafkaConsumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG));


    Assert.assertEquals(KafkaConsumerConfig.createClientId(CLIENT_ID_PREFIX, config),
        kafkaConsumerConfig.get(ConsumerConfig.CLIENT_ID_CONFIG));

    // with non-default job id
    props.put(JobConfig.JOB_ID(), JOB_ID);
    config = new MapConfig(props);
    Assert.assertEquals(CLIENT_ID_PREFIX.replace("-", "_") + "-jobName-jobId",
        kafkaConsumerConfig.createClientId(CLIENT_ID_PREFIX, config));

    Assert.assertEquals("jobName-jobId", KafkaConsumerConfig.createConsumerGroupId(config));

  }

  // test stuff that should not be overridden
  @Test
  public void testNotOverride() {
    Map<String, String> props = new HashMap<>();

    // if KAFKA_CONSUMER_PROPERTY_PREFIX is not set, then PRODUCER should be used
    props.put(KAFKA_PRODUCER_PROPERTY_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "useThis:9092");
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        TestKafkaConsumerConfig.class.getName());
    props.put(KAFKA_CONSUMER_PROPERTY_PREFIX + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        TestKafkaConsumerConfig.class.getName());

    props.put(JobConfig.JOB_NAME(), "jobName");

    Config config = new MapConfig(props);
    String clientId = KafkaConsumerConfig.createClientId(CLIENT_ID_PREFIX, config);
    KafkaConsumerConfig kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, SYSTEM_NAME, clientId);

    Assert.assertEquals("useThis:9092", kafkaConsumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));

    Assert.assertEquals(TestKafkaConsumerConfig.class.getName(),
        kafkaConsumerConfig.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));

    Assert.assertEquals(TestKafkaConsumerConfig.class.getName(),
        kafkaConsumerConfig.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
  }

  @Test
  public void testGetConsumerClientId() {
    Map<String, String> map = new HashMap<>();

    map.put(JobConfig.JOB_NAME(), "jobName");
    map.put(JobConfig.JOB_ID(), "jobId");

    String result = KafkaConsumerConfig.createClientId("consumer", new MapConfig(map));
    Assert.assertEquals("consumer-jobName-jobId", result);

    result = KafkaConsumerConfig.createClientId("consumer-", new MapConfig(map));
    Assert.assertEquals("consumer_-jobName-jobId", result);

    result = KafkaConsumerConfig.createClientId("super-duper-consumer", new MapConfig(map));
    Assert.assertEquals("super_duper_consumer-jobName-jobId", result);

    map.put(JobConfig.JOB_NAME(), " very important!job");
    result = KafkaConsumerConfig.createClientId("consumer", new MapConfig(map));
    Assert.assertEquals("consumer-_very_important_job-jobId", result);

    map.put(JobConfig.JOB_ID(), "number-#3");
    result = KafkaConsumerConfig.createClientId("consumer", new MapConfig(map));
    Assert.assertEquals("consumer-_very_important_job-number__3", result);
  }

  @Test(expected = SamzaException.class)
  public void testNoBootstrapServers() {
    Config config = new MapConfig(Collections.emptyMap());
    String clientId = KafkaConsumerConfig.createClientId("clientId", config);
    KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, SYSTEM_NAME, clientId);

    Assert.fail("didn't get exception for the missing config:" + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
  }

  @Test
  public void testResetValues() {
    Map<String, String> props = new HashMap<>();
    props.put(KAFKA_PRODUCER_PROPERTY_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "locahost:9092");
    props.put(JobConfig.JOB_NAME(), JOB_NAME);


    // largest -> latest
    props.put(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "largest");

    Config config = new MapConfig(props);
    KafkaConsumerConfig kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, SYSTEM_NAME, "client1");

    Assert.assertEquals("latest", kafkaConsumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));


    // smallest -> earliest
    props.put(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "smallest");

    kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(new MapConfig(props), SYSTEM_NAME, "client1");

    Assert.assertEquals("earliest", kafkaConsumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    // earliest -> earliest
    props.put(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(new MapConfig(props), SYSTEM_NAME, "client1");

    Assert.assertEquals("earliest", kafkaConsumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    // none -> none
    props.put(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "none");

    kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(new MapConfig(props), SYSTEM_NAME, "client1");

    Assert.assertEquals("none", kafkaConsumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));


    // someval -> latest
    props.put(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "someval");

    try {
      kafkaConsumerConfig =
          KafkaConsumerConfig.getKafkaSystemConsumerConfig(new MapConfig(props), SYSTEM_NAME, "client1");
      Assert.fail("Should've failed for invalid value for default offset reset");
    } catch (Exception e) {
      // expected
    }

    // no value -> latest
    props.remove(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(new MapConfig(props), SYSTEM_NAME, "client1");

    Assert.assertEquals("latest", kafkaConsumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));


    // if samza system has a reset value - use it (override kafka
    // upcoming -> latest
    props.put(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    props.put(String.format("systems.%s.samza.offset.default", SYSTEM_NAME), "upcoming");

    kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(new MapConfig(props), SYSTEM_NAME, "client1");

    Assert.assertEquals("earliest", kafkaConsumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    // stream default should override it
    props.remove(String.format("systems.%s.consumer.%s", SYSTEM_NAME, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    props.put(String.format("systems.%s.default.stream.samza.offset.default", SYSTEM_NAME), "oldest");

    kafkaConsumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(new MapConfig(props), SYSTEM_NAME, "client1");

    Assert.assertEquals("earliest", kafkaConsumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
  }

  @Test
  public void testKafkaAutoResetValue() {
    Assert.assertEquals("latest",
        KafkaConsumerConfig.getAutoOffsetResetValue("latest", "oldest"));

    try {
      KafkaConsumerConfig.getAutoOffsetResetValue("someValue", "oldest");
      Assert.fail("Invalid value should've triggered an exception");
    } catch (Exception e) {
      // expected
    }
    Assert.assertEquals("earliest",
        KafkaConsumerConfig.getAutoOffsetResetValue("earliest", "upcoming"));
    Assert.assertEquals("none",
        KafkaConsumerConfig.getAutoOffsetResetValue("none", "oldest"));
    Assert.assertEquals("latest",
        KafkaConsumerConfig.getAutoOffsetResetValue("largest", "oldest"));
    Assert.assertEquals("earliest",
        KafkaConsumerConfig.getAutoOffsetResetValue("smallest", "upcoming"));

    Assert.assertEquals("earliest",
        KafkaConsumerConfig.getAutoOffsetResetValue("", "oldest"));
    Assert.assertEquals("latest",
        KafkaConsumerConfig.getAutoOffsetResetValue("", "upcoming"));
    try {
      KafkaConsumerConfig.getAutoOffsetResetValue("", "whatever");
      Assert.fail("Invalid value should've triggered an exception");
    } catch (Exception e) {
      //expected
    }
  }
}
