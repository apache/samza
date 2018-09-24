/*
 *
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
 *
 */

package org.apache.samza.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


/**
 * The configuration class for KafkaConsumer
 */
public class KafkaConsumerConfig extends HashMap<String, Object> {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerConfig.class);

  private static final String PRODUCER_CLIENT_ID_PREFIX = "kafka-producer";
  private static final String CONSUMER_CLIENT_ID_PREFIX = "kafka-consumer";
  private static final String ADMIN_CLIENT_ID_PREFIX = "samza-admin";

  /*
   * By default, KafkaConsumer will fetch some big number of available messages for all the partitions.
   * This may cause memory issues. That's why we will limit the number of messages per partition we get on EACH poll().
   */
  static final String DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS = "100";

  private KafkaConsumerConfig(Map<String, Object> map) {
    super(map);
  }

  /**
   * This is a help method to create the configs for use in Kafka consumer.
   * The values are based on the "consumer" subset of the configs provided by the app and Samza overrides.
   *
   * @param config - config provided by the app.
   * @param systemName - system name for which the consumer is configured.
   * @param clientId - client id to be used in the Kafka consumer.
   * @return KafkaConsumerConfig
   */
  public static KafkaConsumerConfig getKafkaSystemConsumerConfig(Config config, String systemName, String clientId) {

    Config subConf = config.subset(String.format("systems.%s.consumer.", systemName), true);

    //Kafka client configuration
    String groupId = getConsumerGroupId(config);

    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.putAll(subConf);

    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

    // These are values we enforce in sazma, and they cannot be overwritten.

    // Disable consumer auto-commit because Samza controls commits
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // Translate samza config value to kafka config value
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        getAutoOffsetResetValue((String) consumerProps.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)));

    // make sure bootstrap configs are in, if not - get them from the producer
    if (!subConf.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      String bootstrapServers =
          config.get(String.format("systems.%s.producer.%s", systemName, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
      if (StringUtils.isEmpty(bootstrapServers)) {
        throw new SamzaException("Missing " + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " config  for " + systemName);
      }
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    // Always use default partition assignment strategy. Do not allow override.
    consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

    // the consumer is fully typed, and deserialization can be too. But in case it is not provided we should
    // default to byte[]
    if (!consumerProps.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
      LOG.info("setting key serialization for the consumer(for system {}) to ByteArrayDeserializer", systemName);
      consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }
    if (!consumerProps.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
      LOG.info("setting value serialization for the consumer(for system {}) to ByteArrayDeserializer", systemName);
      consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    // Override default max poll config if there is no value
    consumerProps.computeIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        (k) -> DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS);

    return new KafkaConsumerConfig(consumerProps);
  }

  // group id should be unique per job
  static String getConsumerGroupId(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    Option<String> jobIdOption = jobConfig.getJobId();
    Option<String> jobNameOption = jobConfig.getName();
    return (jobNameOption.isDefined() ? jobNameOption.get() : "undefined_job_name") + "-" + (jobIdOption.isDefined()
        ? jobIdOption.get() : "undefined_job_id");
  }

  // client id should be unique per job
  public static String getConsumerClientId(Config config) {
    return getConsumerClientId(CONSUMER_CLIENT_ID_PREFIX, config);
  }

  public static String getProducerClientId(Config config) {
    return getConsumerClientId(PRODUCER_CLIENT_ID_PREFIX, config);
  }

  public static String getAdminClientId(Config config) {
    return getConsumerClientId(ADMIN_CLIENT_ID_PREFIX, config);
  }

  static String getConsumerClientId(String id, Config config) {
    if (config.get(JobConfig.JOB_NAME()) == null) {
      throw new ConfigException("Missing job name");
    }
    String jobName = config.get(JobConfig.JOB_NAME());
    String jobId = (config.get(JobConfig.JOB_ID()) != null) ? config.get(JobConfig.JOB_ID()) : "1";

    return String.format("%s-%s-%s", id.replaceAll("\\W", "_"), jobName.replaceAll("\\W", "_"),
        jobId.replaceAll("\\W", "_"));
  }

  /**
   * If settings for auto.reset in samza are different from settings in Kafka (auto.offset.reset),
   * then need to convert them (see kafka.apache.org/documentation):
   * "largest" -> "latest"
   * "smallest" -> "earliest"
   *
   * If no setting specified we return "latest" (same as Kafka).
   * @param autoOffsetReset value from the app provided config
   * @return String representing the config value for "auto.offset.reset" property
   */
  static String getAutoOffsetResetValue(final String autoOffsetReset) {
    final String SAMZA_OFFSET_LARGEST = "largest";
    final String SAMZA_OFFSET_SMALLEST = "smallest";
    final String KAFKA_OFFSET_LATEST = "latest";
    final String KAFKA_OFFSET_EARLIEST = "earliest";
    final String KAFKA_OFFSET_NONE = "none";

    if (autoOffsetReset == null) {
      return KAFKA_OFFSET_LATEST; // return default
    }

    // accept kafka values directly
    if (autoOffsetReset.equals(KAFKA_OFFSET_EARLIEST) || autoOffsetReset.equals(KAFKA_OFFSET_LATEST)
        || autoOffsetReset.equals(KAFKA_OFFSET_NONE)) {
      return autoOffsetReset;
    }

    String newAutoOffsetReset;
    switch (autoOffsetReset) {
      case SAMZA_OFFSET_LARGEST:
        newAutoOffsetReset = KAFKA_OFFSET_LATEST;
        break;
      case SAMZA_OFFSET_SMALLEST:
        newAutoOffsetReset = KAFKA_OFFSET_EARLIEST;
        break;
      default:
        newAutoOffsetReset = KAFKA_OFFSET_LATEST;
    }
    LOG.info("AutoOffsetReset value converted from {} to {}", autoOffsetReset, newAutoOffsetReset);
    return newAutoOffsetReset;
  }
}