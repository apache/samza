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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.samza.SamzaException;
import org.apache.samza.execution.JobPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


/**
 * The configuration class for KafkaConsumer
 */
public class KafkaConsumerConfig extends HashMap<String, Object> {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerConfig.class);

  public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  private static final int FETCH_MAX_BYTES = 1024 * 1024;

  private final String systemName;
  /*
   * By default, KafkaConsumer will fetch some big number of available messages for all the partitions.
   * This may cause memory issues. That's why we will limit the number of messages per partition we get on EACH poll().
   */
  static final String DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS = "100";

  private KafkaConsumerConfig(Map<String, Object> props, String systemName) {
    super(props);
    this.systemName = systemName;
  }

  /**
   * Create kafka consumer configs, based on the subset of global configs.
   * @param config application config
   * @param systemName system name
   * @param clientId client id provided by the caller
   * @return KafkaConsumerConfig
   */
  public static KafkaConsumerConfig getKafkaSystemConsumerConfig(Config config, String systemName, String clientId) {

    Config subConf = config.subset(String.format("systems.%s.consumer.", systemName), true);

    final String groupId = createConsumerGroupId(config);

    Map<String, Object> consumerProps = new HashMap<>(subConf);

    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

    // These are values we enforce in sazma, and they cannot be overwritten.

    // Disable consumer auto-commit because Samza controls commits
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // check if samza default offset value is defined
    String systemOffsetDefault = new SystemConfig(config).getSystemOffsetDefault(systemName);

    // Translate samza config value to kafka config value
    String autoOffsetReset = getAutoOffsetResetValue((String) consumerProps.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), systemOffsetDefault);
    LOG.info("setting auto.offset.reset for system {} to {}", systemName, autoOffsetReset);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

    // if consumer bootstrap servers are not configured, get them from the producer configs
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
    consumerProps.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS);

    return new KafkaConsumerConfig(consumerProps, systemName);
  }

  public String getClientId() {
    String clientId = (String) get(ConsumerConfig.CLIENT_ID_CONFIG);
    if (StringUtils.isBlank(clientId)) {
      throw new SamzaException("client Id is not set for consumer for system=" + systemName);
    }
    return clientId;
  }

  public int fetchMessageMaxBytes() {
    String fetchSize = (String)get("fetch.message.max.bytes");
    if (StringUtils.isBlank(fetchSize)) {
      return FETCH_MAX_BYTES;
    } else  {
      return Integer.valueOf(fetchSize);
    }
  }

  public String getZkConnect() {
    return (String) get(ZOOKEEPER_CONNECT);
  }

  // group id should be unique per job
  static String createConsumerGroupId(Config config) {
    Pair<String, String> jobNameId = getJobNameAndId(config);

    return String.format("%s-%s", jobNameId.getLeft(), jobNameId.getRight());
  }

  // client id should be unique per job
  public static String createClientId(String prefix, Config config) {

    Pair<String, String> jobNameId = getJobNameAndId(config);
    String jobName = jobNameId.getLeft();
    String jobId = jobNameId.getRight();
    return String.format("%s-%s-%s", prefix.replaceAll("\\W", "_"), jobName.replaceAll("\\W", "_"),
        jobId.replaceAll("\\W", "_"));
  }

  public static Pair<String, String> getJobNameAndId(Config config) {
    JobConfig jobConfig = new JobConfig(JobPlanner.generateSingleJobConfig(config));
    Option jobNameOption = jobConfig.getName();
    if (jobNameOption.isEmpty()) {
      throw new ConfigException("Missing job name");
    }
    String jobName = (String) jobNameOption.get();
    return new ImmutablePair<>(jobName, jobConfig.getJobId());
  }

  /**
   * If settings for Kafka Consumer auto.offset.reset is set - use it.
   * If this setting is using the old (deprecated values) - translate them:
   * "largest" -> "latest"
   * "smallest" -> "earliest"
   *
   * If no setting specified - match it to the Samza default offset setting.
   * If none defined - return "latest"
   * @param autoOffsetReset consumer.auto.offset.reset config
   * @param samzaOffsetDefault  samza system default
   * @return String representing the config value for "auto.offset.reset" property
   */
  static String getAutoOffsetResetValue(final String autoOffsetReset, final String samzaOffsetDefault) {
    // valid kafka consumer values
    final String KAFKA_OFFSET_LATEST = "latest";
    final String KAFKA_OFFSET_EARLIEST = "earliest";
    final String KAFKA_OFFSET_NONE = "none";

    // if the value for KafkaConsumer is set - use it.
    if (!StringUtils.isBlank(autoOffsetReset)) {
      if (autoOffsetReset.equals(KAFKA_OFFSET_EARLIEST) || autoOffsetReset.equals(KAFKA_OFFSET_LATEST)
          || autoOffsetReset.equals(KAFKA_OFFSET_NONE)) {
        return autoOffsetReset;
      }
      // translate old kafka consumer values into new ones (SAMZA-1987 top remove it)
      String newAutoOffsetReset;
      switch (autoOffsetReset) {
        case "largest":
          newAutoOffsetReset = KAFKA_OFFSET_LATEST;
          LOG.warn("Using old (deprecated) value for kafka consumer config auto.offset.reset = {}. The right value should be {}", autoOffsetReset, KAFKA_OFFSET_LATEST);
          break;
        case "smallest":
          newAutoOffsetReset = KAFKA_OFFSET_EARLIEST;
          LOG.warn("Using old (deprecated) value for kafka consumer config auto.offset.reset = {}. The right value should be {}", autoOffsetReset, KAFKA_OFFSET_EARLIEST);
          break;
        default:
          throw new SamzaException("Using invalid value for kafka consumer config auto.offset.reset " + autoOffsetReset + ". See KafkaConsumer config for the correct values.");
      }

      LOG.info("Auto offset reset value converted from {} to {}", autoOffsetReset, newAutoOffsetReset);
      return newAutoOffsetReset;
    }

    // in case kafka consumer configs are not provided we should match them to Samza's ones.
    String newAutoOffsetReset = KAFKA_OFFSET_LATEST;
    if (!StringUtils.isBlank(samzaOffsetDefault)) {
      switch (samzaOffsetDefault) {
        case SystemConfig.SAMZA_SYSTEM_OFFSET_UPCOMING:
          newAutoOffsetReset = KAFKA_OFFSET_LATEST;
          break;
        case SystemConfig.SAMZA_SYSTEM_OFFSET_OLDEST:
          newAutoOffsetReset = KAFKA_OFFSET_EARLIEST;
          break;
        default:
          throw new SamzaException("Using invalid value for samza default offset config " + autoOffsetReset + ". See samza config for the correct values");
      }
      LOG.info("Auto offset reset value for KafkaConsumer for system {} converted from {}(samza) to {}", samzaOffsetDefault, newAutoOffsetReset);
    }

    return newAutoOffsetReset;
  }
}
