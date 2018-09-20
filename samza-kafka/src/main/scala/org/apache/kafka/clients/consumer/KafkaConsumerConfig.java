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

package org.apache.kafka.clients.consumer;

import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


/**
 * The configuration class for KafkaConsumer
 */
public class KafkaConsumerConfig extends ConsumerConfig {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerConfig.class);

  public static final String PRODUCER_CLIENT_ID_PREFIX = "kafka-producer";
  public static final String CONSUMER_CLIENT_ID_PREFIX = "kafka-consumer";
  public static final String ADMIN_CLIENT_ID_PREFIX = "kafka-admin-metadata";
  public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  private static final String SAMZA_OFFSET_LARGEST = "largest";
  private static final String SAMZA_OFFSET_SMALLEST = "smallest";
  private static final String KAFKA_OFFSET_LATEST = "latest";
  private static final String KAFKA_OFFSET_EARLIEST = "earliest";
  private static final String KAFKA_OFFSET_NONE = "none";

  private final String systemName;

  /*
   * By default, KafkaConsumer will fetch ALL available messages for all the partitions.
   * This may cause memory issues. That's why we will limit the number of messages per partition we get on EACH poll().
   */
  static final String DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS = "100";

  private KafkaConsumerConfig(Properties props, String systemName) {
    super(props);
    this.systemName = systemName;
  }

  /**
   * Create kafka consumer configs, based on the subset of global configs.
   * @param config
   * @param systemName
   * @param idPrefix - prefix for the client id provided by the caller
   * @param injectProps
   * @return KafkaConsumerConfig
   */
  public static KafkaConsumerConfig getKafkaSystemConsumerConfig(Config config, String systemName, String idPrefix,
      Map<String, String> injectProps) {

    final Config subConf = config.subset(String.format("systems.%s.consumer.", systemName), true);

    final String groupId = createConsumerGroupId(config);
    final String clientId = createClientId(idPrefix, config);

    final Properties consumerProps = new Properties();
    consumerProps.putAll(subConf);

    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

    //Kafka client configuration

    // put overrides
    consumerProps.putAll(injectProps);

    // These are values we enforce in sazma, and they cannot be overwritten.

    // Disable consumer auto-commit because Samza controls commits
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // Translate samza config value to kafka config value
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        getAutoOffsetResetValue(consumerProps));

    // make sure bootstrap configs are in ?? SHOULD WE FAIL IF THEY ARE NOT?
    if (!subConf.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      // get it from the producer config
      String bootstrapServers =
          config.get(String.format("systems.%s.producer.%s", systemName, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
      if (StringUtils.isEmpty(bootstrapServers)) {
        throw new SamzaException("Missing " + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " config  for " + systemName);
      }
      consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    // Always use default partition assignment strategy. Do not allow override.
    consumerProps.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

    // the consumer is fully typed, and deserialization can be too. But in case it is not provided we should
    // default to byte[]
    if (!consumerProps.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
      LOG.info("setting default key serialization for the consumer(for {}) to ByteArrayDeserializer", systemName);
      consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }
    if (!consumerProps.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
      LOG.info("setting default value serialization for the consumer(for {}) to ByteArrayDeserializer", systemName);
      consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    // Override default max poll config if there is no value
    consumerProps.computeIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        (k) -> DEFAULT_KAFKA_CONSUMER_MAX_POLL_RECORDS);

    return new KafkaConsumerConfig(consumerProps, systemName);
  }

  public String getClientId() {
    String clientId = getString(ConsumerConfig.CLIENT_ID_CONFIG);
    if (clientId == null) {
      throw new SamzaException("client Id is not set for consumer for system=" + systemName);
    }
    return clientId;
  }

  // group id should be unique per job
  static String createConsumerGroupId(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    Option<String> jobIdOption = jobConfig.getJobId();
    Option<String> jobNameOption = jobConfig.getName();
    return (jobNameOption.isDefined() ? jobNameOption.get() : "undefined_job_name") + "-" + (jobIdOption.isDefined()
        ? jobIdOption.get() : "undefined_job_id");
  }

  public static String createProducerClientId(String prefix, Config config) {
    return createClientId(prefix , config);
  }
  // client id should be unique per job
  static String createClientId(String prefix, Config config) {
    if (config.get(JobConfig.JOB_NAME()) == null) {
      throw new ConfigException("Missing job name");
    }
    String jobName = config.get(JobConfig.JOB_NAME());
    String jobId = (config.get(JobConfig.JOB_ID()) != null) ? config.get(JobConfig.JOB_ID()) : "1";

    return String.format("%s-%s-%s", prefix.replaceAll("[^A-Za-z0-9]", "_"), jobName.replaceAll("[^A-Za-z0-9]", "_"),
        jobId.replaceAll("[^A-Za-z0-9]", "_"));
  }

  /**
   * If settings for auto.reset in samza are different from settings in Kafka (auto.offset.reset),
   * then need to convert them (see kafka.apache.org/documentation):
   * "largest" -> "latest"
   * "smallest" -> "earliest"
   *
   * If no setting specified we return "latest" (same as Kafka).
   * @param properties All consumer related {@link Properties} parsed from samza config
   * @return String representing the config value for "auto.offset.reset" property
   */
  static String getAutoOffsetResetValue(Properties properties) {
    String autoOffsetReset = properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_LATEST);

    // accept kafka values directly
    if (autoOffsetReset.equals(KAFKA_OFFSET_EARLIEST) || autoOffsetReset.equals(KAFKA_OFFSET_LATEST)
        || autoOffsetReset.equals(KAFKA_OFFSET_NONE)) {
      return autoOffsetReset;
    }

    String newAutoOffsetReset;
    switch (autoOffsetReset) {
      case SAMZA_OFFSET_LARGEST:
        newAutoOffsetReset =  KAFKA_OFFSET_LATEST;
        break;
      case SAMZA_OFFSET_SMALLEST:
        newAutoOffsetReset =  KAFKA_OFFSET_EARLIEST;
        break;
      default:
        newAutoOffsetReset =  KAFKA_OFFSET_LATEST;
    }
    LOG.info("AutoOffsetReset value converted from {} to {}", autoOffsetReset,  newAutoOffsetReset);
    return newAutoOffsetReset;
  }
}