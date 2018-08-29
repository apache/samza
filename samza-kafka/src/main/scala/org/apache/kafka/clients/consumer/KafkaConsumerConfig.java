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
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import scala.Option;


/**
 * The configuration class for KafkaConsumer
 */
public class KafkaConsumerConfig extends ConsumerConfig {

  private static final String PRODUCER_CLIENT_ID_PREFIX = "kafka-producer";
  private static final String CONSUMER_CLIENT_ID_PREFIX = "kafka-consumer";
  private static final String SAMZA_OFFSET_LARGEST = "largest";
  private static final String SAMZA_OFFSET_SMALLEST = "smallest";
  private static final String KAFKA_OFFSET_LATEST = "latest";
  private static final String KAFKA_OFFSET_EARLIEST = "earliest";
  private static final String KAFKA_OFFSET_NONE = "none";
  /*
   * By default, KafkaConsumer will fetch ALL available messages for all the partitions.
   * This may cause memory issues. That's why we will limit the number of messages per partition we get on EACH poll().
   */
  private static final String KAFKA_CONSUMER_MAX_POLL_RECORDS_DEFAULT = "100";


  public KafkaConsumerConfig(Properties props) {
    super(props);
  }

  public static KafkaConsumerConfig getKafkaSystemConsumerConfig(Config config,
      String systemName, String clientId, Map<String, String> injectProps) {

    Config subConf = config.subset(String.format("systems.%s.consumer.", systemName), true);

    String groupId = getConsumerGroupId(config);

    Properties consumerProps = new Properties();
    consumerProps.putAll(subConf);

    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

    //Open-source Kafka Consumer configuration
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable consumer auto-commit

    consumerProps.setProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        getAutoOffsetResetValue(consumerProps));  // Translate samza config value to kafka config value

    // make sure bootstrap configs are in ?? SHOULD WE FAIL IF THEY ARE NOT?
    if (! subConf.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      // get it from the producer config
      String bootstrapServer = config.get(String.format("systems.%s.producer.%s", systemName, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
      consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    }

    // Always use default partition assignment strategy. Do not allow override.
    consumerProps.setProperty(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        RangeAssignor.class.getName());


    // NOT SURE THIS IS NEEDED TODO
    String maxPollRecords = subConf.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KAFKA_CONSUMER_MAX_POLL_RECORDS_DEFAULT);;
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

    // put overrides
    consumerProps.putAll(injectProps);

    return new KafkaConsumerConfig(consumerProps);
  }

  // group id should be unique per job
  static String getConsumerGroupId(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    Option<String> jobIdOption = jobConfig.getJobId();
    Option<String> jobNameOption = jobConfig.getName();
    return (jobNameOption.isDefined()? jobNameOption.get() : "undefined_job_name") + "-"
        + (jobIdOption.isDefined()? jobIdOption.get() : "undefined_job_id");
  }
  // client id should be unique per job
  public static String getClientId(String id, Config config) {
    if (config.get(JobConfig.JOB_NAME()) == null) {
      throw new ConfigException("Missing job name");
    }
    String jobName = config.get(JobConfig.JOB_NAME());
    String jobId = "1";
    if (config.get(JobConfig.JOB_ID()) != null) {
      jobId = config.get(JobConfig.JOB_ID());
    }
    return getClientId(id, jobName, jobId);
  }

  private static String getClientId(String id, String jobName, String jobId) {
    return String.format(
        "%s-%s-%s",
        id.replaceAll("[^A-Za-z0-9]", "_"),
        jobName.replaceAll("[^A-Za-z0-9]", "_"),
        jobId.replaceAll("[^A-Za-z0-9]", "_"));
  }

  public static String getProducerClientId(Config config) {
    return getClientId(PRODUCER_CLIENT_ID_PREFIX, config);
  }

  /**
   * Settings for auto.reset in samza are different from settings in Kafka (auto.offset.reset) - need to convert
   * "largest" -> "latest"
   * "smallest" -> "earliest"
   * "none" - will fail the kafka consumer, if offset is out of range
   * @param properties All consumer related {@link Properties} parsed from samza config
   * @return String representing the config value for "auto.offset.reset" property
   */
  static String getAutoOffsetResetValue(Properties properties) {
    String autoOffsetReset = properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_LATEST);

    // accept kafka values directly
    if (autoOffsetReset.equals(KAFKA_OFFSET_EARLIEST) ||
        autoOffsetReset.equals(KAFKA_OFFSET_LATEST) ||
        autoOffsetReset.equals(KAFKA_OFFSET_NONE)) {
      return autoOffsetReset;
    }

    switch (autoOffsetReset) {
      case SAMZA_OFFSET_LARGEST:
        return KAFKA_OFFSET_LATEST;
      case SAMZA_OFFSET_SMALLEST:
        return KAFKA_OFFSET_EARLIEST;
      default:
        return KAFKA_OFFSET_LATEST;
    }
  }

}