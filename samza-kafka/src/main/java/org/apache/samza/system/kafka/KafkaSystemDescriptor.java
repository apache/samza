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
package org.apache.samza.system.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SimpleSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;


/**
 * A descriptor for a Kafka system.
 */
@SuppressWarnings("unchecked")
public class KafkaSystemDescriptor extends SimpleSystemDescriptor<KafkaSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = KafkaSystemFactory.class.getName();
  private static final String CONSUMER_ZK_CONNECT_CONFIG_KEY = "systems.%s.consumer.zookeeper.connect";
  private static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG_KEY = "systems.%s.consumer.auto.offset.reset";
  private static final String CONSUMER_FETCH_THRESHOLD_CONFIG_KEY = KafkaConfig.CONSUMER_FETCH_THRESHOLD();
  private static final String CONSUMER_FETCH_THRESHOLD_BYTES_CONFIG_KEY = KafkaConfig.CONSUMER_FETCH_THRESHOLD_BYTES();
  private static final String CONSUMER_FETCH_MESSAGE_MAX_BYTES_KEY = "systems.%s.consumer.fetch.message.max.bytes";
  private static final String CONSUMER_CONFIGS_CONFIG_KEY = "systems.%s.consumer.%s";
  private static final String PRODUCER_BOOTSTRAP_SERVERS_CONFIG_KEY = "systems.%s.producer.bootstrap.servers";
  private static final String PRODUCER_CONFIGS_CONFIG_KEY = "systems.%s.producer.%s";

  private List<String> consumerZkConnect = Collections.emptyList();
  private Optional<String> consumerAutoOffsetResetOptional = Optional.empty();
  private Optional<Integer> consumerFetchThresholdOptional = Optional.empty();
  private Optional<Long> consumerFetchThresholdBytesOptional = Optional.empty();
  private Optional<Long> consumerFetchMessageMaxBytesOptional = Optional.empty();
  private Map<String, String> consumerConfigs = Collections.emptyMap();
  private List<String> producerBootstrapServers = Collections.emptyList();
  private Map<String, String> producerConfigs = Collections.emptyMap();

  /**
   * Constructs a {@link KafkaSystemDescriptor} instance with no system level serde.
   * Serdes must be provided explicitly at stream level when getting input or output descriptors.
   *
   * @param systemName name of this system
   */
  public KafkaSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> KafkaInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new KafkaInputDescriptor<>(streamId, this, serde, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> InputDescriptor<StreamMessageType, ? extends InputDescriptor> getInputDescriptor(String streamId, InputTransformer<StreamMessageType> transformer, Serde serde) {
    return new KafkaInputDescriptor<>(streamId, this, serde, transformer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <StreamMessageType> KafkaOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new KafkaOutputDescriptor<>(streamId, this, serde);
  }

  /**
   * The hostname and port of one or more Zookeeper nodes where information about the Kafka cluster can be found.
   * This is given as a list of hostname:port pairs, such as
   * {@code ImmutableList.of("zk1.example.com:2181", "zk2.example.com:2181", "zk3.example.com:2181")}.
   * If the cluster information is at some sub-path of the Zookeeper namespace, you need to include the path at the
   * end of the list of hostnames, for example:
   * {@code ImmutableList.of("zk1.example.com:2181", "zk2.example.com:2181/clusters/my-kafka")}.
   *
   * @param consumerZkConnect Zookeeper connection information for the system
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withConsumerZkConnect(List<String> consumerZkConnect) {
    this.consumerZkConnect = consumerZkConnect;
    return this;
  }

  /**
   * This setting determines what happens if a consumer attempts to read an offset that is outside of the current
   * valid range. This could happen if the topic does not exist, or if a checkpoint is older than the maximum message
   * history retained by the brokers. This property is not to be confused with {@link InputDescriptor#withOffsetDefault},
   * which determines what happens if there is no checkpoint.
   * <p>
   * The following are valid values for auto.offset.reset:
   * <ul>
   *   <li>smallest: Start consuming at the smallest (oldest) offset available on the broker
   *   (process as much message history as available).
   *   <li>largest: Start consuming at the largest (newest) offset available on the broker
   *   (skip any messages published while the job was not running).
   *   <li>anything else: Throw an exception and refuse to start up the job.
   * </ul>
   * <p>
   * Note: This property may be set at a topic level using {@link KafkaInputDescriptor#withConsumerAutoOffsetReset}
   *
   * @param consumerAutoOffsetReset consumer auto offset reset policy for the system
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withConsumerAutoOffsetReset(String consumerAutoOffsetReset) {
    this.consumerAutoOffsetResetOptional = Optional.of(StringUtils.stripToNull(consumerAutoOffsetReset));
    return this;
  }

  /**
   * When consuming streams from Kafka, a Samza container maintains an in-memory buffer for incoming messages in
   * order to increase throughput (the stream task can continue processing buffered messages while new messages are
   * fetched from Kafka). This parameter determines the number of messages we aim to buffer across all stream partitions
   * consumed by a container. For example, if a container consumes 50 partitions, it will try to buffer 1000
   * messages per partition by default. When the number of buffered messages falls below that threshold, Samza
   * fetches more messages from the Kafka broker to replenish the buffer. Increasing this parameter can increase
   * a job's processing throughput, but also increases the amount of memory used.
   *
   * @param fetchThreshold number of incoming messages to buffer in-memory
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withSamzaFetchThreshold(int fetchThreshold) {
    this.consumerFetchThresholdOptional = Optional.of(fetchThreshold);
    return this;
  }

  /**
   * When consuming streams from Kafka, a Samza container maintains an in-memory buffer for incoming messages in
   * order to increase throughput (the stream task can continue processing buffered messages while new messages are
   * fetched from Kafka). This parameter determines the total size of messages we aim to buffer across all stream
   * partitions consumed by a container based on bytes. Defines how many bytes to use for the buffered prefetch
   * messages for job as a whole. The bytes for a single system/stream/partition are computed based on this.
   * This fetches the entire messages, hence this bytes limit is a soft one, and the actual usage can be
   * the bytes limit + size of max message in the partition for a given stream. If the value of this property
   * is &gt; 0 then this takes precedence over systems.system-name.samza.fetch.threshold.
   * <p>
   * For example, if fetchThresholdBytes is set to 100000 bytes, and there are 50 SystemStreamPartitions registered,
   * then the per-partition threshold is (100000 / 2) / 50 = 1000 bytes. As this is a soft limit, the actual usage
   * can be 1000 bytes + size of max message. As soon as a SystemStreamPartition's buffered messages bytes drops
   * below 1000, a fetch request will be executed to get more data for it. Increasing this parameter will decrease
   * the latency between when a queue is drained of messages and when new messages are enqueued, but also leads
   * to an increase in memory usage since more messages will be held in memory. The default value is -1,
   * which means this is not used.
   *
   * @param fetchThresholdBytes number of bytes for incoming messages to buffer in-memory
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withSamzaFetchThresholdBytes(long fetchThresholdBytes) {
    this.consumerFetchThresholdBytesOptional = Optional.of(fetchThresholdBytes);
    return this;
  }

  /**
   * The number of bytes of messages to attempt to fetch for each topic-partition in each fetch request.
   * These bytes will be read into memory for each partition, so this helps control the memory used by the consumer.
   * The fetch request size must be at least as large as the maximum message size the server allows or else it is
   * possible for the producer to send messages larger than the consumer can fetch.
   * <p>
   * Note: This property may be set at a topic level using {@link KafkaInputDescriptor#withConsumerFetchMessageMaxBytes}
   *
   * @param fetchMessageMaxBytes number of bytes of messages to attempt to fetch for each topic-partition
   *                             in each fetch request
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withConsumerFetchMessageMaxBytes(long fetchMessageMaxBytes) {
    this.consumerFetchMessageMaxBytesOptional = Optional.of(fetchMessageMaxBytes);
    return this;
  }

  /**
   * Any Kafka consumer configuration can be included here. For example, to change the socket timeout,
   * you can set socket.timeout.ms. (There is no need to configure group.id or client.id, as they are automatically
   * configured by Samza. Also, there is no need to set auto.commit.enable because Samza has its own
   * checkpointing mechanism.)
   *
   * @param consumerConfigs additional consumer configuration
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withConsumerConfigs(Map<String, String> consumerConfigs) {
    this.consumerConfigs = consumerConfigs;
    return this;
  }

  /**
   * A list of network endpoints where the Kafka brokers are running. This is given as a list of hostname:port pairs,
   * for example {@code ImmutableList.of("kafka1.example.com:9092", "kafka2.example.com:9092", "kafka3.example.com:9092")}.
   * It's not necessary to list every single Kafka node in the cluster: Samza uses this property in order to discover
   * which topics and partitions are hosted on which broker. This property is needed even if you are only consuming
   * from Kafka, and not writing to it, because Samza uses it to discover metadata about streams being consumed.
   *
   * @param producerBootstrapServers network endpoints where the kafka brokers are running
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withProducerBootstrapServers(List<String> producerBootstrapServers) {
    this.producerBootstrapServers = producerBootstrapServers;
    return this;
  }

  /**
   * Any Kafka producer configuration can be included here. For example, to change the request timeout,
   * you can set timeout.ms. (There is no need to configure client.id as it is automatically configured by Samza.)
   *
   * @param producerConfigs additional producer configuration
   * @return this system descriptor
   */
  public KafkaSystemDescriptor withProducerConfigs(Map<String, String> producerConfigs) {
    this.producerConfigs = producerConfigs;
    return this;
  }

  private List<String> getConsumerZkConnect() {
    return this.consumerZkConnect;
  }

  private Optional<String> getConsumerAutoOffsetReset() {
    return this.consumerAutoOffsetResetOptional;
  }

  private Optional<Integer> getConsumerFetchThreshold() {
    return this.consumerFetchThresholdOptional;
  }

  private Optional<Long> getConsumerFetchThresholdBytes() {
    return this.consumerFetchThresholdBytesOptional;
  }

  private Optional<Long> getConsumerFetchMessageMaxBytes() {
    return this.consumerFetchMessageMaxBytesOptional;
  }

  private Map<String, String> getConsumerConfigs() {
    return this.consumerConfigs;
  }

  private List<String> getProducerBootstrapServers() {
    return this.producerBootstrapServers;
  }

  private Map<String, String> getProducerConfigs() {
    return this.producerConfigs;
  }

  @Override
  public Map<String, String> toConfig() {
    Map<String, String> configs = new HashMap<>(super.toConfig());
    if(!getConsumerZkConnect().isEmpty()) {
      configs.put(String.format(CONSUMER_ZK_CONNECT_CONFIG_KEY, getSystemName()), String.join(",", getConsumerZkConnect()));
    }
    getConsumerAutoOffsetReset().ifPresent(consumerAutoOffsetReset ->
        configs.put(String.format(CONSUMER_AUTO_OFFSET_RESET_CONFIG_KEY, getSystemName()), consumerAutoOffsetReset));
    getConsumerFetchThreshold().ifPresent(consumerFetchThreshold ->
        configs.put(String.format(CONSUMER_FETCH_THRESHOLD_CONFIG_KEY, getSystemName()), Integer.toString(consumerFetchThreshold)));
    getConsumerFetchThresholdBytes().ifPresent(consumerFetchThresholdBytes ->
        configs.put(String.format(CONSUMER_FETCH_THRESHOLD_BYTES_CONFIG_KEY, getSystemName()), Long.toString(consumerFetchThresholdBytes)));
    getConsumerFetchMessageMaxBytes().ifPresent(consumerFetchMessageMaxBytes ->
        configs.put(String.format(CONSUMER_FETCH_MESSAGE_MAX_BYTES_KEY, getSystemName()), Long.toString(consumerFetchMessageMaxBytes)));
    getConsumerConfigs().forEach((key, value) -> configs.put(String.format(CONSUMER_CONFIGS_CONFIG_KEY, getSystemName(), key), value));
    if (!getProducerBootstrapServers().isEmpty()) {
      configs.put(String.format(PRODUCER_BOOTSTRAP_SERVERS_CONFIG_KEY, getSystemName()), String.join(",", getProducerBootstrapServers()));
    }
    getProducerConfigs().forEach((key, value) -> configs.put(String.format(PRODUCER_CONFIGS_CONFIG_KEY, getSystemName(), key), value));
    return configs;
  }
}