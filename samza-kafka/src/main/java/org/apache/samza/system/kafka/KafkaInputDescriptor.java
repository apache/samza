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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;

public class KafkaInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, KafkaInputDescriptor<StreamMessageType>> {
  private static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG_KEY = "systems.%s.streams.%s.consumer.auto.offset.reset";
  private static final String CONSUMER_FETCH_MESSAGE_MAX_BYTES_CONFIG_KEY = "systems.%s.streams.%s.consumer.fetch.message.max.bytes";

  private Optional<String> consumerAutoOffsetResetOptional = Optional.empty();
  private Optional<Long> consumerFetchMessageMaxBytesOptional = Optional.empty();

  KafkaInputDescriptor(String streamId, SystemDescriptor systemDescriptor, Serde serde, InputTransformer<StreamMessageType> transformer) {
    super(streamId, systemDescriptor.getSystemName(), serde, systemDescriptor, transformer);
  }

  /**
   * This setting determines what happens if a consumer attempts to read an offset for this topic that is outside of
   * the current valid range. This could happen if the topic does not exist, or if a checkpoint is older than the
   * maximum message history retained by the brokers. This property is not to be confused with
   * {@link InputDescriptor#withOffsetDefault}, which determines what happens if there is no checkpoint.
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
   * Note: This property may be set at a system level using {@link KafkaSystemDescriptor#withConsumerAutoOffsetReset}
   *
   * @param consumerAutoOffsetReset consumer auto offset reset policy for the input
   * @return this input descriptor
   */
  public KafkaInputDescriptor<StreamMessageType> withConsumerAutoOffsetReset(String consumerAutoOffsetReset) {
    this.consumerAutoOffsetResetOptional = Optional.of(StringUtils.stripToNull(consumerAutoOffsetReset));
    return this;
  }

  /**
   * The number of bytes of messages to attempt to fetch for each topic-partition for this topic in each fetch request.
   * These bytes will be read into memory for each partition, so this helps control the memory used by the consumer.
   * The fetch request size must be at least as large as the maximum message size the server allows or else it is
   * possible for the producer to send messages larger than the consumer can fetch.
   * <p>
   * Note: This property may be set at a system level using {@link KafkaSystemDescriptor#withConsumerFetchMessageMaxBytes}
   *
   * @param fetchMessageMaxBytes number of bytes of messages to attempt to fetch for each topic-partition
   *                             in each fetch request
   * @return this input descriptor
   */
  public KafkaInputDescriptor<StreamMessageType> withConsumerFetchMessageMaxBytes(long fetchMessageMaxBytes) {
    this.consumerFetchMessageMaxBytesOptional = Optional.of(fetchMessageMaxBytes);
    return this;
  }

  private Optional<String> getConsumerAutoOffsetReset() {
    return this.consumerAutoOffsetResetOptional;
  }

  private Optional<Long> getConsumerFetchMessageMaxBytes() {
    return this.consumerFetchMessageMaxBytesOptional;
  }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>(super.toConfig());
    // Note: Kafka configuration needs the topic's physical name, not the stream-id.
    // We won't have that here if user only specified it in configs, or if it got rewritten
    // by the planner to something different than what's in this descriptor.
    String streamName;
    if (getPhysicalName().isPresent()) {
      streamName = getPhysicalName().get();
    } else {
      streamName = getStreamId();
    }
    String systemName = getSystemName();

    getConsumerAutoOffsetReset().ifPresent(autoOffsetReset ->
        configs.put(String.format(CONSUMER_AUTO_OFFSET_RESET_CONFIG_KEY, systemName, streamName), autoOffsetReset));
    getConsumerFetchMessageMaxBytes().ifPresent(fetchMessageMaxBytes ->
        configs.put(String.format(CONSUMER_FETCH_MESSAGE_MAX_BYTES_CONFIG_KEY, systemName, streamName), Long.toString(fetchMessageMaxBytes)));
    return configs;
  }
}
