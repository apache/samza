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
package org.apache.samza.system.kinesis.descriptors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.system.kinesis.KinesisConfig;


/**
 * A {@link KinesisInputDescriptor} can be used for specifying Samza and Kinesis specific properties of Kinesis
 * input streams.
 * <p>
 * Use {@link KinesisSystemDescriptor#getInputDescriptor} to obtain an instance of this descriptor.
 * <p>
 * Stream properties provided in configuration override corresponding properties specified using a descriptor.
 *
 * @param <StreamMessageType> type of messages in this stream
 */
public class KinesisInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, KinesisInputDescriptor<StreamMessageType>> {
  private Optional<String> accessKey = Optional.empty();
  private Optional<String> secretKey = Optional.empty();
  private Optional<String> region = Optional.empty();
  private Map<String, String> kclConfig = Collections.emptyMap();


  /**
   * Constructs an {@link InputDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param valueSerde serde the values in the messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  <T> KinesisInputDescriptor(String streamId, Serde<T> valueSerde, SystemDescriptor systemDescriptor) {
    super(streamId, KVSerde.of(new NoOpSerde<>(), valueSerde), systemDescriptor, null);
  }

  /**
   * Kinesis region for the system stream.
   * @param region Kinesis region
   * @return this input descriptor
   */
  public KinesisInputDescriptor<StreamMessageType> withRegion(String region) {
    this.region = Optional.of(StringUtils.stripToNull(region));
    return this;
  }

  /**
   * Kinesis access key name for the system stream.
   * @param accessKey Kinesis access key name
   * @return this input descriptor
   */
  public KinesisInputDescriptor<StreamMessageType> withAccessKey(String accessKey) {
    this.accessKey = Optional.of(StringUtils.stripToNull(accessKey));
    return this;
  }

  /**
   * Kinesis secret key name for the system stream.
   * @param secretKey Kinesis secret key
   * @return this input descriptor
   */
  public KinesisInputDescriptor<StreamMessageType> withSecretKey(String secretKey) {
    this.secretKey = Optional.of(StringUtils.stripToNull(secretKey));
    return this;
  }

  /**
   * KCL (Kinesis Client Library) config for the system stream. This is not required by default.
   * @param kclConfig A map of specified KCL configs
   * @return this input descriptor
   */
  public KinesisInputDescriptor<StreamMessageType> withKCLConfig(Map<String, String> kclConfig) {
    this.kclConfig = kclConfig;
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    Map<String, String> config = new HashMap<>(super.toConfig());

    String systemName = getSystemName();
    String streamId = getStreamId();
    String clientConfigPrefix =
        String.format(KinesisConfig.CONFIG_STREAM_KINESIS_CLIENT_LIB_CONFIG, systemName, streamId);

    this.region.ifPresent(
        val -> config.put(String.format(KinesisConfig.CONFIG_STREAM_REGION, systemName, streamId), val));
    this.accessKey.ifPresent(
        val -> config.put(String.format(KinesisConfig.CONFIG_STREAM_ACCESS_KEY, systemName, streamId), val));
    this.secretKey.ifPresent(
        val -> config.put(String.format(KinesisConfig.CONFIG_STREAM_SECRET_KEY, systemName, streamId), val));
    this.kclConfig.forEach((k, v) -> config.put(clientConfigPrefix + k, v));

    return config;
  }
}
