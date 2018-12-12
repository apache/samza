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

import org.apache.samza.operators.KV;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.system.kinesis.KinesisConfig;
import org.apache.samza.system.kinesis.KinesisSystemFactory;


/**
 * A {@link KinesisSystemDescriptor} can be used for specifying Samza and Kinesis-specific properties of a Kinesis
 * input system. It can also be used for obtaining {@link KinesisInputDescriptor}s,
 * which can be used for specifying Samza and system-specific properties of Kinesis input streams.
 * <p>
 * System properties provided in configuration override corresponding properties specified using a descriptor.
 */
public class KinesisSystemDescriptor extends SystemDescriptor<KinesisSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = KinesisSystemFactory.class.getName();

  private Optional<String> region = Optional.empty();
  private Optional<String> proxyHost = Optional.empty();
  private Optional<Integer> proxyPort = Optional.empty();
  private Map<String, String> awsConfig = Collections.emptyMap();
  private Map<String, String> kclConfig = Collections.emptyMap();

  public KinesisSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, null, null);
  }

  /**
   * Gets an {@link KinesisInputDescriptor} for the input stream of this system.
   * <p>
   * The message in the stream will have {@link String} keys and {@code ValueType} values.
   *
   * @param streamId id of the input stream
   * @param valueSerde stream level serde for the values in the messages in the input stream
   * @param <ValueType> type of the value in the messages in this stream
   * @return an {@link KinesisInputDescriptor} for the Kinesis input stream
   */
  public <ValueType> KinesisInputDescriptor<KV<String, ValueType>> getInputDescriptor(String streamId,
      Serde<ValueType> valueSerde) {
    return new KinesisInputDescriptor<>(streamId, valueSerde, this);
  }

  /**
   * Kinesis region for this system.
   * @param region Kinesis region
   * @return this system descriptor
   */
  public KinesisSystemDescriptor withRegion(String region) {
    this.region = Optional.of(region);
    return this;
  }

  /**
   * AWS config for this system. This is not required by default.
   * @param awsConfig A map of specified AWS configs
   * @return this system descriptor
   */
  public KinesisSystemDescriptor withAWSConfig(Map<String, String> awsConfig) {
    this.awsConfig = awsConfig;
    return this;
  }

  /**
   * KCL (Kinesis Client Library) config for this system. This is not required by default.
   * @param kclConfig A map of specified KCL configs
   * @return this system descriptor
   */
  public KinesisSystemDescriptor withKCLConfig(Map<String, String> kclConfig) {
    this.kclConfig = kclConfig;
    return this;
  }

  /**
   * Proxy host to be used for this system.
   * @param proxyHost Proxy host
   * @return this system descriptor
   */
  public KinesisSystemDescriptor withProxyHost(String proxyHost) {
    this.proxyHost = Optional.of(proxyHost);
    return this;
  }

  /**
   * Proxy port to be used for this system.
   * @param proxyPort Proxy port
   * @return this system descriptor
   */
  public KinesisSystemDescriptor withProxyPort(int proxyPort) {
    this.proxyPort = Optional.of(proxyPort);
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    final Map<String, String> config = new HashMap<>(super.toConfig());
    final String systemName = getSystemName();

    this.region.ifPresent(
        val -> config.put(String.format(KinesisConfig.CONFIG_SYSTEM_REGION, systemName), val));
    this.proxyHost.ifPresent(
        val -> config.put(String.format(KinesisConfig.CONFIG_PROXY_HOST, systemName), val));
    this.proxyPort.ifPresent(
        val -> config.put(String.format(KinesisConfig.CONFIG_PROXY_PORT, systemName), String.valueOf(val)));

    final String kclConfigPrefix = String.format(KinesisConfig.CONFIG_SYSTEM_KINESIS_CLIENT_LIB_CONFIG, systemName);
    this.kclConfig.forEach((k, v) -> config.put(kclConfigPrefix + k, v));

    final String awsConfigPrefix = String.format(KinesisConfig.CONFIG_AWS_CLIENT_CONFIG, systemName);
    this.awsConfig.forEach((k, v) -> config.put(awsConfigPrefix + k, v));

    return config;
  }
}
