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

package org.apache.samza.system;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * StreamSpec is a blueprint for creating, validating, or simply describing a stream in the runtime environment.
 *
 * It has specific attributes for common behaviors that Samza uses.
 *
 * It also includes a map of configurations which may be system-specific.
 *
 * It is immutable by design.
 */
public class StreamSpec {

  private static final int DEFAULT_PARTITION_COUNT = 1;

  /**
   * Unique identifier for the stream in a Samza application.
   * This identifier is used as a key for stream properties in the
   * job config and to distinguish between streams in a graph.
   */
  private String id;

  /**
   * The System name on which this stream will exist. Corresponds to a named implementation of the
   * Samza System abstraction.
   */
  private String systemName;

  /**
   * The physical identifier for the stream. This is the identifier that will be used in remote
   * systems to identify the stream. In Kafka this would be the topic name whereas in HDFS it
   * might be a file URN.
   */
  private String physicalName;

  /**
   * The number of partitions for the stream.
   */
  private int partitionCount;

  /**
   * A set of all system-specific configurations for the stream.
   */
  private Map<String, String> config;

  /**
   *  @param id           The application-unique logical identifier for the stream. It is used to distinguish between
   *                      streams in a Samza application so it must be unique in the context of one deployable unit.
   *                      It does not need to be globally unique or unique with respect to a host.
   *
   * @param physicalName  The physical identifier for the stream. This is the identifier that will be used in remote
   *                      systems to identify the stream. In Kafka this would be the topic name whereas in HDFS it
   *                      might be a file URN.
   *
   * @param systemName    The System name on which this stream will exist. Corresponds to a named implementation of the
   *                      Samza System abstraction. See {@link SystemFactory}
   */
  public StreamSpec(String id, String physicalName, String systemName) {
    this(id, physicalName, systemName, DEFAULT_PARTITION_COUNT, Collections.emptyMap());
  }

  /**
   *
   *  @param id           The application-unique logical identifier for the stream. It is used to distinguish between
   *                      streams in a Samza application so it must be unique in the context of one deployable unit.
   *                      It does not need to be globally unique or unique with respect to a host.
   *
   * @param physicalName  The physical identifier for the stream. This is the identifier that will be used in remote
   *                      systems to identify the stream. In Kafka this would be the topic name whereas in HDFS it
   *                      might be a file URN.
   *
   * @param systemName    The System name on which this stream will exist. Corresponds to a named implementation of the
   *                      Samza System abstraction. See {@link SystemFactory}
   *
   * @param partitionCount  The number of partitionts for the stream. A value of {@code 1} indicates unpartitioned.
   */
  public StreamSpec(String id, String physicalName, String systemName, int partitionCount) {
    this(id, physicalName, systemName, partitionCount, Collections.emptyMap());
  }

  /**
   *  @param id           The application-unique logical identifier for the stream. It is used to distinguish between
   *                      streams in a Samza application so it must be unique in the context of one deployable unit.
   *                      It does not need to be globally unique or unique with respect to a host.
   *
   * @param physicalName  The physical identifier for the stream. This is the identifier that will be used in remote
   *                      systems to identify the stream. In Kafka this would be the topic name whereas in HDFS it
   *                      might be a file URN.
   *
   * @param systemName    The System name on which this stream will exist. Corresponds to a named implementation of the
   *                      Samza System abstraction. See {@link SystemFactory}
   *
   * @param config        A map of properties for the stream. These may be System-specfic.
   */
  public StreamSpec(String id, String physicalName, String systemName, Map<String, String> config) {
    this(id, physicalName, systemName, DEFAULT_PARTITION_COUNT, config);
  }

  /**
   *  @param id             The application-unique logical identifier for the stream. It is used to distinguish between
   *                        streams in a Samza application so it must be unique in the context of one deployable unit.
   *                        It does not need to be globally unique or unique with respect to a host.
   *
   * @param physicalName    The physical identifier for the stream. This is the identifier that will be used in remote
   *                        systems to identify the stream. In Kafka this would be the topic name whereas in HDFS it
   *                        might be a file URN.
   *
   * @param systemName      The System name on which this stream will exist. Corresponds to a named implementation of the
   *                        Samza System abstraction. See {@link SystemFactory}
   *
   * @param partitionCount  The number of partitionts for the stream. A value of {@code 1} indicates unpartitioned.
   *
   * @param config          A map of properties for the stream. These may be System-specfic.
   */
  public StreamSpec(String id, String physicalName, String systemName, int partitionCount,  Map<String, String> config) {
    validateLogicalIdentifier("streamId", id);
    validateLogicalIdentifier("systemName", systemName);

    if (partitionCount < 1) {
      throw new IllegalArgumentException("Parameter 'partitionCount' must be greater than 0");
    }

    this.id = id;
    this.systemName = systemName;
    this.physicalName = physicalName;
    this.partitionCount = partitionCount;

    if (config != null) {
      this.config = Collections.unmodifiableMap(new HashMap<>(config));
    } else {
      this.config = Collections.emptyMap();
    }
  }

  /**
   * Copies this StreamSpec, but applies a new partitionCount.
   *
   * This method is not static s.t. subclasses can override it.
   *
   * @param partitionCount  The partitionCount for the returned StreamSpec.
   * @return                A copy of this StreamSpec with the specified partitionCount.
   */
  public StreamSpec copyWithPartitionCount(int partitionCount) {
    return new StreamSpec(id, physicalName, systemName, partitionCount, config);
  }

  public String getId() {
    return id;
  }

  public String getSystemName() {
    return systemName;
  }

  public String getPhysicalName() {
    return physicalName;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public String get(String propertyName) {
    return config.get(propertyName);
  }

  public String getOrDefault(String propertyName, String defaultValue) {
    return config.getOrDefault(propertyName, defaultValue);
  }

  public SystemStream toSystemStream() {
    return new SystemStream(systemName, physicalName);
  }

  private void validateLogicalIdentifier(String identifierName, String identifierValue) {
    if (identifierValue == null || !identifierValue.matches("[A-Za-z0-9_-]+")) {
      throw new IllegalArgumentException(String.format("Identifier '%s' is '%s'. It must match the expression [A-Za-z0-9_-]+", identifierName, identifierValue));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !getClass().equals(o.getClass())) return false;

    StreamSpec that = (StreamSpec) o;

    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  public StreamSpec() {
  }
}
