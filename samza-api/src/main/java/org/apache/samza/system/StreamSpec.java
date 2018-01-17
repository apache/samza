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

  // Internal changelog stream id. It is used for creating changelog StreamSpec.
  private static final String CHANGELOG_STREAM_ID = "samza-internal-changelog-stream-id";

  // Internal coordinator stream id. It is used for creating coordinator StreamSpec.
  private static final String COORDINATOR_STREAM_ID = "samza-internal-coordinator-stream-id";

  // Internal checkpoint stream id. It is used for creating checkpoint StreamSpec.
  private static final String CHECKPOINT_STREAM_ID = "samza-internal-checkpoint-stream-id";

  /**
   * Unique identifier for the stream in a Samza application.
   * This identifier is used as a key for stream properties in the
   * job config and to distinguish between streams in a graph.
   */
  private final String id;

  /**
   * The System name on which this stream will exist. Corresponds to a named implementation of the
   * Samza System abstraction.
   */
  private final String systemName;

  /**
   * The physical identifier for the stream. This is the identifier that will be used in remote
   * systems to identify the stream. In Kafka this would be the topic name whereas in HDFS it
   * might be a file URN.
   */
  private final String physicalName;

  /**
   * The number of partitions for the stream.
   */
  private final int partitionCount;

  /**
   * Bounded or unbounded stream
   */
  private final boolean isBounded;

  /**
   * broadcast stream to all tasks
   */
  private final boolean isBroadcast;

  /**
   * A set of all system-specific configurations for the stream.
   */
  private final Map<String, String> config;

  @Override
  public String toString() {
    return String.format("StreamSpec: id=%s, systemName=%s, pName=%s, partCount=%d.", id, systemName, physicalName, partitionCount);
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
   */
  public StreamSpec(String id, String physicalName, String systemName) {
    this(id, physicalName, systemName, DEFAULT_PARTITION_COUNT, false, false, Collections.emptyMap());
  }

  /**
   * @see {@link StreamSpec#StreamSpec(String, String, String, int, boolean, boolean, Map)}
   */
  public StreamSpec(String id, String physicalName, String systemName, int partitionCount) {
    this(id, physicalName, systemName, partitionCount, false, false, Collections.emptyMap());
  }

  /**
   * @see {@link StreamSpec#StreamSpec(String, String, String, int, boolean, boolean, Map)}
   */
  public StreamSpec(String id, String physicalName, String systemName, boolean isBounded, Map<String, String> config) {
    this(id, physicalName, systemName, DEFAULT_PARTITION_COUNT, isBounded, false, config);
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
   * @param isBounded       The stream is bounded or not.
   *
   * @param isBroadcast     This stream is broadcast or not.
   *
   * @param config          A map of properties for the stream. These may be System-specfic.
   */
  public StreamSpec(String id, String physicalName, String systemName, int partitionCount,
                    boolean isBounded, boolean isBroadcast, Map<String, String> config) {
    validateLogicalIdentifier("streamId", id);
    validateLogicalIdentifier("systemName", systemName);

    // partition count being 0 is a valid use case in Hadoop when the output stream is an empty folder
    if (partitionCount < 0) {
      throw new IllegalArgumentException("Parameter 'partitionCount' must be >= 0");
    }

    this.id = id;
    this.systemName = systemName;
    this.physicalName = physicalName;
    this.partitionCount = partitionCount;
    this.isBounded = isBounded;
    this.isBroadcast = isBroadcast;

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
    return new StreamSpec(id, physicalName, systemName, partitionCount, this.isBounded, this.isBroadcast, config);
  }

  public StreamSpec copyWithPhysicalName(String physicalName) {
    return new StreamSpec(id, physicalName, systemName, partitionCount, this.isBounded, this.isBroadcast, config);
  }

  public StreamSpec copyWithBroadCast() {
    return new StreamSpec(id, physicalName, systemName, partitionCount, this.isBounded, true, config);
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

  public boolean isChangeLogStream() {
    return id.equals(CHANGELOG_STREAM_ID);
  }

  public boolean isCoordinatorStream() {
    return id.equals(COORDINATOR_STREAM_ID);
  }

  public boolean isBounded() {
    return isBounded;
  }

  public boolean isBroadcast() {
    return isBroadcast;
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

  public static StreamSpec createChangeLogStreamSpec(String physicalName, String systemName, int partitionCount) {
    return new StreamSpec(CHANGELOG_STREAM_ID, physicalName, systemName, partitionCount);
  }

  public static StreamSpec createCoordinatorStreamSpec(String physicalName, String systemName) {
    return new StreamSpec(COORDINATOR_STREAM_ID, physicalName, systemName, 1);
  }

  public static StreamSpec createCheckpointStreamSpec(String physicalName, String systemName) {
    return new StreamSpec(CHECKPOINT_STREAM_ID, physicalName, systemName, 1);
  }
}
