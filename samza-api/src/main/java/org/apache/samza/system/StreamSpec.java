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

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Blueprint for creating a stream in the runtime environment.
 * Has some generic attributes that describe common behaviors that samza uses.
 * Also includes a set of system-specific properties.
 */
public class StreamSpec {
  /**
   * Unique, user-defined identifier for the stream. Can be used to bind to config.
   */
  private final String id;

  /**
   * The name of the stream.
   */
  private final String name;

  /**
   * The number of partitions in the stream.
   * Use 1 for unpartitioned streams and a negative number to set the
   * initial value for dynamic partitioning.
   */
  private int partitionCount = 1;

  /**
   * The number of replicas for stream durability.
   */
  private int replicationFactor = 2;

  /**
   * Not currently used because we need to do 2-phase deprecation of methods to create changelog/coordinator stream.
   * Then we can update the code to request streams with these abstract properties and have the system interpret them,
   * rather than expecting the system to know what properties are expected of a coordinator stream, for example.
   *
   * Indicates whether the stream is to be treated as a table
   */
  private boolean deduplicated = false;

  /**
   * Not currently used because we need to do 2-phase deprecation of methods to create changelog/coordinator stream.
   * Then we can update the code to request streams with these abstract properties and have the system interpret them,
   * rather than expecting the system to know what properties are expected of a coordinator stream, for example.
   *
   * Expected size of the stream in bytes or -1 for unknown. Enables optimizations in some systems.
   * For streams with time retention, calculate the expected retained size.
   *
   * For chkpt, it depends on the tasks & inputs and frequency, but avg around 2MB over 1hr
   * Coord also depends on size, but tends to be around 800KB over 2 days (infrequent restarts)
   */
  private long sizeBytes = -1;

  /**
   * Not currently used because we need to do 2-phase deprecation of methods to create changelog/coordinator stream.
   * Then we can update the code to request streams with these abstract properties and have the system interpret them,
   * rather than expecting the system to know what properties are expected of a coordinator stream, for example.
   *
   * If the stream retains tombstones for deletions, this defines the ttl for those tombstones.
   */
  private long tombstoneTtlMs = TimeUnit.DAYS.toMillis(1);

  /**
   * A set of all system-specific properties for the stream.
   */
  private final Properties properties = new Properties();

  public StreamSpec(String id) {
    this(id, id);
  }

  public StreamSpec(String id, String streamName) {
    if (streamName == null) {
      throw new NullPointerException("Parameter 'streamName' must not be null");
    }

    this.id = id;
    this.name = streamName;
  }

  public StreamSpec(String id, String streamName, int partitionCount, int replicationFactor, Properties properties) {
    this(id, streamName);
    this.partitionCount = partitionCount;
    this.replicationFactor = replicationFactor;
    this.properties.putAll(properties);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public long getSizeBytes() {
    return sizeBytes;
  }

  public void setSizeBytes(long sizeBytes) {
    this.sizeBytes = sizeBytes;
  }

  public long getTombstoneTtlMs() {
    return tombstoneTtlMs;
  }

  public void setTombstoneTtlMs(long tombstoneTtlMs) {
    this.tombstoneTtlMs = tombstoneTtlMs;
  }

  public boolean isDeduplicated() {
    return deduplicated;
  }

  public void setDeduplicated(boolean deduplicated) {
    this.deduplicated = deduplicated;
  }

  public Properties getProperties() {
    return properties;
  }

  public String get(String propertyName) {
    return properties.getProperty(propertyName);
  }

  public Object set(String propertyName, String propertyValue) {
    return properties.setProperty(propertyName, propertyValue);
  }

}
