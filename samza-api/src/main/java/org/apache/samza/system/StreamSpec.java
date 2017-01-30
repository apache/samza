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


/**
 * Blueprint for creating a stream in the runtime environment.
 * Has some generic attributes that describe common behaviors that samza uses.
 * Also includes a set of system-specific properties.
 */
public class StreamSpec {

  /**
   * Unique, user-defined identifier for the stream in Samza.
   * This identifier is used to set stream properties in
   * config and to distinguish between streams in a graph.
   */
  private final String id;

  /**
   * The system on which this stream exists or will exist.
   */
  private final String system;

  /**
   * Identifier for the stream in the external system (if applicable). e.g. in Kafka
   * this would be the topic physicalName, whereas for HDFS it might be the file URN.
   */
  private String physicalName;

  /**
   * The number of partitions for the stream.
   */
  private int partitionCount = 1;

  /**
   * A set of all system-specific properties for the stream.
   */
  private final Properties properties = new Properties();

  public StreamSpec(String id, String system, Properties properties) {
    if (id == null) {
      throw new NullPointerException("Parameter 'id' must not be null");
    }

    if (system == null) {
      throw new NullPointerException("Parameter 'system' must not be null");
    }

    this.id = id;
    this.system = system;

    if (properties != null) {
      this.properties.putAll(properties);
    }
  }

  public StreamSpec(String id, String system, String physicalName, Properties properties) {
    this(id, system, properties);
    this.physicalName = physicalName;
  }

  public StreamSpec(String id, String system, String physicalName, int partitionCount, Properties properties) {
    this(id, system, physicalName, properties);
    this.partitionCount = partitionCount;
  }

  public String getId() {
    return id;
  }


  public String getSystem() {
    return system;
  }

  public String getPhysicalName() {
    return physicalName;
  }

  public void setPhysicalName(String physicalName) {
    this.physicalName = physicalName;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public Properties getProperties() {
    return properties;
  }

  public String get(String propertyName) {
    return properties.getProperty(propertyName);
  }

  public String getOrDefault(String propertyName, String defaultValue) {
    return properties.getProperty(propertyName, defaultValue);
  }

  public Object set(String propertyName, String propertyValue) {
    return properties.setProperty(propertyName, propertyValue);
  }
}
