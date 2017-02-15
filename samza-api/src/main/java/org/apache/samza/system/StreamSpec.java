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
 * StreamSpec is a blueprint for creating, validating, or simply describing a stream in the runtime environment.
 *
 * It has specific attributes for common behaviors that samza uses.
 *
 * It also includes a set of properties which may be systemName-specific.
 */
public class StreamSpec {

  /**
   * Unique identifier for the stream in a Samza application.
   * This identifier is used as a key for stream properties in
   * config and to distinguish between streams in a graph.
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
  private String physicalName;

  /**
   * The number of partitions for the stream.
   */
  private int partitionCount = 1;

  /**
   * A set of all systemName-specific properties for the stream.
   */
  private final Properties properties = new Properties();

  /**
   * Base constructor.
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
 *                        Samza System abstraction. See {@link SystemFactory}
   *
   * @param properties    A set of properties for the stream. These may be System-specfic.
   */
  public StreamSpec(String id, String physicalName, String systemName, Properties properties) {
    if (id == null) {
      throw new NullPointerException("Parameter 'id' must not be null");
    }

    if (systemName == null) {
      throw new NullPointerException("Parameter 'systemName' must not be null");
    }

    this.id = id;
    this.systemName = systemName;
    this.physicalName = physicalName;

    if (properties != null) {
      this.properties.putAll(properties);
    }
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
   * @param properties      A set of properties for the stream. These may be System-specfic.
   */
  public StreamSpec(String id, String physicalName, String systemName, int partitionCount, Properties properties) {
    this(id, physicalName, systemName, properties);
    this.partitionCount = partitionCount;
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
