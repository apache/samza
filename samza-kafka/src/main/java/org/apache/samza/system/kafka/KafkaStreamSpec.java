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
import java.util.Properties;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.system.StreamSpec;


/**
 * Extends StreamSpec with the ability to easily get the topic replication factor.
 */
public class KafkaStreamSpec extends StreamSpec {
  private static final int DEFAULT_REPLICATION_FACTOR = 2;

  /**
   * The number of replicas for stream durability.
   */
  private final int replicationFactor;

  /**
   * Convenience method to convert a config map to Properties.
   * @param map The Map to convert.
   * @return    The Properties instance.
   */
  private static Properties mapToProperties(Map<String, String> map) {
    Properties props = new Properties();
    props.putAll(map);
    return props;
  }

  /**
   * Convenience method to convert Properties to a config map.
   * @param properties  The Properties to convert.
   * @return            The Map instance.
   */
  private static Map<String, String> propertiesToMap(Properties properties) {
    Map<String, String> map = new HashMap<String, String>();
    for (final String name: properties.stringPropertyNames()) {
      map.put(name, properties.getProperty(name));
    }
    return map;
  }

  /**
   * Converts any StreamSpec to a KafkaStreamSpec.
   * If the original spec already is a KafkaStreamSpec, it is simply returned.
   *
   * @param originalSpec  The StreamSpec instance to convert to KafkaStreamSpec.
   * @return              A KafkaStreamSpec instance.
   */
  public static KafkaStreamSpec fromSpec(StreamSpec originalSpec) {
    if (originalSpec instanceof KafkaStreamSpec) {
      return ((KafkaStreamSpec) originalSpec);
    }

    int replicationFactor = Integer.parseInt(originalSpec.getOrDefault( KafkaConfig.TOPIC_REPLICATION_FACTOR(),
                                                                        KafkaConfig.TOPIC_DEFAULT_REPLICATION_FACTOR()));

    return new KafkaStreamSpec( originalSpec.getId(),
                                originalSpec.getPhysicalName(),
                                originalSpec.getSystemName(),
                                originalSpec.getPartitionCount(),
                                replicationFactor,
                                mapToProperties(originalSpec.getConfig()));
  }

  /**
   * Convenience constructor to create a KafkaStreamSpec with just a topicName, systemName, and partitionCount.
   *
   * @param topicName       The name of the topic.
   * @param systemName      The name of the System. See {@link org.apache.samza.system.SystemFactory}
   * @param partitionCount  The number of partitions.
   */
  public KafkaStreamSpec(String topicName, String systemName, int partitionCount) {
    this(topicName, topicName, systemName, partitionCount, DEFAULT_REPLICATION_FACTOR, new Properties());
  }

  /**
   * Constructs a StreamSpec with a replication factor.
   *
   * @param id                The application-unique logical identifier for the stream. It is used to distinguish between
   *                          streams in a Samza application so it must be unique in the context of one deployable unit.
   *                          It does not need to be globally unique or unique with respect to a host.
   *
   * @param topicName         The physical identifier for the stream. This is the identifier that will be used in remote
   *                          systems to identify the stream. In Kafka this would be the topic name whereas in HDFS it
   *                          might be a file URN.
   *
   * @param systemName        The System name on which this stream will exist. Corresponds to a named implementation of the
   *                          Samza System abstraction. See {@link org.apache.samza.system.SystemFactory}
   *
   * @param partitionCount    The number of partitionts for the stream. A value of {@code 1} indicates unpartitioned.
   *
   * @param replicationFactor The number of topic replicas in the Kafka cluster for durability.
   *
   * @param properties        A set of properties for the stream. These may be System-specfic.
   */
  public KafkaStreamSpec(String id, String topicName, String systemName, int partitionCount, int replicationFactor,
      Properties properties) {
    super(id, topicName, systemName, partitionCount, propertiesToMap(properties));

    if (replicationFactor <= 0) {
      throw new IllegalArgumentException(
          String.format("Replication factor %d must be greater than 0.", replicationFactor));
    }
    this.replicationFactor = replicationFactor;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public Properties getProperties() {
    return mapToProperties(getConfig());
  }
}
