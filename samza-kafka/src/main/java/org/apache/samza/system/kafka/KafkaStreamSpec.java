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

import java.util.Properties;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.system.StreamSpec;


/**
 * Extends StreamSpec with the ability to easily get the topic replication factor.
 */
public class KafkaStreamSpec extends StreamSpec {
  private static final int DEFAULT_PARTITION_COUNT = 1;
  private static final int DEFAULT_REPLICATION_FACTOR = 2;

  /**
   * The number of replicas for stream durability.
   */
  private final int replicationFactor;

  public static KafkaStreamSpec fromSpec(StreamSpec other) {
    if (other instanceof KafkaStreamSpec) {
      return ((KafkaStreamSpec) other);
    }

    int replicationFactor = Integer.parseInt(other.getOrDefault(KafkaConfig.TOPIC_REPLICATION_FACTOR(), KafkaConfig.TOPIC_DEFAULT_REPLICATION_FACTOR()));
    return new KafkaStreamSpec(other.getId(), other.getSystem(), other.getPhysicalName(), other.getPartitionCount(), replicationFactor, other.getProperties());
  }

  public KafkaStreamSpec(String id, String system, String topicName) {
    this(id, system, topicName, DEFAULT_PARTITION_COUNT, DEFAULT_REPLICATION_FACTOR);
  }

  public KafkaStreamSpec(String topicName, String system, int partitionCount) {
    this(topicName, system, topicName, partitionCount, DEFAULT_REPLICATION_FACTOR);
  }

  public KafkaStreamSpec(String id, String system, String topicName, int partitionCount, int replicationFactor) {
    this(id, system, topicName, partitionCount, replicationFactor, new Properties());
  }

  public KafkaStreamSpec(String id, String system, String topicName, int partitionCount, int replicationFactor,
      Properties properties) {
    super(id, system, topicName, partitionCount, properties);

    if (replicationFactor <= 0) {
      throw new IllegalArgumentException(
          String.format("Replication factor %d must be greater than 0.", replicationFactor));
    }

    this.replicationFactor = replicationFactor;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }
}
