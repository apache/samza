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
package org.apache.samza.util;

import java.util.List;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;


public class KafkaUtil {
  private static final int CHECKPOINT_LOG_VERSION_NUMBER = 1;
  /**
   * Need to fill in checkpoint log version number, job name, job id.
   */
  private static final String CHECKPOINT_TOPIC_FORMAT = "__samza_checkpoint_ver_%d_for_%s_%s";

  public static String getCheckpointTopic(String jobName, String jobId, Config config) {
    String checkpointTopic = String.format(CHECKPOINT_TOPIC_FORMAT, CHECKPOINT_LOG_VERSION_NUMBER,
        jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"));
    return StreamManager.createUniqueNameForBatch(checkpointTopic, config);
  }

  /**
   * Partition key in the envelope must not be null.
   */
  public static Integer getIntegerPartitionKey(OutgoingMessageEnvelope envelope, List<PartitionInfo> partitions) {
    int numPartitions = partitions.size();
    return abs(envelope.getPartitionKey().hashCode()) % numPartitions;
  }

  public static SystemStreamPartition toSystemStreamPartition(String systemName, TopicPartition topicPartition) {
    Partition partition = new Partition(topicPartition.partition());
    return new SystemStreamPartition(systemName, topicPartition.topic(), partition);
  }

  private static int abs(int n) {
    return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
  }
}
