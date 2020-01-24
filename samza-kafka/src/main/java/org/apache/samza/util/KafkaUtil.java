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

  public static String getCheckpointTopic(String jobName, String jobId, Config config) {
    String checkpointTopic = String.format("__samza_checkpoint_ver_%d_for_%s_%s", CHECKPOINT_LOG_VERSION_NUMBER,
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
