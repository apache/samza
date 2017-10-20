package org.apache.samza.checkpoint.kafka;

import org.apache.samza.checkpoint.kafka.KafkaCheckpointLogKey;
import org.apache.samza.checkpoint.kafka.KafkaCheckpointLogKeySerde;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by jvenkatr on 10/19/17.
 */
public class TestKafkaCheckpointLogKeySerde {

  @Test
  public void testBinaryCompatibility() {
    KafkaCheckpointLogKey logKey1 = new KafkaCheckpointLogKey(GroupByPartitionFactory.class.getCanonicalName(),
        new TaskName("Partition 0"), KafkaCheckpointLogKey.CHECKPOINT_TYPE);
    KafkaCheckpointLogKeySerde checkpointSerde = new KafkaCheckpointLogKeySerde();

    byte[] bytes = ("{\"systemstreampartition-grouper-factory\"" +
        ":\"org.apache.samza.container.grouper.stream.GroupByPartitionFactory\",\"taskName\":\"Partition 0\"," +
        "\"type\":\"checkpoint\"}").getBytes();

    Assert.assertEquals(true, Arrays.equals(bytes, checkpointSerde.toBytes(logKey1)));
  }

  @Test
  public void testSerDe() {
    KafkaCheckpointLogKey logKey1 = new KafkaCheckpointLogKey(GroupByPartitionFactory.class.getCanonicalName(),
        new TaskName("Partition 0"), KafkaCheckpointLogKey.CHECKPOINT_TYPE);
    KafkaCheckpointLogKeySerde checkpointSerde = new KafkaCheckpointLogKeySerde();
    Assert.assertEquals(logKey1, checkpointSerde.fromBytes(checkpointSerde.toBytes(logKey1)));
  }
}
