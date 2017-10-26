package org.apache.samza.checkpoint.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import kafka.common.KafkaException;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.CheckpointSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.kafka.KafkaStreamSpec;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created by jvenkatr on 10/24/17.
 */
public class TestKafkaCheckpointManager1 {
  private static final String CHECKPOINT_TOPIC = "topic-1";
  private static final String CHECKPOINT_SYSTEM = "system-1";
  private static final Partition CHECKPOINT_PARTITION = new Partition(0);

  private static final SystemStreamPartition CHECKPOINT_SSP = new SystemStreamPartition(CHECKPOINT_SYSTEM, CHECKPOINT_TOPIC, CHECKPOINT_PARTITION);
  private static final Checkpoint CHECKPOINT1 = new Checkpoint(ImmutableMap.of(CHECKPOINT_SSP, "offset-1"));
  private static final TaskName TASK1 = new TaskName("task1");

  @Test
  public void testStartFailsOnTopicValidationErrors() {

    KafkaStreamSpec checkpointSpec = mock(KafkaStreamSpec.class);
    when(checkpointSpec.getSystemName()).thenReturn(CHECKPOINT_SYSTEM);
    when(checkpointSpec.getPhysicalName()).thenReturn(CHECKPOINT_TOPIC);

    SystemProducer mockProducer = mock(SystemProducer.class);
    doNothing().when(mockProducer).start();

    SystemConsumer mockConsumer = mock(SystemConsumer.class);
    doNothing().when(mockConsumer).start();

    SystemStreamMetadata metadata = new SystemStreamMetadata(CHECKPOINT_TOPIC,
        ImmutableMap.of(new Partition(0), new SystemStreamPartitionMetadata("oldest",
            "newest", "upcoming")));

    SystemAdmin mockAdmin = mock(SystemAdmin.class);
    when(mockAdmin.getSystemStreamMetadata(Collections.singleton(CHECKPOINT_TOPIC))).thenReturn(
        ImmutableMap.of(CHECKPOINT_TOPIC, metadata));
    when(mockAdmin.createStream(checkpointSpec)).thenThrow(new StreamValidationException("invalid stream"));

    SystemFactory factory = createSystemFactory(mockProducer, mockConsumer, mockAdmin);
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(checkpointSpec, factory,
        true, mock(Config.class), mock(MetricsRegistry.class), null);

    // expect an exception
    checkpointManager.start();
  }

  @Test
  public void testSerdeErrorsShouldFailCheckpointRead() throws Exception {
    KafkaStreamSpec checkpointSpec = mock(KafkaStreamSpec.class);
    when(checkpointSpec.getSystemName()).thenReturn(CHECKPOINT_SYSTEM);
    when(checkpointSpec.getPhysicalName()).thenReturn(CHECKPOINT_TOPIC);

    SystemProducer mockProducer = mock(SystemProducer.class);
    doNothing().when(mockProducer).start();

    SystemConsumer mockConsumer = mock(SystemConsumer.class);
    doNothing().when(mockConsumer).start();
    ImmutableMap<SystemStreamPartition, List<IncomingMessageEnvelope>> of = ImmutableMap.of(CHECKPOINT_SSP, ImmutableList.of(new IncomingMessageEnvelope(CHECKPOINT_SSP, "", new KafkaCheckpointLogKeySerde().toBytes(new KafkaCheckpointLogKey("g1", TASK1, "checkpoint")), new CheckpointSerde().toBytes(CHECKPOINT1))));
    System.out.println("here" + mockConsumer);
    when(mockConsumer.poll(any(), any())).thenReturn(of);

    SystemStreamMetadata metadata = new SystemStreamMetadata(CHECKPOINT_TOPIC,
        ImmutableMap.of(new Partition(0), new SystemStreamPartitionMetadata("oldest",
            "newest", "upcoming")));

    SystemAdmin mockAdmin = mock(SystemAdmin.class);
    when(mockAdmin.getSystemStreamMetadata(Collections.singleton(CHECKPOINT_TOPIC))).thenReturn(
        ImmutableMap.of(CHECKPOINT_TOPIC, metadata));


    SystemFactory factory = createSystemFactory(mockProducer, mockConsumer, mockAdmin);
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(checkpointSpec, factory,
        true, mock(Config.class), mock(MetricsRegistry.class), new ExceptionThrowingCheckpointSerde());
    checkpointManager.register(TASK1);
    checkpointManager.start();

    // expect an exception
    checkpointManager.readLastCheckpoint(TASK1);
  }

  private SystemFactory createSystemFactory(SystemProducer producer, SystemConsumer consumer, SystemAdmin admin) {
    SystemFactory factory = mock(SystemFactory.class);
    when(factory.getProducer(any(), any(), any())).thenReturn(producer);
    when(factory.getConsumer(any(), any(), any())).thenReturn(consumer);
    when(factory.getAdmin(any(), any())).thenReturn(admin);
    return factory;
  }

  private static class ExceptionThrowingCheckpointSerde extends CheckpointSerde  {

    public Checkpoint fromBytes(byte[] bytes) {
      throw new KafkaException("exception");
    }
  }
}
