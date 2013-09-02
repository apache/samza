---
layout: page
title: Checkpointing
---

On the [Streams](streams.html) page, on important detail was glossed over. When a TaskRunner instantiates a SystemConsumer for an input stream/partition pair, how does the TaskRunner know where in the stream to start reading messages. If you recall, Kafka has the concept of an offset, which defines a specific location in a topic/partition pair. The idea is that an offset can be used to reference a specific point in a stream/partition pair. When you read messages from Kafka, you can supply an offset to specify at which point you'd like to read from. After you read, you increment your offset, and get the next message.

![diagram](/img/0.7.0/learn/documentation/container/checkpointing.png)

This diagram looks the same as on the [Streams](streams.html) page, except that there are black lines at different points in each input stream/partition pair. These lines represent the current offset for each stream consumer. As the stream consumer reads, the offset increases, and moves closer to the "head" of the stream. The diagram also illustrates that the offsets might be staggered, such that some offsets are farther along in their stream/partition than others.

If a SystemConsumer is reading messages for a TaskRunner, and the TaskRunner stops for some reason (due to hardware failure, re-deployment, or whatever), the SystemConsumer should start where it left off when the TaskRunner starts back up again. We're able to do this because the Kafka broker is buffering messages on a remote server (the broker). Since the messages are available when we come back, we can just start from our last offset, and continue moving forward, without losing data.

The TaskRunner supports this ability using something called a CheckpointManager.

```
public interface CheckpointManager {
  void start();

  void register(Partition partition);

  void writeCheckpoint(Partition partition, Checkpoint checkpoint);

  Checkpoint readLastCheckpoint(Partition partition);

  void stop();
}

public class Checkpoint {
  private final Map<SystemStream, String> offsets;
  ...
}
```

As you can see, the checkpoint manager provides a way to write out checkpoints for a given partition. Right now, the checkpoint contains a map. The map's keys are input stream names, and the map's values are each input stream's offset. Each checkpoint is managed per-partition. For example, if you have page-view-event and service-metric-event defined as streams in your Samza job's configuration file, the TaskRunner would supply a checkpoint with two keys in each checkpoint offset map (one for page-view-event and the other for service-metric-event).

Samza provides two checkpoint managers: FileSystemCheckpointManager and KafkaCheckpointManager. The KafkaCheckpointManager is what you generally want to use. The way that KafkaCheckpointManager works is as follows: it writes checkpoint messages for your Samza job to a special Kafka topic. This topic's name is \_\_samza\_checkpoint\_your-job-name. For example, if you had a Samza job called "my-first-job", the Kafka topic would be called \_\_samza\_checkpoint\_my-first-job. This Kafka topic is partitioned identically to your Samza job's partition count. If your Samza job has 10 partitions, the checkpoint topic for your Samza job will also have 10 partitions. Every time that the TaskRunner calls writeCheckpoint, a checkpoint message will be sent to the partition that corresponds with the partition for the checkpoint that the TaskRunner wishes to write.

![diagram](/img/0.7.0/learn/documentation/container/checkpointing-2.png)

When the TaskRunner starts for the first time, the offset behavior of the SystemConsumers is undefined. If the system for the SystemConsumer is Kafka, we fall back to the auto.offset.reset setting. If the auto.offset.reset is set to "largest", we start reading messages from the head of the stream; if it's set to "smallest", we read from the tail. If it's undefined, the TaskRunner will fail.

The TaskRunner calls writeCheckpoint at a windowed interval (e.g. every 10 seconds). If the TaskRunner fails, and restarts, it simply calls readLastCheckpoint for each partition. In the case of the KafkaCheckpointManager, this readLastCheckpoint method will read the last message that was written to the checkpoint topic for each partition in the job. One edge case to consider is that SystemConsumers might have read messages from an offset that hasn't yet been checkpointed. In such a case, when the TaskRunner reads the last checkpoint for each partition, the offsets might be farther back in the stream. When this happens, your StreamTask could get duplicate messages (i.e. it saw message X, failed, restarted at an offset prior to message X, and then reads message X again). Thus, Samza currently provides at least once messaging. You might get duplicates. Caveat emptor.

<!-- TODO Add a link to the fault tolerance SEP when one exists -->

*Note that there are design proposals in the works to give exactly once messaging.*

## [State Management &raquo;](state-management.html)
