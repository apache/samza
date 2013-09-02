---
layout: page
title: TaskRunner
---
<!-- TODO: Is TaskRunner still appropriate terminology to use (appears to be a combo of SamzaContainer and TaskInstance in the code)? -->

The TaskRunner is Samza's stream processing container. It is responsible for managing the startup, execution, and shutdown of one or more StreamTask instances.

When the a TaskRunner starts up, it does the following:

1. Get last checkpointed offset for each input stream/partition pair
2. Create a "reader" thread for every input stream/partition pair
3. Start metrics reporters to report metrics
4. Start a checkpoint timer to save your task's input stream offsets every so often
5. Start a window timer to trigger your StreamTask's window method, if it is defined
6. Instantiate and initialize your StreamTask once for each input stream partition
7. Start an event loop that takes messages from the input stream reader threads, and gives them to your StreamTasks
8. Notify lifecycle listeners during each one of these steps

Let's go over each of these items, starting in the middle, with the instantiation of a StreamTask.

### Tasks and Partitions

When the TaskRunner starts, it creates an instance of the StreamTask that you've written. If the StreamTask implements the InitableTask interface, the TaskRunner will also call the init() method.

```
public interface InitableTask {
  void init(Config config, TaskContext context);
}
```

It doesn't just do this once, though. It creates the StreamTask once for each partition in your Samza job. If your Samza job has ten partitions, there will be ten instantiations of your StreamTask: one for each partition. The StreamTask instance for partition one will receive all messages for partition one, the instance for partition two will receive all messages for partition two, and so on.

The number of partitions that a Samza job has is determined by the number of partitions in its input streams. If a Samza job is set up to read from a topic called PageViewEvent, which has 12 partitions, then the Samza job will have 12 partitions when it executes.

![diagram](/img/0.7.0/learn/documentation/container/tasks-and-partitions.png)

If a Samza job has more than one input stream, then the number of partitions for the Samza job will be the maximum number of partitions across all input streams. For example, if a Samza job is reading from PageView event, which has 12 partitions, and ServiceMetricEvent, which has 14 partitions, then the Samza job would have 14 partitions (0 through 13).

When the TaskRunner's StreamConsumer threads are reading messages from each input stream partition, the messages that it receives are tagged with the partition number that it came from. Each message is fed to the StreamTask instance that corresponds to the message's partition. This design has two important properties. When a Samza job has more than one input stream, and those streams have an imbalanced number of partitions (e.g. one has 12 partitions and the other has 14), then some of your StreamTask instances will not receive messages from all streams. In the PageViewEvent/ServiceMetricEvent example, the last two StreamTask instances would only receive messages from the ServiceMetricEvent topic (partitions 12 and 13). The lower 12 instances would receive messages from both streams. If your Samza job is reading more than one input stream, you probably want all input streams to have the same number of partitions, especially if you're trying to join streams together. The second important property is that Samza assumes that a stream's partition count will never change. No partition splitting is supported. If an input stream has N partitions, it is expected that it has always had, and will always have N partitions. If you want to re-partition, you must read messages from the stream, and write them out to a new stream that has the number of partitions that you want. For example you could read messages from PageViewEvent, and write them to PageViewEventRepartition, which could have 14 partitions. If you did this, then you would achieve balance between PageViewEventRepartition and ServiceMetricEvent.

This design is important because it guarantees that any state that your StreamTask keeps in memory will be isolated on a per-partition basis. For example, if you refer back to the page-view counting job we used as an example in the [Architecture](../introduction/architecture.html) section, we might have a Map&lt;Integer, Integer&gt; map that keeps track of page view counts per-member ID. If we were to have just one StreamTask per Samza job, for instance, then the member ID counts from different partitions would be inter-mingled into the same map. This inter-mingling would prevent us from moving partitions between processes or machines, which is something that we want to do with YARN. You can imagine a case where you started with one TaskRunner in a single YARN container. Your Samza job might be unable to keep up with only one container, so you ask for a second YARN container to put some of the StreamTask partitions. In such a case, how would we split the counts such that one container gets only member ID counts for the partitions in charge of? This is effectively impossible if we've inter-mingled the StreamTask's state together. This is why we isolate StreamTask instances on a per-partition basis: to make partition migration possible.

## [Streams &raquo;](streams.html)
