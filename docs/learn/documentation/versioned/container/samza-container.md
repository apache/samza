---
layout: page
title: SamzaContainer
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

The SamzaContainer is responsible for managing the startup, execution, and shutdown of one or more [StreamTask](../api/overview.html) instances. Each SamzaContainer typically runs as an indepentent Java virtual machine. A Samza job can consist of several SamzaContainers, potentially running on different machines.

When a SamzaContainer starts up, it does the following:

1. Get last checkpointed offset for each input stream partition that it consumes
2. Create a "reader" thread for every input stream partition that it consumes
3. Start metrics reporters to report metrics
4. Start a checkpoint timer to save your task's input stream offsets every so often
5. Start a window timer to trigger your task's [window method](../api/javadocs/org/apache/samza/task/WindowableTask.html), if it is defined
6. Instantiate and initialize your StreamTask once for each input stream partition
7. Start an event loop that takes messages from the input stream reader threads, and gives them to your StreamTasks
8. Notify lifecycle listeners during each one of these steps

Let's start in the middle, with the instantiation of a StreamTask. The following sections of the documentation cover the other steps.

### Tasks and Partitions

When the container starts, it creates instances of the [task class](../api/overview.html) that you've written. If the task class implements the [InitableTask](../api/javadocs/org/apache/samza/task/InitableTask.html) interface, the SamzaContainer will also call the init() method.

{% highlight java %}
/** Implement this if you want a callback when your task starts up. */
public interface InitableTask {
  void init(Config config, TaskContext context);
}
{% endhighlight %}

By default, how many instances of your task class are created depends on the number of partitions in the job's input streams. If your Samza job has ten partitions, there will be ten instantiations of your task class: one for each partition. The first task instance will receive all messages for partition one, the second instance will receive all messages for partition two, and so on.

<img src="/img/{{site.version}}/learn/documentation/container/tasks-and-partitions.svg" alt="Illustration of tasks consuming partitions" class="diagram-large">

The number of partitions in the input streams is determined by the systems from which you are consuming. For example, if your input system is Kafka, you can specify the number of partitions when you create a topic from the command line or using the num.partitions in Kafka's server properties file.

If a Samza job has more than one input stream, the number of task instances for the Samza job is the maximum number of partitions across all input streams. For example, if a Samza job is reading from PageViewEvent (12 partitions), and ServiceMetricEvent (14 partitions), then the Samza job would have 14 task instances (numbered 0 through 13). Task instances 12 and 13 only receive events from ServiceMetricEvent, because there is no corresponding PageViewEvent partition.

With this default approach to assigning input streams to task instances, Samza is effectively performing a group-by operation on the input streams with their partitions as the key. Other strategies for grouping input stream partitions are possible by implementing a new [SystemStreamPartitionGrouper](../api/javadocs/org/apache/samza/container/grouper/stream/SystemStreamPartitionGrouper.html) and factory, and configuring the job to use it via the job.systemstreampartition.grouper.factory configuration value.

Samza provides the above-discussed per-partition grouper as well as the GroupBySystemStreamPartitionGrouper, which provides a separate task class instance for every input stream partition, effectively grouping by the input stream itself. This provides maximum scalability in terms of how many containers can be used to process those input streams and is appropriate for very high volume jobs that need no grouping of the input streams.

Considering the above example of a PageViewEvent partitioned 12 ways and a ServiceMetricEvent partitioned 14 ways, the GroupBySystemStreamPartitionGrouper would create 12 + 14 = 26 task instances, which would then be distributed across the number of containers configured, as discussed below.

Note that once a job has been started using a particular SystemStreamPartitionGrouper and that job is using state or checkpointing, it is not possible to change that grouping in subsequent job starts, as the previous checkpoints and state information would likely be incorrect under the new grouping approach.

### Containers and resource allocation

Although the number of task instances is fixed &mdash; determined by the number of input partitions &mdash; you can configure how many containers you want to use for your job. If you are [using YARN](../jobs/yarn-jobs.html), the number of containers determines what CPU and memory resources are allocated to your job.

If the data volume on your input streams is small, it might be sufficient to use just one SamzaContainer. In that case, Samza still creates one task instance per input partition, but all those tasks run within the same container. At the other extreme, you can create as many containers as you have partitions, and Samza will assign one task instance to each container.

Each SamzaContainer is designed to use one CPU core, so it uses a [single-threaded event loop](event-loop.html) for execution. It's not advisable to create your own threads within a SamzaContainer. If you need more parallelism, please configure your job to use more containers.

Any [state](state-management.html) in your job belongs to a task instance, not to a container. This is a key design decision for Samza's scalability: as your job's resource requirements grow and shrink, you can simply increase or decrease the number of containers, but the number of task instances remains unchanged. As you scale up or down, the same state remains attached to each task instance. Task instances may be moved from one container to another, and any persistent state managed by Samza will be moved with it. This allows the job's processing semantics to remain unchanged, even as you change the job's parallelism.

### Joining multiple input streams

If your job has multiple input streams, Samza provides a simple but powerful mechanism for joining data from different streams: each task instance receives messages from one partition of *each* of the input streams. For example, say you have two input streams, A and B, each with four partitions. Samza creates four task instances to process them, and assigns the partitions as follows:

<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Task instance</th>
      <th>Consumes stream partitions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td><td>stream A partition 0, stream B partition 0</td>
    </tr>
    <tr>
      <td>1</td><td>stream A partition 1, stream B partition 1</td>
    </tr>
    <tr>
      <td>2</td><td>stream A partition 2, stream B partition 2</td>
    </tr>
    <tr>
      <td>3</td><td>stream A partition 3, stream B partition 3</td>
    </tr>
  </tbody>
</table>

Thus, if you want two events in different streams to be processed by the same task instance, you need to ensure they are sent to the same partition number. You can achieve this by using the same partitioning key when [sending the messages](../api/overview.html). Joining streams is discussed in detail in the [state management](state-management.html) section.

There is one caveat in all of this: Samza currently assumes that a stream's partition count will never change. Partition splitting or repartitioning is not supported. If an input stream has N partitions, it is expected that it has always had, and will always have N partitions. If you want to re-partition a stream, you can write a job that reads messages from the stream, and writes them out to a new stream with the required number of partitions. For example, you could read messages from PageViewEvent, and write them to PageViewEventRepartition.

### Broadcast Streams

After 0.10.0, Samza supports broadcast streams. You can assign partitions from some streams to all the tasks. For example, you want all the tasks can consume partition 0 and 1 from a stream called global-stream-1, and partition 2 from a stream called global-stream-2. You now can configure:

{% highlight jproperties %}
task.broadcast.inputs=yourSystem.broadcast-stream-1#[0-1], yourSystem.broadcast-stream-2#2 
{% endhighlight %}

If you use "[]", you are specifying a range.

## [Streams &raquo;](streams.html)
