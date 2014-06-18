---
layout: page
title: Checkpointing
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

Samza provides fault-tolerant processing of streams: Samza guarantees that messages won't be lost, even if your job crashes, if a machine dies, if there is a network fault, or something else goes wrong. In order to provide this guarantee, Samza expects the [input system](streams.html) to meet the following requirements:

* The stream may be sharded into one or more *partitions*. Each partition is independent from the others, and is replicated across multiple machines (the stream continues to be available, even if a machine fails).
* Each partition consists of a sequence of messages in a fixed order. Each message has an *offset*, which indicates its position in that sequence. Messages are always consumed sequentially within each partition.
* A Samza job can start consuming the sequence of messages from any starting offset.

Kafka meets these requirements, but they can also be implemented with other message broker systems.

As described in the [section on SamzaContainer](samza-container.html), each task instance of your job consumes one partition of an input stream. Each task has a *current offset* for each input stream: the offset of the next message to be read from that stream partition. Every time a message is read from the stream, the current offset moves forwards.

If a Samza container fails, it needs to be restarted (potentially on another machine) and resume processing where the failed container left off. In order to enable this, a container periodically checkpoints the current offset for each task instance.

<img src="/img/0.7.0/learn/documentation/container/checkpointing.svg" alt="Illustration of checkpointing" class="diagram-large">

When a Samza container starts up, it looks for the most recent checkpoint and starts consuming messages from the checkpointed offsets. If the previous container failed unexpectedly, the most recent checkpoint may be slightly behind the current offsets (i.e. the job may have consumed some more messages since the last checkpoint was written), but we can't know for sure. In that case, the job may process a few messages again.

This guarantee is called *at-least-once processing*: Samza ensures that your job doesn't miss any messages, even if containers need to be restarted. However, it is possible for your job to see the same message more than once when a container is restarted. We are planning to address this in a future version of Samza, but for now it is just something to be aware of: for example, if you are counting page views, a forcefully killed container could cause events to be slightly over-counted. You can reduce duplication by checkpointing more frequently, at a slight performance cost.

For checkpoints to be effective, they need to be written somewhere where they will survive faults. Samza allows you to write checkpoints to the file system (using FileSystemCheckpointManager), but that doesn't help if the machine fails and the container needs to be restarted on another machine. The most common configuration is to use Kafka for checkpointing. You can enable this with the following job configuration:

{% highlight jproperties %}
# The name of your job determines the name under which checkpoints will be stored
job.name=example-job

# Define a system called "kafka" for consuming and producing to a Kafka cluster
systems.kafka.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory

# Declare that we want our job's checkpoints to be written to Kafka
task.checkpoint.factory=org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory
task.checkpoint.system=kafka

# By default, a checkpoint is written every 60 seconds. You can change this if you like.
task.commit.ms=60000
{% endhighlight %}

In this configuration, Samza writes checkpoints to a separate Kafka topic called \_\_samza\_checkpoint\_&lt;job-name&gt;\_&lt;job-id&gt; (in the example configuration above, the topic would be called \_\_samza\_checkpoint\_example-job\_1). Once per minute, Samza automatically sends a message to this topic, in which the current offsets of the input streams are encoded. When a Samza container starts up, it looks for the most recent offset message in this topic, and loads that checkpoint.

Sometimes it can be useful to use checkpoints only for some input streams, but not for others. In this case, you can tell Samza to ignore any checkpointed offsets for a particular stream name:

{% highlight jproperties %}
# Ignore any checkpoints for the topic "my-special-topic"
systems.kafka.streams.my-special-topic.samza.reset.offset=true

# Always start consuming "my-special-topic" at the oldest available offset
systems.kafka.streams.my-special-topic.samza.offset.default=oldest
{% endhighlight %}

The following table explains the meaning of these configuration parameters:

<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Parameter name</th>
      <th>Value</th>
      <th>Meaning</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="2" class="nowrap">systems.&lt;system&gt;.<br>streams.&lt;stream&gt;.<br>samza.reset.offset</td>
      <td>false (default)</td>
      <td>When container starts up, resume processing from last checkpoint</td>
    </tr>
    <tr>
      <td>true</td>
      <td>Ignore checkpoint (pretend that no checkpoint is present)</td>
    </tr>
    <tr>
      <td rowspan="2" class="nowrap">systems.&lt;system&gt;.<br>streams.&lt;stream&gt;.<br>samza.offset.default</td>
      <td>upcoming (default)</td>
      <td>When container starts and there is no checkpoint (or the checkpoint is ignored), only process messages that are published after the job is started, but no old messages</td>
    </tr>
    <tr>
      <td>oldest</td>
      <td>When container starts and there is no checkpoint (or the checkpoint is ignored), jump back to the oldest available message in the system, and consume all messages from that point onwards (most likely this means repeated processing of messages already seen previously)</td>
    </tr>
  </tbody>
</table>

Note that the example configuration above causes your tasks to start consuming from the oldest offset *every time a container starts up*. This is useful in case you have some in-memory state in your tasks that you need to rebuild from source data in an input stream. If you are using streams in this way, you may also find [bootstrap streams](streams.html) useful.

### Manipulating Checkpoints Manually

If you want to make a one-off change to a job's consumer offsets, for example to force old messages to be [processed again](../jobs/reprocessing.html) with a new version of your code, you can use CheckpointTool to inspect and manipulate the job's checkpoint. The tool is included in Samza's [source repository](/contribute/code.html).

To inspect a job's latest checkpoint, you need to specify your job's config file, so that the tool knows which job it is dealing with:

{% highlight bash %}
samza-example/target/bin/checkpoint-tool.sh \
  --config-path=file:///path/to/job/config.properties
{% endhighlight %}

This command prints out the latest checkpoint in a properties file format. You can save the output to a file, and edit it as you wish. For example, to jump back to the oldest possible point in time, you can set all the offsets to 0. Then you can feed that properties file back into checkpoint-tool.sh and save the modified checkpoint:

{% highlight bash %}
samza-example/target/bin/checkpoint-tool.sh \
  --config-path=file:///path/to/job/config.properties \
  --new-offsets=file:///path/to/new/offsets.properties
{% endhighlight %}

Note that Samza only reads checkpoints on container startup. In order for your checkpoint change to take effect, you need to first stop the job, then save the modified offsets, and then start the job again. If you write a checkpoint while the job is running, it will most likely have no effect.

## [State Management &raquo;](state-management.html)
