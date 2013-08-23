---
layout: page
title: Architecture
---

Samza is made up of three layers:

1. A streaming layer.
2. An execution layer.
3. A processing layer.

Samza provides out of the box support for all three layers.

1. **Streaming:** [Kafka](http://kafka.apache.org/)
2. **Execution:** [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)
3. **Processing:** [Samza API](../api/overview.html)

These three pieces fit together to form Samza.

![diagram-medium](/img/0.7.0/learn/documentation/introduction/samza-ecosystem.png)

This architecture should be familiar to anyone that's used Hadoop.

![diagram-medium](/img/0.7.0/learn/documentation/introduction/samza-hadoop.png)

Before going in-depth on each of these three layers, it should be noted that Samza supports is not limited to these systems. Both Samza's execution and streaming layer are pluggable, and allow developers to implement alternatives if they prefer.

### Kafka

[Kafka](http://kafka.apache.org/) is a distributed pub/sub and message queueing system that provides at-least once messaging guarantees, and highly available partitions (i.e. a stream's partitions will be available, even if a machine goes down).

In Kafka, each stream is called a "topic". Each topic is partitioned up, to make things scalable. When a "producer" sends a message to a topic, the producer provides a key, which is used to determine which partition the message should be sent to. Kafka "brokers", each of which are in charge of some partitions, receive the messages that the producer sends, and stores them on their disk in a log file. Kafka "consumers" can then read from a topic by getting messages from all of a topic's partitions.

This has some interesting properties. First, all messages partitioned by the same key are guaranteed to be in the same Kafka topic partition. This means, if you wish to read all messages for a specific member ID, you only have to read the messages from the partition that the member ID is on, not the whole topic (assuming the topic is partitioned by member ID). Second, since a Kafka broker's file is a log, you can reference any point in the log file using an "offset". This offset determines where a consumer is in a topic/partition pair. After every message a consumer reads from a topic/partition pair, the offset is incremented.

For more details on Kafka, see Kafka's [introduction](http://kafka.apache.org/introduction.html) and [design](http://kafka.apache.org/design.html) pages.

### YARN

[YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) (Yet Another Resource Negotiator) is Hadoop's next-generation cluster scheduler. It allows you to allocate a number of "containers" (processes) in a cluster of machines, and execute arbitrary commands on them.

When an application interacts with YARN, it looks something like this:

1. **Application**: I want to run command X on two machines with 512M memory
2. **YARN**: Cool, where's your code?
3. **Application**: http://path.to.host/jobs/download/my.tgz
4. **YARN**: I'm running your job on node-1.grid and node-1.grid

Samza uses YARN to manage:

* Deployment
* Fault tolerance
* Logging
* Isolation
* Security
* Locality

This page covers a brief overview of YARN, but [this page](http://hortonworks.com/blog/apache-hadoop-yarn-background-and-an-overview/) from Hortonworks contains a much better overview.

#### YARN Architecture

YARN has three important pieces: a ResourceManager, a NodeManager, and an ApplicationMaster. In a YARN grid, every computer runs a NodeManager, which is responsible for running processes on the local machine. A ResourceManager talks to all of the NodeManagers to tell it what to run. Applications, in turn, talk to the ResourceManager when they wish to run something on the cluster. The flow, when starting a new application, goes from user application to YARN RM, to YARN NM. The third piece, the ApplicationMaster, is actually application-specific code that runs in the YARN cluster. It's responsible for managing the application's workload, asking for containers (usually, UNIX processes), and handling notifications when one of its containers fails.

#### Samza and YARN

Samza provides a YARN ApplicationMaster, and YARN job runner out of the box. The integration between Samza and YARN is outlined in the following diagram (different colors indicate different host machines).

![diagram-small](/img/0.7.0/learn/documentation/introduction/samza-yarn-integration.png)

The Samza client talks to the YARN RM when it wants to start a new Samza job. The YARN RM talks to a YARN NM to allocate space on the cluster for Samza's ApplicationMaster. Once the NM allocates space, it starts the Samza AM. After the Samza AM starts, it asks the YARN RM for one, or more, YARN containers to run Samza [TaskRunners](../container/task-runner.html). Again, the RM works with NMs to allocate space for the containers. Once the space has been allocated, the NMs start the Samza containers.

### Samza

Samza uses YARN and Kafka to provide a framework for stage-wise stream processing and partitioning. Everything, put together, looks like this (different colors indicate different host machines):

![diagram-small](/img/0.7.0/learn/documentation/introduction/samza-yarn-kafka-integration.png)

The Samza client uses YARN to run a Samza job. The Samza [TaskRunners](../container/task-runner.html) run in one, or more, YARN containers, and execute user-written Samza [StreamTasks](../api/overview.html). The input and output for the Samza StreamTasks come from Kafka brokers that are (usually) co-located on the same machines as the YARN NMs.

### Example

Let's take a look at a real example. Suppose that we wanted to count page views grouped by member ID. In SQL, it would look something like: SELECT COUNT(\*) FROM PageViewEvent GROUP BY member_id. Although Samza doesn't support SQL right now, the idea is the same. Two jobs are required to calculate this query: one to group messages by member ID, and the other to do the counting. The counting and grouping can't be done in the same Samza job because the input topic might not be partitioned by the member ID. Anyone familiar with Hadoop will recognize this as a Map/Reduce operation, where you first map data by a particular key, and then count in the reduce step.

![diagram-large](/img/0.7.0/learn/documentation/introduction/group-by-example.png)

The input topic is partitioned using Kafka. Each Samza process reads messages from one or more of the input topic's partitions, and emits them back out to a different Kafka topic. Each output message is keyed by the message's member ID attribute, and this key is mapped to one of the topic's partitions (usually by hashing the key, and modding by the number of partitions in the topic). The Kafka brokers receive these messages, and buffer them on disk until the second job (the counting job on the bottom of the diagram) reads the messages, and increments its counters.

There are some neat things to consider about this example. First, we're leveraging the fact that Kafka topics are inherently partitioned. This lets us run one or more Samza processes, and assign them each some partitions to read from. Second, since we're guaranteed that, for a given key, all messages will be on the same partition, we can actually split up the aggregation (counting). For example, if the first job's output had four partitions, we could assign two partitions to the first count process, and the other two partitions to the second count process. We'd be guaranteed that for any give member ID, all of their messages will be consumed by either the first process or the second, but not both. This means we'll get accurate counts, even when partitioning. Third, the fact that we're using Kafka, which buffers messages on its brokers, also means that we don't have to worry as much about failures. If a process or machine fails, we can use YARN to start the process on another machine. When the process starts up again, it can get its last offset, and resume reading messages where it left off.

## [Comparison Introduction &raquo;](../comparisons/introduction.html)
