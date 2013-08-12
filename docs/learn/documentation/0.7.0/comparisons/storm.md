---
layout: page
title: Storm
---

*People generally want to know how similar systems compare. We've done our best to fairly contrast the feature sets of Samza with other systems. But we aren't experts in these frameworks, and we are, of course, totally biased. If we have goofed anything let us know and we will correct it.*

[Storm](http://storm-project.net/) and Samza are fairly similar. Both systems provide many of the same features: a partitioned stream model, a distributed execution environment, an API for stream processing, fault tolerance, Kafka integration, etc.

### Ordering and Guarantees

Storm has more conceptual building blocks than Samza. "Spouts" in Storm are similar to Streams in Samza, and Samza does not have an equivalent of their transient zeromq communication.

There are also several approaches to handling delivery guarantees.

The primary approach is implemented by keeping a record of all emitted records in memory until they are acknowledged by all elements of a particular processing graph. In this mode messages that timeout are re-emitted. This seems to imply that messages can be processed out of order. This mechanism requires some co-operation from the user code which must maintain the ancestry of records in order to properly acknowledge its input. This is detailed in-depth on [Storm's wiki](https://github.com/nathanmarz/storm/wiki/Guaranteeing-message-processing).

Out of order processing is a problem for handling keyed data. For example if you have a stream of database updates where later updates may replace earlier updates then reordering them may change the output.

This mechanism also implies that individual stages may produce back pressure up the processing graph, so the graphs are probably mostly limited to a single logical function. However multiple graphs could likely be stitched together using Spouts in between to buffer.

Storm offers a secondary approach to delivery guarantees called [transactional topologies](https://github.com/nathanmarz/storm/wiki/Transactional-topologies). These require an underlying system similar to Kafka that maintains strongly sequenced messages. Transactional topologies seem to be limited to a single input stream.

Samza always offers guaranteed delivery and ordering of input within a stream partition. We make no guarantee of ordering between different input streams or input stream partitions. Since all stages are repayable there is no need for the user code to track its ancestry.

Like Storm's transactional topologies Samza provides a unique "offset" which is a sequential integer uniquely denoting the message in that stream partition. That is the first message in a stream partition has offset 0, the second offset 1, etc. Samza always records the position of a job in its input streams as a vector of offsets for the input stream partitions it consumers.

Storm has integrated these transaction ids into some of its storage abstractions to help with deduplicating updates. We have a different take on ensuring the semantics of output in the presence of failures however we have not yet implemented this.

### State Management

We are not aware of any state management facilities in Storm though transactional topologies have plugins for external storage to use the transaction id for deduping. In this case, Storm will manage only the metadata necessary to make a topology transactional. It's still up to the Bolt implementer to handle transaction IDs, and store state in a remote database, somewhere.

Samza provides [built-in primitives](../container/state-management.html) for managing large amounts of state.

### Partitioning and Parallelism

Storm's [parallelism model](https://github.com/nathanmarz/storm/wiki/Understanding-the-parallelism-of-a-Storm-topology) maps fairly similar to Samza's. The biggest difference is that Samza holds only a single job per process and the process is single threaded regardless of the number of tasks it contains. Storm's more optimistic parallelism model has the advantage of taking better advantage of excess capacity on an idle machine. However this significantly complicates the resource model. In Samza since each container map exactly to a CPU core a job run in 100 containers will use 100 CPU cores. This allows us to better model the CPU usage on a machine and ensure that we don't see uneven performance based on the other tasks that happen to be collocated on that machine. 

Storm supports "dynamic rebalancing", which means adding more threads or processes to a topology without restarting the topology or cluster. This is a convenient feature, especially for during development. We haven't added this yet as philosophically we feel that these kind of changes should go through a normal configuration management process (i.e. version control, notification, etc) as they impact production performance. In other words the jobs + configs should fully recreate the state of the cluster.

### Deployment &amp; Execution

A Storm cluster is composed of a series of nodes running a "Supervisor" daemon. The supervisor daemons talk to a single master node running a daemon called "Nimbus". The Nimbus daemon is responsible for assigning work and managing resources in the cluster. See Storm's [Tutorial](https://github.com/nathanmarz/storm/wiki/Tutorial) page for details. This is quite similar to YARN; though YARN is a bit more fully featured and intended to be multi-framework, Nimbus is better integrated with Storm.

Yahoo! has also released [Storm-YARN](https://github.com/yahoo/storm-yarn). As described in [this Yahoo! blog post](http://developer.yahoo.com/blogs/ydn/storm-yarn-released-open-source-143745133.html), Storm-YARN is a wrapper that starts a single Storm cluster (complete with Nimbus, and Supervisors) inside a YARN grid.

Anyone familiar with YARN will recognize the similarity between Storm's "Nimbus" daemon, and YARN's ResourceManager, and Storm's "Supervisor" daemon, and YARN's Node Managers. Rather than writing its own resource management framework, or running a second one inside of YARN, Samza simply uses YARN directly, as a first-class citizen in the Samza ecosystem. YARN is stable, well adopted, fully-featured, and inter-operable with Hadoop. It also provides a bunch of nice features like Security, CGroup process isolation, etc.

### Language Support

Storm is written in Java and Clojure but has good support for non-JVM languages. It follows a model similar to MapReduce Streaming by piping input and output streams fed to externally managed processes.

On top of this, Storm provides [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial), a DSL that's meant to make writing Storm topologies easier.

Samza is built with language support in mind, but currently only supports JVM languages.

### Workflow

Storm provides modeling of "Topologies" (a processing graph of multiple stages) [in code](https://github.com/nathanmarz/storm/wiki/Tutorial). This manual wiring together of the flow can serve as nice documentation of the processing flow.

Each job in a Samza graph is an independent entity that communicates with other jobs through a named stream rather than manually wiring them together. All the jobs on a cluster comprise a single (potentially disconnected) data flow graph. Each job can be stopped or started independently and there is no code coupling between jobs.

### Maturity

We can't speak to Storm's maturity, but it has an [impressive amount of adopters](https://github.com/nathanmarz/storm/wiki/Powered-By), a strong feature set, and seems to be under active development. It integrates well with many common messaging systems (RabbitMQ, Kesrel, Kafka, etc).

Samza is pretty immature, though it builds on solid components. YARN is fairly new, but is already being run on 3000+ node clusters at Yahoo!, and the project is under active development by both [Hortonworks](http://hortonworks.com/) and [Cloudera](http://www.cloudera.com/content/cloudera/en/home.html). Kafka has a strong [powered by](https://cwiki.apache.org/KAFKA/powered-by.html) page, and has seen its share of adoption, recently. It's also frequently used with Storm. Samza is a brand new project that is in use at LinkedIn. Our hope is that others will find it useful, and adopt it as well.

### Buffering &amp; Latency

Within a single topology, Storm has producers and consumers, but no broker (to use Kafka's terminology). This design decision leads to a number of interesting properties.

Since Storm uses ZeroMQ without intermediate brokers, the transmission of messages from one Bolt to another is extremely low latency. It's just a network hop.

On the flip side, when a Bolt is trying to send messages using ZeroMQ, and the consumer can't read them fast enough, the ZeroMQ buffer in the producer's process begins to fill up with messages. When it becomes full, you have the option to drop them, log to local disk, or block until space becomes available again. These options are outlined in the [MUPD8 comparison](mupd8) page, as well, and none of them are ideal. This style of stream processing runs the risk of completely grinding to a halt (or dropping messages) if a single Bolt has a throughput issue. This problem is commonly known as back pressure. When back pressure occurs, Storm essentially offloads the problem to the Spout implementation. In cases where the Spout can't handle large volumes of back-logged messages, the same problem occurs. In systems like Kafka, where large volumes of backlogged messages are supported, the entire topology just reads messages from the spout at a lower rate.

A lack of a broker between bolts also adds complexity when trying to deal with fault tolerance and messaging semantics. Storm has a very well written page on [Transactional Topologies](https://github.com/nathanmarz/storm/wiki/Transactional-topologies) that describes this problem, and Storm's solution, in depth.

Samza takes a different approach to buffering. We buffer to disk at every hop between a StreamTask. This decision, and its trade-offs, are described in detail on the [Comparison Introduction](introduction.html) page's "stream model" section. This design decision lets us cheat a little bit, when it comes to things like durability guarantees, and exactly once messaging semantics, but it comes at the price of increased latency, since everything must be written to disk in Kafka.

### Isolation

Storm provides standard UNIX process-level isolation. Your topology can impact another topology's performance (or vice-versa) if too much CPU, disk, network, or memory is used.

Samza relies on YARN to provide resource-level isolation. Currently, YARN provides explicit controls for memory and CPU limits (through [CGroups](../yarn/isolation.html)), and both have been used successfully with Samza. No isolation for disk or network is provided by YARN at this time.

### Data Model

Storm models all messages as "Tuples" with a defined data model but pluggable serialization.

Samza's serialization and data model are both pluggable. We are not terribly opinionated about which approach is best.

## [API Overview &raquo;](../api/overview.html)
