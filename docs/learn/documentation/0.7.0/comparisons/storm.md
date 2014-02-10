---
layout: page
title: Storm
---

*People generally want to know how similar systems compare. We've done our best to fairly contrast the feature sets of Samza with other systems. But we aren't experts in these frameworks, and we are, of course, totally biased. If we have goofed anything, please let us know and we will correct it.*

[Storm](http://storm-project.net/) and Samza are fairly similar. Both systems provide many of the same high-level features: a partitioned stream model, a distributed execution environment, an API for stream processing, fault tolerance, Kafka integration, etc.

Storm and Samza use different words for similar concepts: *spouts* in Storm are similar to stream consumers in Samza, *bolts* are similar to tasks, and *tuples* are similar to messages in Samza. Storm also has some additional building blocks which don't have direct equivalents in Samza.

### Ordering and Guarantees

Storm allows you to choose the level of guarantee with which you want your messages to be processed:

* The simplest mode is *at-most-once delivery*, which drops messages if they are not processed correctly, or if the machine doing the processing fails. This mode requires no special logic, and processes messages in the order they were produced by the spout.
* There is also *at-least-once delivery*, which tracks whether each input tuple (and any downstream tuples it generated) was successfully processed within a configured timeout, by keeping an in-memory record of all emitted tuples. Any tuples that are not fully processed within the timeout are re-emitted by the spout. This implies that a bolt may see the same tuple more than once, and that messages can be processed out-of-order. This mechanism also requires some co-operation from the user code, which must maintain the ancestry of records in order to properly acknowledge its input. This is explained in depth on [Storm's wiki](https://github.com/nathanmarz/storm/wiki/Guaranteeing-message-processing).
* Finally, Storm offers *exactly-once semantics* using its [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial) abstraction. This mode uses the same failure detection mechanism as the at-least-once mode. Tuples are actually processed at least once, but Storm's state implementation allows duplicates to be detected and ignored. (The duplicate detection only applies to state managed by Storm. If your code has other side-effects, e.g. sending messages to a service outside of the topology, it will not have exactly-once semantics.) In this mode, the spout breaks the input stream into batches, and processes batches in strictly sequential order.

Samza also offers guaranteed delivery &mdash; currently only at-least-once delivery, but support for exactly-once semantics is planned. Within each stream partition, Samza always processes messages in the order they appear in the partition, but there is no guarantee of ordering across different input streams or partitions. This model allows Samza to offer at-least-once delivery without the overhead of ancestry tracking. In Samza, there would be no performance advantage to using at-most-once delivery (i.e. dropping messages on failure), which is why we don't offer that mode &mdash; message delivery is always guaranteed.

Moreover, because Samza never processes messages in a partition out-of-order, it is better suited for handling keyed data. For example, if you have a stream of database updates &mdash; where later updates may replace earlier updates &mdash; then reordering the messages may change the final result. Provided that all updates for the same key appear in the same stream partition, Samza is able to guarantee a consistent state.

### State Management

Storm's lower-level API of bolts does not offer any help for managing state in a stream process. A bolt can maintain in-memory state (which is lost if that bolt dies), or it can make calls to a remote database to read and write state. However, a topology can usually process messages at a much higher rate than calls to a remote database can be made, so making a remote call for each message quickly becomes a bottleneck.

As part of its higher-level Trident API, Storm offers automatic [state management](https://github.com/nathanmarz/storm/wiki/Trident-state). It keeps state in memory, and periodically checkpoints it to a remote database (e.g. Cassandra) for durability, so the cost of the remote database call is amortized over several processed tuples. By maintaining metadata alongside the state, Trident is able to achieve exactly-once processing semantics &mdash; for example, if you are counting events, this mechanism allows the counters to be correct, even when machines fail and tuples are replayed.

Storm's approach of caching and batching state changes works well if the amount of state in each bolt is fairly small &mdash; perhaps less than 100kB. That makes it suitable for keeping track of counters, minimum, maximum and average values of a metric, and the like. However, if you need to maintain a large amount of state, this approach essentially degrades to making a database call per processed tuple, with the associated performance cost.

Samza takes a [completely different approach](../container/state-management.html) to state management. Rather than using a remote database for durable storage, each Samza task includes an embedded key-value store, located on the same machine. Reads and writes to this store are very fast, even when the contents of the store are larger than the available memory. Changes to this key-value store are replicated to other machines in the cluster, so that if one machine dies, the state of the tasks it was running can be restored on another machine.

By co-locating storage and processing on the same machine, Samza is able to achieve very high throughput, even when there is a large amount of state. This is necessary if you want to perform stateful operations that are not just counters. For example, if you want to perform a window join of multiple streams, or join a stream with a database table (replicated to Samza through a changelog), or group several related messages into a bigger message, then you need to maintain so much state that it is much more efficient to keep the state local to the task.

A limitation of Samza's state handling is that it currently does not support exactly-once semantics &mdash; only at-least-once is supported right now. But we're working on fixing that, so stay tuned for updates.

### Partitioning and Parallelism

Storm's [parallelism model](https://github.com/nathanmarz/storm/wiki/Understanding-the-parallelism-of-a-Storm-topology) is fairly similar to Samza's. Both frameworks split processing into independent *tasks* that can run in parallel. Resource allocation is independent of the number of tasks: a small job can keep all tasks in a single process on a single machine; a large job can spread the tasks over many processes on many machines.

The biggest difference is that Storm uses one thread per task by default, whereas Samza uses single-threaded processes (containers). A Samza container may contain multiple tasks, but there is only one thread that invokes each of the tasks in turn. This means each container is mapped to exactly one CPU core, which makes the resource model much simpler and reduces interference from other tasks running on the same machine. Storm's multithreaded model has the advantage of taking better advantage of excess capacity on an idle machine, at the cost of a less predictable resource model.

Storm supports *dynamic rebalancing*, which means adding more threads or processes to a topology without restarting the topology or cluster. This is a convenient feature, especially during development. We haven't added this to Samza: philosophically we feel that this kind of change should go through a normal configuration management process (i.e. version control, notification, etc.) as it impacts production performance. In other words, the code and configuration of the jobs should fully recreate the state of the cluster.

When using a transactional spout with Trident (a requirement for achieving exactly-once semantics), parallelism is potentially reduced. Trident relies on a global ordering in its input streams &mdash; that is, ordering across all partitions of a stream, not just within one partion. This means that the topology's input stream has to go through a single spout instance, effectively ignoring the partitioning of the input stream. This spout may become a bottleneck on high-volume streams. In Samza, all stream processing is parallel &mdash; there are no such choke points.

### Deployment &amp; Execution

A Storm cluster is composed of a set of nodes running a *Supervisor* daemon. The supervisor daemons talk to a single master node running a daemon called *Nimbus*. The Nimbus daemon is responsible for assigning work and managing resources in the cluster. See Storm's [Tutorial](https://github.com/nathanmarz/storm/wiki/Tutorial) page for details. This is quite similar to YARN; though YARN is a bit more fully featured and intended to be multi-framework, Nimbus is better integrated with Storm.

Yahoo! has also released [Storm-YARN](https://github.com/yahoo/storm-yarn). As described in [this Yahoo! blog post](http://developer.yahoo.com/blogs/ydn/storm-yarn-released-open-source-143745133.html), Storm-YARN is a wrapper that starts a single Storm cluster (complete with Nimbus, and Supervisors) inside a YARN grid.

There are a lot of similarities between Storm's Nimbus and YARN's ResourceManager, as well as between Storm's Supervisor and YARN's Node Managers. Rather than writing our own resource management framework, or running a second one inside of YARN, we decided that Samza should use YARN directly, as a first-class citizen in the YARN ecosystem. YARN is stable, well adopted, fully-featured, and inter-operable with Hadoop. It also provides a bunch of nice features like security (user authentication), cgroup process isolation, etc.

The YARN support in Samza is pluggable, so you can swap it for a different execution framework if you wish.

### Language Support

Storm is written in Java and Clojure but has good support for non-JVM languages. It follows a model similar to MapReduce Streaming: the non-JVM task is launched in a separate process, data is sent to its stdin, and output is read from its stdout.

Samza is written in Java and Scala. It is built with multi-language support in mind, but currently only supports JVM languages.

### Workflow

Storm provides modeling of *topologies* (a processing graph of multiple stages) [in code](https://github.com/nathanmarz/storm/wiki/Tutorial). Trident provides a further [higher-level API](https://github.com/nathanmarz/storm/wiki/Trident-tutorial) on top of this, including familiar relational-like operators such as filters, grouping, aggregation and joins. This means the entire topology is wired up in one place, which has the advantage that it is documented in code, but has the disadvantage that the entire topology needs to be developed and deployed as a whole.

In Samza, each job is an independent entity. You can define multiple jobs in a single codebase, or you can have separate teams working on different jobs using different codebases. Each job is deployed, started and stopped independently. Jobs communicate only through named streams, and you can add jobs to the system without affecting any other jobs. This makes Samza well suited for handling the data flow in a large company.

Samza's approach can be emulated in Storm by connecting two separate topologies via a broker, such as Kafka. However, Storm's implementation of exactly-once semantics only works within a single topology.

### Maturity

We can't speak to Storm's maturity, but it has an [impressive number of adopters](https://github.com/nathanmarz/storm/wiki/Powered-By), a strong feature set, and seems to be under active development. It integrates well with many common messaging systems (RabbitMQ, Kestrel, Kafka, etc).

Samza is pretty immature, though it builds on solid components. YARN is fairly new, but is already being run on 3000+ node clusters at Yahoo!, and the project is under active development by both [Hortonworks](http://hortonworks.com/) and [Cloudera](http://www.cloudera.com/content/cloudera/en/home.html). Kafka has a strong [powered by](https://cwiki.apache.org/KAFKA/powered-by.html) page, and has seen increased adoption recently. It's also frequently used with Storm. Samza is a brand new project that is in use at LinkedIn. Our hope is that others will find it useful, and adopt it as well.

### Buffering &amp; Latency

Storm uses [ZeroMQ](http://zeromq.org/) for non-durable communication between bolts, which enables extremely low latency transmission of tuples. Samza does not have an equivalent mechanism, and always writes task output to a stream.

On the flip side, when a bolt is trying to send messages using ZeroMQ, and the consumer can't read them fast enough, the ZeroMQ buffer in the producer's process begins to fill up with messages. If this buffer grows too much, the topology's processing timeout may be reached, which causes messages to be re-emitted at the spout and makes the problem worse by adding even more messages to the buffer. In order to prevent such overflow, you can configure a maximum number of messages that can be in flight in the topology at any one time; when that threshold is reached, the spout blocks until some of the messages in flight are fully processed. This mechanism allows back pressure, but requires [topology.max.spout.pending](http://nathanmarz.github.io/storm/doc/backtype/storm/Config.html#TOPOLOGY_MAX_SPOUT_PENDING) to be carefully configured. If a single bolt in a topology starts running slow, the processing in the entire topology grinds to a halt.

A lack of a broker between bolts also adds complexity when trying to deal with fault tolerance and messaging semantics.  Storm has a [clever mechanism](https://github.com/nathanmarz/storm/wiki/Guaranteeing-message-processing) for detecting tuples that failed to be processed, but Samza doesn't need such a mechanism because every input and output stream is fault-tolerant and replicated.

Samza takes a different approach to buffering. We buffer to disk at every hop between a StreamTask. This decision, and its trade-offs, are described in detail on the [Comparison Introduction](introduction.html) page. This design decision makes durability guarantees easy, and has the advantage of allowing the buffer to absorb a large backlog of messages if a job has fallen behind in its processing. However, it comes at the price of slightly higher latency.

As described in the *workflow* section above, Samza's approach can be emulated in Storm, but comes with a loss in functionality.

### Isolation

Storm provides standard UNIX process-level isolation. Your topology can impact another topology's performance (or vice-versa) if too much CPU, disk, network, or memory is used.

Samza relies on YARN to provide resource-level isolation. Currently, YARN provides explicit controls for memory and CPU limits (through [cgroups](../yarn/isolation.html)), and both have been used successfully with Samza. No isolation for disk or network is provided by YARN at this time.

### Distributed RPC

In Storm, you can write topologies which not only accept a stream of fixed events, but also allow clients to run distributed computations on demand. The query is sent into the topology as a tuple on a special spout, and when the topology has computed the answer, it is returned to the client (who was synchronously waiting for the answer). This facility is called [Distributed RPC](https://github.com/nathanmarz/storm/wiki/Distributed-RPC) (DRPC).

Samza does not currently have an equivalent API to DRPC, but you can build it yourself using Samza's stream processing primitives.

### Data Model

Storm models all messages as *tuples* with a defined data model but pluggable serialization.

Samza's serialization and data model are both pluggable. We are not terribly opinionated about which approach is best.

## [API Overview &raquo;](../api/overview.html)
