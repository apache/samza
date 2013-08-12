---
layout: page
title: MUPD8
---

*People generally want to know how similar systems compare. We've done our best to fairly contrast the feature sets of Samza with other systems. But we aren't experts in these frameworks, and we are, of course, totally biased. If we have goofed anything let us know and we will correct it.*

### Durability

MUPD8 makes no durability or delivery guarantees. Within MUPD8, stream processor tasks receive messages at most once. Samza uses Kafka for messaging, which guarantees message delivery.

### Ordering

As with durability, developers would ideally like their stream processors to receive messages in exactly the order that they were written.

We don't entirely follow MUPD8's description of their ordering guarantees, but it seems to guarantee that all messages will be processed in the order in which they are written to MUPD8 queues, which is comparable to Kafka and Samza's guarantee.

### Buffering

A critical issue for handling large data flows is handling back pressure when one downstream processing stage gets slow.

MUPD8 buffers messages in an in-memory queue when passing messages between two MUPD8 tasks. When a queue fills up, developers are left to either drop the messages on the floor, log the messages to local disk, or block until the queue frees up. All of these options are sub-optimal. Dropping messages destroys durability guarantees. Blocking your stream processor can result in back pressure, where the slowest processor blocks all upstream processors, which in turn block their upstream processors, until the whole system comes to a grinding hault. Logging to local disk is the most reasonable, but when a fault occurs, those messages will be lost on failover.

By adopting Kafka's broker as a remote buffer, Samza solves all of these problems. It doesn't need to block because consumers and producers are decoupled using Kafka's brokers' disks as async buffers. Messages shouldn't be dropped because Kafkas's 0.8 brokers should be highly available. In the event of a failure, when a Samza job resumes on another system, its input and output are not lost because it's stored remotely on replicated Kafka brokers.

### State Management

Stream processors frequently will accrue state as they process messages. For example, they might be incrementing a counter when a certain type of message is seen. They might also be storing messages in memory while trying to join them with messages from another stream (e.g. ad impressions vs. ad clicks). A design decision that needs to be made is how (if at all) to handle this in-memory state in situations where a failure occurs.

MUPD8 uses a write back caching strategy to manage in-memory state that is periodically written back to Cassandra.

Samza maintains state locally with the task. This allows state larger than will fit in memory. State is persisted to an output stream for recovery purposes should the task fail. In the long run we believe this design will be better suited to strong fault tolerance semantics as the change log captures the evolution of state allowing consistent restore of a task to a consistent point of time.

### Deployment and execution

MUPD8 includes a custom execution framework. The functionality that this framework supports in terms of users and resource limits isn't clear to us.

Samza simply leverages YARN to deploy user code, and execute it in a distributed environment.

### Fault Tolerance

What should a stream processing system do when a machine or processor fails?

MUPD8 uses its custom rolled equivalent to YARN to manage fault tolerance. When a stream processor is unable to send a message to a downstream processor, it notifies MUPD8's coordinator, and all other machines are notified. The machines then send all messages to a new machine based on the key hash that's used. Messages and state can both be lost when this happens.

Samza uses YARN to manage fault tolerance. YARN will detect when nodes or Samza tasks fail, and will notify Samza's [ApplicationMaster](../yarn/application-master.html). At that point, it's up to Samza to decide what to do. Generally, this means re-starting the task on another machine. Since messages are persisted to Kafka brokers remotely, and there are no in-memory queues, no messages should be lost unless the processors are using async Kafka producers.

### Workflow

Sometimes more than one job or processing stage is needed to accomplish something. This is the case where you wish to re-partition a stream, for example. MUPD8 has a custom workflow system setup to define how to execute multiple jobs at once, and how to feed stream data from one into the other.

Samza makes the individual jobs the level of granularity of execution. Jobs communicate via named input and output streams. This implicitly defines a data flow graph between all running jobs. We chose this model to enable data flow graphs with processing stages owned by different engineers on different teams working in different code bases without the need to wire everything together into a single topology.

This was motivated by our experience with Hadoop where the data flow between jobs is implicitly wired together by their input and output directories. We have had good experience making this decentralized model work well.

### Memory

MUPD8 executes all of its map/update processors inside a single JVM, using threads. This should shrink the memory footprint of a stream processor by amortizing JVM overhead across the number of processors currently being executed.

Samza tends to use more memory since it has distinct JVMs for each stream processor container ([TaskRunner](../container/task-runner.html)), rather than, running multiple stream processors in the same JVM, which is what MUPD8 does. The benefit of having separate processes, however, is isolation.

### Isolation

MUPD8 provides no stream processor isolation. A single badly behaved stream processor can bring down all processors on the node.

Samza uses process level isolation between stream processor tasks. This is also the approach that Hadoop takes. We can enforce strict per-process memory footprints. In addition, Samza supports CPU limits when used with YARN CGroups. As YARN CGroup maturity progresses, the possibility to support disk and network CGroup limits should become available as well.

### Further Reading

The MUPD8 team has published a very good [paper](http://vldb.org/pvldb/vol5/p1814_wanglam_vldb2012.pdf) on the design of their system.

## [Storm &raquo;](storm.html)
