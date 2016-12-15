---
layout: page
title: MUPD8
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

*People generally want to know how similar systems compare. We've done our best to fairly contrast the feature sets of Samza with other systems. But we aren't experts in these frameworks, and we are, of course, totally biased. If we have goofed anything, please let us know and we will correct it.*

### Durability

MUPD8 makes no durability or delivery guarantees. Within MUPD8, stream processor tasks receive messages at most once. Samza uses Kafka for messaging, which guarantees message delivery.

### Ordering

As with durability, developers would ideally like their stream processors to receive messages in exactly the order that they were written.

Based on the understanding of MUPD8's description of their ordering guarantees, it guarantees that all messages will be processed in the order in which they are written to MUPD8 queues, which is comparable to Kafka and Samza's guarantee.

### Buffering

A critical issue for handling large data flows is handling back pressure when one downstream processing stage gets slow.

MUPD8 buffers messages in an in-memory queue when passing messages between two MUPD8 tasks. When a queue fills up, developers have the option to either drop the messages on the floor, log the messages to local disk, or block until the queue frees up. All of these options are sub-optimal. Dropping messages leads to incorrect results. Blocking your stream processor can have a cascading effect, where the slowest processor blocks all upstream processors, which in turn block their upstream processors, until the whole system grinds to a halt. Logging to local disk is the most reasonable, but when a fault occurs, those messages are lost on failover.

By adopting Kafka's broker as a remote buffer, Samza solves all of these problems. It doesn't need to block because consumers and producers are decoupled using the Kafka brokers' disks as buffers. Messages are not dropped because Kafka brokers are highly available as of version 0.8. In the event of a failure, when a Samza job is restarted on another machine, its input and output are not lost, because they are stored remotely on replicated Kafka brokers.

### State Management

As described in the [introduction](introduction.html#state), stream processors often need to maintain some state as they process messages. Different frameworks have different approaches to handling such state, and what to do in case of a failure.

MUPD8 uses a write back caching strategy to manage in-memory state that is periodically written back to Cassandra.

Samza maintains state locally with the task. This allows state larger than will fit in memory. State is persisted to an output stream to enable recovery should the task fail. We believe this design enables stronger fault tolerance semantics, because the change log captures the evolution of state, allowing the state of a task to restored to a consistent point in time.

### Deployment and execution

MUPD8 includes a custom execution framework. The functionality that this framework supports in terms of users and resource limits isn't clear to us.

Samza leverages YARN to deploy user code, and execute it in a distributed environment.

### Fault Tolerance

What should a stream processing system do when a machine or processor fails?

MUPD8 uses its custom equivalent to YARN to manage fault tolerance. When a stream processor is unable to send a message to a downstream processor, it notifies MUPD8's coordinator, and all other machines are notified. The machines then send all messages to a new machine based on the key hash that's used. Messages and state can be lost when this happens.

Samza uses YARN to manage fault tolerance. YARN detects when nodes or Samza tasks fail, and notifies Samza's [ApplicationMaster](../yarn/application-master.html). At that point, it's up to Samza to decide what to do. Generally, this means re-starting the task on another machine. Since messages are persisted to Kafka brokers remotely, and there are no in-memory queues, no messages should be lost (unless the processors are using async Kafka producers, which offer higher performance but don't wait for messages to be committed).

### Workflow

Sometimes more than one job or processing stage is needed to accomplish something. This is the case where you wish to re-partition a stream, for example. MUPD8 has a custom workflow system setup to define how to execute multiple jobs at once, and how to feed stream data from one into the other.

Samza makes the individual jobs the level of granularity of execution. Jobs communicate via named input and output streams. This implicitly defines a data flow graph between all running jobs. We chose this model to enable data flow graphs with processing stages owned by different engineers on different teams, working in different code bases, without the need to wire everything together into a single topology.

This was motivated by our experience with Hadoop, where the data flow between jobs is implicitly defined by their input and output directories. This decentralized model has proven itself to scale well to a large organization.

### Memory

MUPD8 executes all of its map/update processors inside a single JVM, using threads. This is memory-efficient, as the JVM memory overhead is shared across the threads.

Samza uses a separate JVM for each [stream processor container](../container/samza-container.html). This has the disadvantage of using more memory compared to running multiple stream processing threads within a single JVM. However, the advantage is improved isolation between tasks, which can make them more reliable.

### Isolation

MUPD8 provides no resource isolation between stream processors. A single badly behaved stream processor can bring down all processors on the node.

Samza uses process level isolation between stream processor tasks, similarly to Hadoop's approach. We can enforce strict per-process memory limits. In addition, Samza supports CPU limits when used with YARN cgroups. As the YARN support for cgroups develops further, it should also become possible to support disk and network cgroup limits.

### Further Reading

The MUPD8 team has published a very good [paper](http://vldb.org/pvldb/vol5/p1814_wanglam_vldb2012.pdf) on the design of their system.

## [Storm &raquo;](storm.html)
