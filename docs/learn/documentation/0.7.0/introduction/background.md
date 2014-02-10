---
layout: page
title: Background
---

This page provides some background about stream processing, describes what Samza is, and why it was built.

### What is messaging?

Messaging systems are a popular way of implementing near-realtime asynchronous computation. Messages can be added to a message queue (ActiveMQ, RabbitMQ), pub-sub system (Kestrel, Kafka), or log aggregation system (Flume, Scribe) when something happens. Downstream *consumers* read messages from these systems, and process them or take actions based on the message contents.

Suppose you have a website, and every time someone loads a page, you send a "user viewed page" event to a messaging system. You might then have consumers which do any of the following:

* Store the message in Hadoop for future analysis
* Count page views and update a dashboard
* Trigger an alert if a page view fails
* Send an email notification to another user
* Join the page view event with the user's profile, and send the message back to the messaging system

A messaging system lets you decouple all of this work from the actual web page serving.

### What is stream processing?

A messaging system is a fairly low-level piece of infrastructure&mdash;it stores messages and waits for consumers to consume them. When you start writing code that produces or consumes messages, you quickly find that there are a lot of tricky problems that have to be solved in the processing layer. Samza aims to help with these problems.

Consider the counting example, above (count page views and update a dashboard). What happens when the machine that your consumer is running on fails, and your current counter values are lost? How do you recover? Where should the processor be run when it restarts? What if the underlying messaging system sends you the same message twice, or loses a message? (Unless you are careful, your counts will be incorrect.) What if you want to count page views grouped by the page URL? How do you distribute the computation across multiple machines if it's too much for a single machine to handle?

Stream processing is a higher level of abstraction on top of messaging systems, and it's meant to address precisely this category of problems.

### Samza

Samza is a stream processing framework with the following features:

* **Simple API:** Unlike most low-level messaging system APIs, Samza provides a very simple callback-based "process message" API comparable to MapReduce.
* **Managed state:** Samza manages snapshotting and restoration of a stream processor's state. When the processor is restarted, Samza restores its state to a consistent snapshot. Samza is built to handle large amounts of state (many gigabytes per partition).
* **Fault tolerance:** Whenever a machine in the cluster fails, Samza works with YARN to transparently migrate your tasks to another machine.
* **Durability:** Samza uses Kafka to guarantee that messages are processed in the order they were written to a partition, and that no messages are ever lost.
* **Scalability:** Samza is partitioned and distributed at every level. Kafka provides ordered, partitioned, replayable, fault-tolerant streams. YARN provides a distributed environment for Samza containers to run in.
* **Pluggable:** Though Samza works out of the box with Kafka and YARN, Samza provides a pluggable API that lets you run Samza with other messaging systems and execution environments.
* **Processor isolation:** Samza works with Apache YARN, which supports Hadoop's security model, and resource isolation through Linux CGroups.

### Alternatives

The available open source stream processing systems are actually quite young, and no single system offers a complete solution. New problems in this area include: how a stream processor's state should be managed, whether or not a stream should be buffered remotely on disk, what to do when duplicate messages are received or messages are lost, and how to model underlying messaging systems.

Samza's main differentiators are:

* Samza supports fault-tolerant local state. State can be thought of as tables that are split up and co-located with the processing tasks. State is itself modeled as a stream. If the local state is lost due to machine failure, the state stream is replayed to restore it.
* Streams are ordered, partitioned, replayable, and fault tolerant.
* YARN is used for processor isolation, security, and fault tolerance.
* Jobs are decoupled: if one job goes slow and builds up a backlog of unprocessed messages, the rest of the system is not affected.

For a more in-depth discussion on Samza, and how it relates to other stream processing systems, have a look at Samza's [Comparisons](../comparisons/introduction.html) documentation.

## [Concepts &raquo;](concepts.html)
