---
layout: page
title: Background
---

This page provides some background about stream processing, describes what Samza is, and why it was built.

### What is messaging?

Messaging systems are a popular way of implementing near-realtime asynchronous computation. Messages can be added to a message queue (Active MQ, Rabbit MQ), pub-sub system (Kestrel, Kafka), or log aggregation system (Flume, Scribe) when something happens. Downstream "consumers" read messages from these systems, and process or take action based on the message contents.

Suppose that you have a server that's serving web pages. You can have the web server send a "user viewed page" event to a messaging system. You might then have consumers:

* Put the message into Hadoop
* Count page views and update a dashboard
* Trigger an alert if a page view fails
* Send an email notification to another use
* Join the page view event with the user's profile, and send the message back to the messaging system

A messaging system lets you decouple all of this work from the actual web page serving.

### What is stream processing?

A messaging system is a fairly low-level piece of infrastructure---it stores messages and waits for consumers to consume them. When you start writing code that produces or consumes messages, you quickly find that there are a lot of tricky problems that have to be solved in the processing layer. Samza aims to help with these problems.

Consider the counting example, above (count page views and update a dashboard). What happens when the machine that your consumer is running on fails, and your "current count" is lost. How do you recover? Where should the processor be run when it restarts? What if the underlying messaging system sends you the same message twice, or loses a message? Your counts will be off. What if you want to count page views grouped by the page URL? How can you do that in a distributed environment?

Stream processing is a higher level of abstraction on top of messaging systems, and it's meant to address precisely this category of problems.

### Samza

Samza is a stream processing framework with the following features:

* **Simpe API:** Samza provides a very simple call-back based "process message" API.
* **Managed state:** Samza manages snapshotting and restoration of a stream processor's state. Samza will restore a stream processor's state to a snapshot consistent with the processor's last read messages when the processor is restarted. Samza is built to handle large amounts of state (even many gigabytes per partition).
* **Fault tolerance:** Samza will work with YARN to transparently migrate your tasks whenever a machine in the cluster fails.
* **Durability:** Samza uses Kafka to guarantee that no messages will ever be lost.
* **Scalability:** Samza is partitioned and distributed at every level. Kafka provides ordered, partitioned, replayable, fault-tolerant streams. YARN provides a distributed environment for Samza containers to run in.
* **Pluggable:** Though Samza works out of the box with Kafka and YARN, Samza provides a pluggable API that lets you run Samza with other messaging systems and execution environments.
* **Processor isolation:** Samza works with Apache YARN, to give security and resource scheduling, and resource isolation through Linux CGroups.

### Alternatives

The open source stream processing systems that are available are actually quite young, and no single system offers a complete solution. Problems like how a stream processor's state should be managed, whether a stream should be buffered remotely on disk or not, what to do when duplicate messages are received or messages are lost, and how to model underlying messaging systems are all pretty new.

Samza's main differentiators are:

* Samza supports fault-tolerant local state. State can be thought of as tables that are split up and maintained with the processing tasks. State is itself modeled as a stream. When a processor is restarted, the state stream is entirely replayed to restore it.
* Streams are ordered, partitioned, replayable, and fault tolerant.
* YARN is used for processor isolation, security, and fault tolerance.
* All streams are materialized to disk.

For a more in-depth discussion on Samza, and how it relates to other stream processing systems, have a look at Samza's [Comparisons](../comparisons/introduction.html) documentation.

## [Concepts &raquo;](concepts.html)
