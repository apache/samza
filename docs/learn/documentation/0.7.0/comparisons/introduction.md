---
layout: page
title: Comparison Introduction
---

Here are a few of the high-level design decisions that we think make Samza a bit different from what we have seen elsewhere.

### The Stream Model

Streams are the input and output to Samza jobs. Samza has a very strong model of a stream&mdash;they are more then just a simple message exchange mechanism. A stream in Samza is a partitioned, ordered, replayable, multi-subscriber, lossless sequence of messages. The stream acts as a buffer that isolates processing stages from one another.

This stronger model requires persistence, fault-tolerance, and buffering in the stream implementation, but it has several benefits.

First, delays in a downstream processing stage cannot block an upstream stage. A Samza job can stop and do a few minutes (or even hours) of blocking processing on accumulated data without causing any backlog for upstream jobs. Our goal is to be able to model all the asynchronous data processing a large company might do as a single unified dataflow graph, while still isolating these jobs which may be written, owned,  and run by different people in different code bases with varying SLAs.

This is motivated in part by our experience building analogous (offline) processing pipelines in Hadoop. In Hadoop the output between processing stages are files and the processing stages MapReduce jobs. We have found that this strong isolation between stages makes it possible to have literally hundreds of loosely coupled jobs that comprise an offline processing ecosystem. Our goal is to be able to replicate this kind of rich ecosystem in the near-real-time setting.

The second benefit of this stronger model is that all stages are multi-subscriber. In practical terms this means that if one person adds a set of processing flows that create output data streams, others can see it, consume it, and build on it, without any central repository or release schedule that ties these things together. As a happy side-effect this makes debugging flows very easy as you can effectively attach to the output of any stage and "tail" its output.

Finally this strong stream model makes each processing stage completely independent which in turns greatly simplifies the implementation of many of the frameworks features. Each stage need only be concerned with its own inputs and outputs, there is no need to handle replaying large subgraphs which would require central control over otherwise independent jobs.

The tradeoff is that this stronger stream model requires durability and persistence. We were willing to make this tradeoff for two reasons. First we think that MapReduce and HDFS have shown that durability can be done at high-throughput while offering near-limitless disk space. This lead us to develop Kafka which allows hundreds of MB/sec of replicated throughput and many TBs of disk space per node. We think this largely neutralizes the downside of persistence. MapReduce has occationally been criticized for its persistence-happy approach to processing. However this criticism is particularly inapplicable to stream processing. Batch processing like MapReduce often is used for processing large historical windows in a very short period of time (i.e. query a month of data in ten minutes); stream processing, on the other hand, mostly needs only keep up with the steady-state flow of data (i.e. process 10 minutes worth of data in 10 minutes). This means that the raw throughput requirements for stream processing are, generally, several orders of magnitude lower than batch processes.

### State

We have put particular effort into allowing Samza jobs to manage large amounts of partitioned local state by providing out-of-the-box support for key-value access to a large local dataset.

This means that you can view a Samza job as being both a piece of processing code, but also a co-partitioned "table" of state. This allows rich local queries and scans against this state. These tables are made fault-tolerant by producing a "changelog" stream which is used to restore the state of the table on fail-over. This stream is just another Samza stream, it can even be used as input for other jobs.

![Stateful Processing](/img/samza_state.png)

In our experience most processing flows require joins against other data sourceIn the absence of state maintenance, any joining or aggregation has to be done by querying an external data system. This tends to be one or two orders of magnitude slower than sequential processing. For example per-node throughput for Kafka would easily be in the 100k-500k messages/sec range (depending on message size) but remote queries against a key-value store tend to be closer to 1-5k queries-per-second per node.

Worse mixing in queries from throughput-oriented stream processing on databases and services that also support live user traffic with low latency is a recipe for disaster. By offloading this into the stream processing system you effectively isolate the high-throughput stream processing from low-latency systems.

By instead moving the data to the processing remote communication is completely eliminated for reads. If the data, once partitioned, fits in memory, then these lookups will be purely in memory and can run at outrageously fast rates.

This pattern is not always appropriate and not required (nothing prevents external calls). To make use of this approach you must be able to produce a feed of changes from your databases, which not everyone can do. It also may be the case that much of the logic required to access the data properly is in an online service. In this case calling the service from your Samza job may be more convenient.

### Execution Framework

One final decision we made was to not build a custom distributed execution system in Samza. Instead execution is pluggable and currently completely handled by YARN. This has two benefits.

The first benefit is practical&mdash;there is another team of smart people working on the execution framework. YARN supports a rich set of features around resource quotas and security and is developing at a rapid pace. This allows you to control both what portion of the cluster is allocated to which users and groups of users and also control the resource utilization on individual nodes (CPU, memory, etc) via CGroups. YARN is run at massive scale to support Hadoop and will likely become an ubiquitous layer. Since Samza runs entirely through YARN there is no separate daemons or masters to run beyond the YARN cluster itself (in other words if you already have Kafka and YARN you don't need to install anything to run Samza jobs).

Secondly our integration with YARN is completely componentized. It exists in a separate package with no build time dependency on the main framework. This allows replacing YARN with other virtualization frameworks. In particular we are interested in adding direct AWS integration. Many companies run in AWS which is itself a virtualization framework, which, for Samza's purposes is equivalent to YARN--it allows you to create and destroy virtual "container" machines and guarantees fixed resources for these containers. Since stream processing jobs run "forever" it is a bit silly to run a YARN cluster inside AWS and then try to fill up this cluster with individual jobs. Instead a more sensible approach would just to directly allocate a set of EC2 instances for your jobs.

We think there will be a lot of innovation both in open source virtualization frameworks like Mesos and YARN and in commercial cloud providers like Amazon so integrating with these makes sense.

## [MUPD8 &raquo;](mupd8.html)
