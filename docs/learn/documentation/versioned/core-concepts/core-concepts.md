---
layout: page
title: Core concepts
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
- [Introduction](#introduction)
- [Streams, Partitions](#streams-partitions)
- [Stream Application](#stream-application)
- [State](#state)
- [Time](#time)
- [Processing guarantee](#processing-guarantee)

### Introduction

Apache Samza is a scalable data processing engine that allows you to process and analyze your data in real-time. Here is a summary of Samza’s features that simplify building your applications:

_**Unified API:**_ Use a simple API to describe your application-logic in a manner independent of your data-source. The same API can process both batch and streaming data.

*Pluggability at every level:* Process and transform data from any source. Samza offers built-in integrations with [Apache Kafka](/learn/documentation/{{site.version}}/connectors/kafka.html), [AWS Kinesis](/learn/documentation/{{site.version}}/connectors/kinesis.html), [Azure EventHubs](/learn/documentation/{{site.version}}/connectors/kinesis.html), ElasticSearch and [Apache Hadoop](/learn/documentation/{{site.version}}/connectors/hdfs.html). Also, it’s quite easy to integrate with your own sources.

*Samza as an embedded library:* Integrate effortlessly with your existing applications eliminating the need to spin up and operate a separate cluster for stream processing. Samza can be used as a light-weight client-library [embedded](/learn/documentation/{{site.version}}/deployment/standalone.html) in your Java/Scala applications. 

*Write once, Run anywhere:* [Flexible deployment options](/learn/documentation/{{site.version}}/deployment/deployment-model.html)  to run applications anywhere - from public clouds to containerized environments to bare-metal hardware.

*Samza as a managed service:* Run stream-processing as a managed service by integrating with popular cluster-managers including [Apache YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html). 

*Fault-tolerance:*  Transparently migrate tasks along with their associated state in the event of failures. Samza supports [host-affinity](/learn/documentation/{{site.version}}/architecture/architecture-overview.html#host-affinity) and [incremental checkpointing](/learn/documentation/{{site.version}}/architecture/architecture-overview.html#incremental-checkpoints) to enable fast recovery from failures.

*Massive scale:* Battle-tested on applications that use several terabytes of state and run on thousands of cores. It [powers](/powered-by/) multiple large companies including LinkedIn, Uber, TripAdvisor, Slack etc. 

Next, we will introduce Samza’s terminology. You will realize that it is extremely easy to [get started](/startup/quick-start/{{site.version}}) with building your first application. 


### Streams, Partitions
Samza processes your data in the form of streams. A _stream_ is a collection of immutable messages, usually of the same type or category. Each message in a stream is modelled as a key-value pair. 

![diagram-medium](/img/{{site.version}}/learn/documentation/core-concepts/streams-partitions.png)
<br/>
A stream can have multiple producers that write data to it and multiple consumers that read data from it. Data in a stream can be unbounded (eg: a Kafka topic) or bounded (eg: a set of files on HDFS). 

A stream is sharded into multiple partitions for scaling how its data is processed. Each _partition_ is an ordered, replayable sequence of records. When a message is written to a stream, it ends up in one of its partitions. Each message in a partition is uniquely identified by an _offset_. 

Samza supports pluggable systems that can implement the stream abstraction. As an example, Kafka implements a stream as a topic while a database might implement a stream as a sequence of updates to its tables.

### Stream Application
A _stream application_ processes messages from input streams, transforms them and emits results to an output stream or a database. It is built by chaining multiple operators, each of which takes in one or more streams and transforms them.

![diagram-medium](/img/{{site.version}}/learn/documentation/core-concepts/stream-application.png)

Samza offers foure top-level APIs to help you build your stream applications: <br/>
1. The [High Level Streams API](/learn/documentation/{{site.version}}/api/high-level-api.html),  which offers several built-in operators like map, filter, etc. This is the recommended API for most use-cases. <br/>
2. The [Low Level Task API](/learn/documentation/{{site.version}}/api/low-level-api.html), which allows greater flexibility to define your processing-logic and offers greater control <br/>
3. [Samza SQL](/learn/documentation/{{site.version}}/api/samza-sql.html), which offers a declarative SQL interface to create your applications <br/>
4. [Apache Beam API](/learn/documentation/{{site.version}}/api/beam-api.html), which offers the full Java API from [Apache beam](https://beam.apache.org/) while Python and Go are work-in-progress.

### State
Samza supports both stateless and stateful stream processing. _Stateless processing_, as the name implies, does not retain any state associated with the current message after it has been processed. A good example of this is filtering an incoming stream of user-records by a field (eg:userId) and writing the filtered messages to their own stream. 

In contrast, _stateful processing_ requires you to record some state about a message even after processing it. Consider the example of counting the number of unique users to a website every five minutes. This requires you to store information about each user seen thus far for de-duplication. Samza offers a fault-tolerant, scalable state-store for this purpose.

### Time
Time is a fundamental concept in stream processing, especially in how it is modeled and interpreted by the system. Samza supports two notions of time. By default, all built-in Samza operators use processing time. In processing time, the timestamp of a message is determined by when it is processed by the system. For example, an event generated by a sensor could be processed by Samza several milliseconds later. 

On the other hand, in event time, the timestamp of an event is determined by when it actually occurred at the source. For example, a sensor which generates an event could embed the time of occurrence as a part of the event itself. Samza provides event-time based processing by its integration with [Apache BEAM](https://beam.apache.org/documentation/runners/samza/).

### Processing guarantee
Samza supports at-least once processing. As the name implies, this ensures that each message in the input stream is processed by the system at-least once. This guarantees no data-loss even when there are failures, thereby making Samza a practical choice for building fault-tolerant applications.


Next Steps: We are now ready to have a closer look at Samza’s architecture.
### [Architecture &raquo;](/learn/documentation/{{site.version}}/architecture/architecture-overview.html)

