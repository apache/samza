---
layout: page
title: Spark Streaming
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

*This overview is comparing Spark Streaming 1.3.1 and Samza 0.9.0. Things may change in the future versions.*

[Spark Streaming](http://spark.apache.org/docs/latest/streaming-programming-guide.html) is a stream processing system that uses the core [Apache Spark](http://spark.apache.org/) API. Both Samza and Spark Streaming provide data consistency, fault tolerance, a programming API, etc. Spark's approach to streaming is different from Samza's. Samza processes messages as they are received, while Spark Streaming treats streaming as a series of deterministic batch operations. Spark Streaming groups the stream into batches of a fixed duration (such as 1 second). Each batch is represented as a Resilient Distributed Dataset ([RDD](http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf)). A neverending sequence of these RDDs is called a Discretized Stream ([DStream](http://www.cs.berkeley.edu/~matei/papers/2012/hotcloud_spark_streaming.pdf)).

### Overview of Spark Streaming

Before going into the comparison, here is a brief overview of the Spark Streaming application. If you already are familiar with Spark Streaming, you may skip this part. There are two main parts of a Spark Streaming application: data receiving and data processing. 

* Data receiving is accomplished by a [receiver](https://spark.apache.org/docs/latest/streaming-custom-receivers.html) which receives data and stores data in Spark (though not in an RDD at this point). They are experiementing a [non-receiver approach](https://spark.apache.org/docs/latest/streaming-kafka-integration.html#approach-2-direct-approach-no-receivers) for Kafka in the 1.3 release.
* Data processing transfers the data stored in Spark into the DStream. You can then apply the two [operations](https://spark.apache.org/docs/latest/streaming-programming-guide.html#operations) -- transformations and output operations -- on the DStream. The operations for DStream are a little different from what you can use for the general Spark RDD because of the streaming environment.

Here is an overview of the Spark Streaming's [deploy](https://spark.apache.org/docs/latest/cluster-overview.html). Spark has a SparkContext (in SparkStreaming, it’s called [StreamingContext](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.StreamingContext) object in the driver program. The SparkContext talks with cluster manager (e.g. YARN, Mesos) which then allocates resources (that is, executors) for the Spark application. And executors will run tasks sent by the SparkContext ([read more](http://spark.apache.org/docs/latest/cluster-overview.html#compenents)). In YARN’s context, one executor is equivalent to one container. Tasks are what is running in the containers. The driver program runs in the client machine that submits job ([client mode](https://spark.apache.org/docs/latest/running-on-yarn.html#launching-spark-on-yarn)) or in the application manager ([cluster mode](https://spark.apache.org/docs/latest/running-on-yarn.html#launching-spark-on-yarn)). Both data receiving and data processing are tasks for executors. One receiver (receives one input stream) is a long-running task. Processing has a bunch of tasks. All the tasks are sent to the available executors.

### Ordering of Messages

Spark Streaming guarantees ordered processing of RDDs in one DStream. Since each RDD is processed in parallel, there is not order guaranteed within the RDD. This is a tradeoff design Spark made. If you want to process the messages in order within the RDD, you have to process them in one thread, which does not have the benefit of parallelism.

Samza guarantees processing the messages as the order they appear in the partition of the stream. Samza also allows you to define a deterministic ordering of messages between partitions using a [MessageChooser](../container/streams.html). 

### Fault-tolerance semantics

Spark Streaming has different fault-tolerance semantics for different data sources. Here, for a better comparison, only discuss the semantic when using Spark Streaming with Kafka. In Spark 1.2, Spark Streaming provides at-least-once semantic in the receiver side (See the [post](https://databricks.com/blog/2015/01/15/improved-driver-fault-tolerance-and-zero-data-loss-in-spark-streaming.html)). In Spark 1.3, it uses the no-receiver approach ([more detail](https://spark.apache.org/docs/latest/streaming-kafka-integration.html#approach-2-direct-approach-no-receivers)), which provides some benefits. However, it still does not guarantee exactly-once semantics for output actions. Because the side-effecting output operations maybe repeated when the job fails and recovers from the checkpoint. If the updates in your output operations are not idempotent or transactional (such as send messages to a Kafka topic), you will get duplicated messages. Do not be confused by the "exactly-once semantic" in Spark Streaming guide. This only means a given item is only processed once and always gets the same result (Also check the "Delivery Semantics" section [posted](http://blog.cloudera.com/blog/2015/03/exactly-once-spark-streaming-from-apache-kafka/) by Cloudera).

Samza provides an at-least-once message delivery guarantee. When the job failure happens, it restarts the containers and reads the latest offset from the [checkpointing](../container/checkpointing.html). When a Samza job recovers from a failure, it's possible that it will process some data more than once. This happens because the job restarts at the last checkpoint, and any messages that had been processed between that checkpoint and the failure are processed again. The amount of reprocessed data can be minimized by setting a small checkpoint interval period.

There is possible for both Spark Streaming and Samza to achieve end-to-end exactly-once semantics if you can ensure [idempotent updates or transactional updates](https://spark.apache.org/docs/latest/streaming-programming-guide.html#semantics-of-output-operations). The link is pointing to the Spark Streaming page, the same idea works in the Samza as well.

### State Management

Spark Streaming provides a state DStream which keeps the state for each key and a transformation operation called [updateStateByKey](https://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations) to mutate state. Everytime updateStateByKey is applied, you will get a new state DStream where all of the state is updated by applying the function passed to updateStateByKey. This transformation can serve as a basic key-value store, though it has a few drawbacks:

* you can only apply the DStream operations to your state because essentially it's a DStream.
* does not provide any key-value access to the data. If you want to access a certain key-value, you need to iterate the whole DStream.
* it is inefficient when the state is large because every time a new batch is processed, Spark Streaming consumes the entire state DStream to update relevant keys and values.

Spark Streaming periodically writes intermedia data of stateful operations (updateStateByKey and window-based operations) into the HDFS. In the case of updateStateByKey, the entire state RDD is written into the HDFS after every checkpointing interval. As we mentioned in the *[in memory state with checkpointing](../container/state-management.html#in-memory-state-with-checkpointing)*, writing the entire state to durable storage is very expensive when the state becomes large.

Samza uses an embedded key-value store for [state management](../container/state-management.html#local-state-in-samza). This store is replicated as it's mutated, and supports both very high throughput writing and reading. And it gives you a lot of flexibility to decide what kind of state you want to maintain. What is more, you can also plug in other [storage engines](../container/state-management.html#other-storage-engines), which enables great flexibility in the stream processing algorithms you can use. A good comparison of different types of state manager approaches can be found [here](../container/state-management.html#approaches-to-managing-task-state).

One of the common use cases in state management is [stream-stream join](../container/state-management.html#stream-stream-join). Though Spark Streaming has the [join](https://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations) operation, this operation only joins two batches that are in the same time interval. It does not deal with the situation where events in two streams have mismatch. Spark Streaming's updateStateByKey approach to store mismatch events also has the limitation because if the number of mismatch events is large, there will be a large state, which causes the inefficience in Spark Streaming. While Samza does not have this limitation.

### Partitioning and Parallelism

Spark Streaming's Parallelism is achieved by splitting the job into small tasks and sending them to executors. There are two types of [parallelism in Spark Streaming](http://spark.apache.org/docs/latest/streaming-programming-guide.html#level-of-parallelism-in-data-receiving): parallelism in receiving the stream and parallelism in processing the stream:
* On the receiving side, one input DStream creates one receiver, and one receiver receives one input stream of data and runs as a long-running task. So in order to parallelize the receiving process, you can split one input stream into multiple input streams based on some criteria (e.g. if you are receiving a Kafka stream with some partitions, you may split this stream based on the partition). Then you can create multiple input DStreams (so multiple receivers) for these streams and the receivers will run as multiple tasks. Accordingly, you should provide enough resources by increasing the core number of the executors or bringing up more executors. Then you can combine all the input Dstreams into one DStream during the processing if necessary. In Spark 1.3, Spark Streaming + Kafka Integration is using the no-receiver approach (called directSream). Spark Streaming creates a RDD whose partitions map to the Kafka partitions one-to-one. This simplifies the parallelism in the receiver side.
* On the processing side, since a DStream is a continuous sequence of RDDs, the parallelism is simply accomplished by normal RDD operations, such as map, reduceByKey, reduceByWindow (check [here](https://spark.apache.org/docs/latest/tuning.html#level-of-parallelism)).

Samza’s parallelism is achieved by splitting processing into independent [tasks](../api/overview.html) which can be parallelized. You can run multiple tasks in one container or only one task per container. That depends on your workload and latency requirement. For example, if you want to quickly [reprocess a stream](../jobs/reprocessing.html), you may increase the number of containers to one task per container. It is important to notice that one container only uses [one thread](../container/event-loop.html), which maps to exactly one CPU. This design attempts to simplify  resource management and the isolation between jobs.

In Samza, you have the flexibility to define what one task contains. For example, in the Kafka use case, by default, Samza groups the partitions with the same partition id into one task. This allows easy join between different streams. Out-of-box, Samza also provides the grouping strategy which assigns one partition to one task. This provides maximum scalability in terms of how many containers can be used to process those input streams and is appropriate for very high volume jobs that need no grouping of the input streams.

### Buffering &amp; Latency

Spark streaming essentially is a sequence of small batch processes. With a fast execution engine, it can reach the latency as low as one second (from their [paper](http://www.cs.berkeley.edu/~matei/papers/2012/hotcloud_spark_streaming.pdf)). From their [page](https://spark.apache.org/docs/latest/streaming-programming-guide.html#level-of-parallelism-in-data-receiving), "the recommended minimum value of block interval is about 50 ms, below which the task launching overheads may be a problem."

If the processing is slower than receiving, the data will be queued as DStreams in memory and the queue will keep increasing. In order to run a healthy Spark streaming application, the system should be [tuned](http://spark.apache.org/docs/latest/streaming-programming-guide.html#performance-tuning) until the speed of processing is as fast as receiving.

Samza jobs can have latency in the low milliseconds when running with Apache Kafka. It has a different approach to buffering. The buffering mechanism is dependent on the input and output system. For example, when using [Kafka](http://kafka.apache.org/) as the input and output system, data is actually buffered to disk. This design decision, by sacrificing a little latency, allows the buffer to absorb a large backlog of messages when a job has fallen behind in its processing.

### Fault-tolerance

There are two kinds of failures in both Spark Streaming and Samza: worker node (running executors) failure in Spark Streaming (equivalent to container failure in Samza) and driver node (running driver program) failure (equivalent to application manager (AM) failure in Samza).

When a worker node fails in Spark Streaming, it will be restarted by the cluster manager. When a container fails in Samza, the application manager will work with YARN to start a new container. When a driver node fails in Spark Streaming, YARN/Mesos/Spark Standalone will automatically restart the driver node. Spark Streaming can use the checkpoint in HDFS to recreate the StreamingContext. 

In Samza, YARN takes care of the fault-tolerance. When the AM fails in Samza, YARN will handle restarting the AM. Samza will restart all the containers if the AM restarts. When the container fails, the AM will bring up a new container.

### Deployment &amp; Execution

Spark has a SparkContext object to talk with cluster managers, which then allocate resources for the application. Currently Spark supports three types of cluster managers: [Spark standalone](http://spark.apache.org/docs/latest/spark-standalone.html), [Apache Mesos](http://mesos.apache.org/) and [Hadoop YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html). Besides these, Spark has a script for launching in [Amazon EC2](http://spark.apache.org/docs/latest/ec2-scripts.html).

Samza supports YARN and local execution currently. There is also [Mesos support](https://github.com/banno/samza-mesos) being integrating.

### Isolation

Spark Streaming and Samza have the same isolation. Spark Streaming depends on cluster managers (e.g Mesos or YARN) and Samza depend on YARN/Mesos to provide processor isolation. Different applications run in different JVMs. Data cannot be shared among different applications unless it is written to external storage. Since Samza provides out-of-box Kafka integration, it is very easy to reuse the output of other Samza jobs (see [here](../introduction/concepts.html#dataflow-graphs)).

### Language Support

Spark Streaming is written in Java and Scala and provides Scala, Java, and Python APIs. Samza is written in Java and Scala and has a Java API.

### Workflow

In Spark Streaming, you build an entire processing graph with a DSL API and deploy that entire graph as one unit. The communication between the nodes in that graph (in the form of DStreams) is provided by the framework. That is a similar to Storm. Samza is totally different -- each job is just a message-at-a-time processor, and there is no framework support for topologies. Output of a processing task always needs to go back to a message broker (e.g. Kafka).

A positive consequence of Samza's design is that a job's output can be consumed by multiple unrelated jobs, potentially run by different teams, and those jobs are isolated from each other through Kafka's buffering. That is not the case with Storm's and Spark Streaming's framework-internal streams.

Although a Storm/Spark Streaming job could in principle write its output to a message broker, the framework doesn't really make this easy. It seems that Storm/Spark aren't intended to used in a way where one topology's output is another topology's input. By contrast, in Samza, that mode of usage is standard.

### Maturity

Spark has an active user and developer community, and recently releases 1.3.1 version. It has a list of companies that use it on its [Powered by](https://cwiki.apache.org/confluence/display/SPARK/Powered+By+Spark) page. Since Spark contains Spark Streaming, Spark SQL, MLlib, GraphX and Bagel, it's tough to tell what portion of companies on the list are actually using Spark Streaming, and not just Spark.

Samza is still young, but has just released version 0.9.0. It has a responsive community and is being developed actively. That said, it is built on solid systems such as YARN and Kafka. Samza is heavily used at LinkedIn and [other companies](https://cwiki.apache.org/confluence/display/SAMZA/Powered+By). we hope others will find it useful as well.

## [API Overview &raquo;](../api/overview.html)
