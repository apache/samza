---
layout: page
title: Low level Task API
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

### Table Of Contents
- [Introduction](#introduction)
- [Code Examples](#code-examples)
- [Key Concepts](#key-concepts)
  - [TaskApplication](#taskapplication)
  - [TaskFactory](#taskfactory)
  - [Task Interfaces](#task-interfaces)
      - [StreamTask](#streamtask)
      - [AsyncStreamTask](#asyncstreamtask)
      - [Additional Task Interfaces](#additional-task-interfaces)
          - [InitableTask](#initabletask)
          - [ClosableTask](#closabletask)
          - [WindowableTask](#windowabletask)
          - [EndOfStreamListenerTask](#endofstreamlistenertask) 
- [Common Operations](#common-operations)
  - [Receiving Messages from Input Streams](#receiving-messages-from-input-streams)
  - [Sending Messages to Output Streams](#sending-messages-to-output-streams)
  - [Accessing Tables](#accessing-tables)
- [Legacy Applications](#legacy-applications)

### Introduction
Samza's powerful Low Level Task API lets you write your application in terms of processing logic for each incoming message. When using the Low Level Task API, you implement a [TaskApplication](javadocs/org/apache/samza/application/TaskApplication). The processing logic is defined as either a [StreamTask](javadocs/org/apache/samza/task/StreamTask) or an [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask).


### Code Examples

The [Hello Samza](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/wikipedia/task/application) Wikipedia applications demonstrate how to use Samza's Low Level Task API. These applications consume various events from Wikipedia, transform them, and calculates several statistics about them.  

- The [WikipediaFeedTaskApplication](https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/wikipedia/task/application/WikipediaFeedTaskApplication.java) demonstrates how to consume multiple Wikipedia event streams and merge them to an Apache Kafka topic. 

- The [WikipediaParserTaskApplication](https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/wikipedia/task/application/WikipediaParserTaskApplication.java) demonstrates how to project the incoming events from the Apache Kafka topic to a custom JSON data type.

- The [WikipediaStatsTaskApplication](https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/wikipedia/task/application/WikipediaStatsTaskApplication.java) demonstrates how to calculate and emit periodic statistics about the incoming events while using a local KV store for durability.

### Key Concepts

#### TaskApplication

A [TaskApplication](javadocs/org/apache/samza/application/TaskApplication) describes the inputs, outputs, state, configuration and the processing logic for an application written using Samza's Low Level Task API.

A typical TaskApplication implementation consists of the following stages:

 1. Configuring the inputs, outputs and state (tables) using the appropriate [SystemDescriptor](javadocs/org/apache/samza/system/descriptors/SystemDescriptor)s, [InputDescriptor](javadocs/org/apache/samza/descriptors/InputDescriptor)s, [OutputDescriptor](javadocs/org/apache/samza/system/descriptors/OutputDescriptor)s and [TableDescriptor](javadocs/org/apache/samza/table/descriptors/TableDescriptor)s.
 2. Adding the descriptors above to the provided [TaskApplicationDescriptor](javadocs/org/apache/samza/application/descriptors/TaskApplicationDescriptor)
 3. Defining the processing logic in a [StreamTask](javadocs/org/apache/samza/task/StreamTask) or an [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask) implementation, and adding its corresponding [StreamTaskFactory](javadocs/org/apache/samza/task/StreamTaskFactory) or [AsyncStreamTaskFactory](javadocs/org/apache/samza/task/AsyncStreamTaskFactory) to the TaskApplicationDescriptor.

The following example TaskApplication removes page views with "bad URLs" from the input stream:
 
{% highlight java %}
    
    public class PageViewFilter implements TaskApplication {
      @Override
      public void describe(TaskApplicationDescriptor appDescriptor) {
        // Step 1: configure the inputs and outputs using descriptors
        KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("kafka")
            .withConsumerZkConnect(ImmutableList.of("..."))
            .withProducerBootstrapServers(ImmutableList.of("...", "..."));
        KafkaInputDescriptor<PageViewEvent> kid = 
            ksd.getInputDescriptor("pageViewEvent", new JsonSerdeV2<>(PageViewEvent.class));
        KafkaOutputDescriptor<PageViewEvent>> kod = 
            ksd.getOutputDescriptor("goodPageViewEvent", new JsonSerdeV2<>(PageViewEvent.class)));
        RocksDbTableDescriptor badUrls = 
            new RocksDbTableDescriptor(“badUrls”, KVSerde.of(new StringSerde(), new IntegerSerde());
            
        // Step 2: Add input, output streams and tables
        appDescriptor
            .withInputStream(kid)
            .withOutputStream(kod)
            .withTable(badUrls)
        
        // Step 3: define the processing logic
        appDescriptor.withTaskFactory(new PageViewFilterTaskFactory());
      }
    }

{% endhighlight %}

#### TaskFactory
Your [TaskFactory](javadocs/org/apache/samza/task/TaskFactory) will be  used to create instances of your Task in each of Samza's processors. If you're implementing a StreamTask, you can provide a [StreamTaskFactory](javadocs/org/apache/samza/task/StreamTaskFactory). Similarly, if you're implementing an AsyncStreamTask, you can provide an [AsyncStreamTaskFactory](javadocs/org/apache/samza/task/AsyncStreamTaskFactory). For example:

{% highlight java %}

    public class PageViewFilterTaskFactory implements StreamTaskFactory {
      @Override
      public StreamTask createInstance() {
        return new PageViewFilterTask();
      }
    }
    
{% endhighlight %}

#### Task Interfaces

Your processing logic can be implemented in a [StreamTask](javadocs/org/apache/samza/task/StreamTask) or an [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask).

##### StreamTask
You can implement a [StreamTask](javadocs/org/apache/samza/task/StreamTask) for synchronous message processing. Samza delivers messages to the task one at a time, and considers each message to be processed when the process method call returns. For example:

{% highlight java %}

    public class PageViewFilterTask implements StreamTask {
      @Override
      public void process(
          IncomingMessageEnvelope envelope, 
          MessageCollector collector, 
          TaskCoordinator coordinator) {
          
          // process the message in the envelope synchronously
      }
    }

{% endhighlight %}

Note that synchronous message processing does not imply sequential execution. Multiple instances of your Task class implementation may still run concurrently within a container. 

##### AsyncStreamTask
You can implement a [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask) for asynchronous message processing. This can be useful when you need to perform long running I/O operations to process a message, e.g., making an http request. For example:

{% highlight java %}

    public class AsyncPageViewFilterTask implements AsyncStreamTask {
      @Override
      public void processAsync(IncomingMessageEnvelope envelope,
          MessageCollector collector,
          TaskCoordinator coordinator,
          TaskCallback callback) {
          
          // process message asynchronously
          // invoke callback.complete or callback.failure upon completion
      }
    }

{% endhighlight %}

Samza delivers the incoming message and a [TaskCallback](javadocs/org/apache/samza/task/TaskCallback) with the processAsync() method call, and considers each message to be processed when its corresponding callback.complete() or callback.failure() has been invoked. If callback.failure() is invoked, or neither callback.complete() or callback.failure() is invoked within <code>task.callback.ms</code> milliseconds, Samza will shut down the running Container. 

If configured, Samza will keep up to <code>task.max.concurrency</code> number of messages processing asynchronously at a time within each Task Instance. Note that while message delivery (i.e., processAsync invocation) is guaranteed to be in-order within a stream partition, message processing may complete out of order when setting <code>task.max.concurrency</code> > 1. 

For more details on asynchronous and concurrent processing, see the [Samza Async API and Multithreading User Guide](../../../tutorials/{{site.version}}/samza-async-user-guide).

##### Additional Task Interfaces

There are a few other interfaces you can implement in your StreamTask or AsyncStreamTask that provide additional functionality.

###### InitableTask
You can implement the [InitableTask](javadocs/org/apache/samza/task/InitableTask) interface to access the [Context](javadocs/org/apache/samza/context/Context). Context provides access to any runtime objects you need in your task,
whether they're provided by the framework, or your own.

{% highlight java %}
    
    public interface InitableTask {
      void init(Context context) throws Exception;
    }
    
{% endhighlight %}

###### ClosableTask
You can implement the [ClosableTask](javadocs/org/apache/samza/task/ClosableTask) to clean up any runtime state during shutdown. This interface is deprecated. It's recommended to use the [ApplicationContainerContext](javadocs/org/apache/samza/context/ApplicationContainerContext) and [ApplicationTaskContext](javadocs/org/apache/samza/context/ApplicationContainerContext) APIs to manage the lifecycle of any runtime objects.

{% highlight java %}

    public interface ClosableTask {
      void close() throws Exception;
    }
    
{% endhighlight %}

###### WindowableTask
You can implement the [WindowableTask](javadocs/org/apache/samza/task/WindowableTask) interface to implement processing logic that is invoked periodically by the framework.

{% highlight java %}
    
    public interface WindowableTask {
      void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception;
    }

{% endhighlight %}

###### EndOfStreamListenerTask
You can implement the [EndOfStreamListenerTask](javadocs/org/apache/samza/task/EndOfStreamListenerTask) interface to implement processing logic that is invoked when a Task Instance has reached the end of all input SystemStreamPartitions it's consuming. This is typically relevant when running Samza as a batch job.

{% highlight java %}

    public interface EndOfStreamListenerTask {
      void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) throws Exception;
    }
    
{% endhighlight %}

### Common Operations

#### Receiving Messages from Input Streams

Samza calls your Task instance's [process](javadocs/org/apache/samza/task/StreamTask#process-org.apache.samza.system.IncomingMessageEnvelope-org.apache.samza.task.MessageCollector-org.apache.samza.task.TaskCoordinator-) or [processAsync](javadocs/org/apache/samza/task/AsyncStreamTask#processAsync-org.apache.samza.system.IncomingMessageEnvelope-org.apache.samza.task.MessageCollector-org.apache.samza.task.TaskCoordinator-org.apache.samza.task.TaskCallback-) method with each incoming message on your input streams. The [IncomingMessageEnvelope](javadocs/org/apache/samza/system/IncomingMessageEnvelope) can be used to obtain the following information: the de-serialized key, the de-serialized message, and the [SystemStreamPartition](javadocs/org/apache/samza/system/SystemStreamPartition) that the message came from.

The key and message objects need to be cast to the correct type in your Task implementation based on the [Serde](javadocs/org/apache/samza/serializers/Serde.html) provided for the InputDescriptor for the input stream.

The [SystemStreamPartition](javadocs/org/apache/samza/system/SystemStreamPartition) object tells you where the message came from. It consists of three parts:
1. The *system*: the name of the system the message came from, as specified for the SystemDescriptor in your TaskApplication. You can have multiple systems for input and/or output, each with a different name.
2. The *stream name*: the name of the stream (e.g., topic, queue) within the input system. This is the physical name of the stream, as specified for the InputDescriptor in your TaskApplication.
3. The [*partition*](javadocs/org/apache/samza/Partition): A stream is normally split into several partitions, and each partition is assigned to one task instance by Samza. 

If you have several input streams for your TaskApplication, you can use the SystemStreamPartition to determine what kind of message you’ve received.

#### Sending Messages to Output Streams
To send a message to a stream, you first create an [OutgoingMessageEnvelope](javadocs/org/apache/samza/system/OutgoingMessageEnvelope). At a minimum, you need to provide the message you want to send, and the system and stream to send it to. Optionally you can specify the partitioning key and other parameters. See the [javadoc](javadocs/org/apache/samza/system/OutgoingMessageEnvelope) for details.

You can then send the OutgoingMessageEnvelope using the [MessageCollector](javadocs/org/apache/samza/task/MessageCollector) provided with the process() or processAsync() call. You **must** use the MessageCollector delivered for the message you're currently processing. Holding on to a MessageCollector and reusing it later will cause your messages to not be sent correctly.  

{% highlight java %}
    
    /** When a task wishes to send a message, it uses this interface. */
    public interface MessageCollector {
      void send(OutgoingMessageEnvelope envelope);
    }
    
{% endhighlight %}

#### Accessing Tables

A [Table](javadocs/org/apache/samza/table/Table) is an abstraction for data sources that support random access by key. It is an evolution of the older [KeyValueStore](javadocs/org/apache/samza/storage/kv/KeyValueStore) API. It offers support for both local and remote data sources and composition through hybrid tables. For remote data sources, a [RemoteTable] provides optimized access with caching, rate-limiting, and retry support. Depending on the implementation, a Table can be a [ReadableTable](javadocs/org/apache/samza/table/ReadableTable) or a [ReadWriteTable](javadocs/org/apache/samza/table/ReadWriteTable).
 
In the Low Level API, you can obtain and use a Table as follows:

1. Use the appropriate TableDescriptor to specify the table properties.
2. Register the TableDescriptor with the TaskApplicationDescriptor.
3. Obtain a Table reference within the task implementation using [TaskContext.getTable()](javadocs/org/apache/samza/task/TaskContext#getTable-java.lang.String-). [TaskContext](javadocs/org/apache/samza/task/TaskContext) is available via [Context.getTaskContext()](javadocs/org/apache/samza/context/Context#getTaskContext--), which in turn is available by implementing [InitiableTask. init()]((javadocs/org/apache/samza/task/InitableTask#init-org.apache.samza.context.Context-)).

For example:

{% highlight java %}

    public class PageViewFilterTask implements StreamTask, InitableTask {
      private final SystemStream outputStream = new SystemStream(“kafka”, “goodPageViewEvent”);
      
      private ReadWriteTable<String, Integer> badUrlsTable;
      
      @Override
      public void init(Context context) {
        badUrlsTable = (ReadWriteTable<String, Integer>) context.getTaskContext().getTable("badUrls");
      }

      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String key = (String) message.getKey();
        if (badUrlsTable.containsKey(key)) {
          // skip the message, increment the counter, do not send it
          badPageUrlTable.put(key, badPageUrlTable.get(key) + 1);
        } else {
          collector.send(new OutgoingMessageEnvelope(outputStream, key, message.getValue()));   }
      }
    }

{% endhighlight %}

### Legacy Applications

For legacy Low Level API applications, you can continue specifying your system, stream and store properties along with your task.class in configuration. An incomplete example of configuration for legacy task application looks like this (see the [configuration](../jobs/configuration.md) documentation for more detail):

{% highlight jproperties %}

    # This is the Task class that Samza will instantiate when the job is run
    task.class=com.example.samza.PageViewFilterTask

    # Define a system called "kafka" (you can give it any name, and you can define
    # multiple systems if you want to process messages from different sources)
    systems.kafka.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory

    # The job consumes a topic called "PageViewEvent" from the "kafka" system
    task.inputs=kafka.PageViewEvent

    # Define a serializer/deserializer called "json" which parses JSON messages
    serializers.registry.json.class=org.apache.samza.serializers.JsonSerdeFactory

    # Use the "json" serializer for messages in the "PageViewEvent" topic
    systems.kafka.streams.PageViewEvent.samza.msg.serde=json
    
{% endhighlight %}