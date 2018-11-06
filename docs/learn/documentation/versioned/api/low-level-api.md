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


# Introduction
Task APIs (i.e. [StreamTask](javadocs/org/apache/samza/task/StreamTask.html) or [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask.html)) are bare-metal interfaces that exposes the system implementation details in Samza. When using Task APIs, you will implement your application as a [TaskApplication](javadocs/org/apache/samza/application/TaskApplication.html). The main difference between a TaskApplication and a StreamApplication is the APIs used to describe the processing logic. In TaskApplication, the processing logic is defined via StreamTask and AsyncStreamTask.

# Key Concepts

## TaskApplication
Here is an example of a user implemented TaskApplication:

{% highlight java %}
    
    package com.example.samza;

    public class BadPageViewFilter implements TaskApplication {
      @Override
      public void describe(TaskApplicationDescriptor appDesc) {
        // Add input, output streams and tables
        KafkaSystemDescriptor<String, PageViewEvent> kafkaSystem = 
            new KafkaSystemDescriptor(“kafka”)
              .withConsumerZkConnect(myZkServers)
              .withProducerBootstrapServers(myBrokers);
        KVSerde<String, PageViewEvent> serde = 
            KVSerde.of(new StringSerde(), new JsonSerdeV2<PageViewEvent>());
        // Add input, output streams and tables
        appDesc.withInputStream(kafkaSystem.getInputDescriptor(“pageViewEvent”, serde))
            .withOutputStream(kafkaSystem.getOutputDescriptor(“goodPageViewEvent”, serde))
            .withTable(new RocksDBTableDescriptor(
                “badPageUrlTable”, KVSerde.of(new StringSerde(), new IntegerSerde())
            .withTaskFactory(new BadPageViewTaskFactory());
      }
    }

{% endhighlight %}

In the above example, user defines the input stream, the output stream, and a RocksDB table for the application, and then provide the processing logic defined in BadPageViewTaskFactory. All descriptors (i.e. input/output streams and tables) and the [TaskFactory](javadocs/org/apache/samza/task/TaskFactory.html) are registered to the [TaskApplicationDescriptor](javadocs/org/apache/samza/application/descriptors/TaskApplicationDescriptor.html).

## TaskFactory
You will need to implement a [TaskFactory](javadocs/org/apache/samza/task/TaskFactory.html) to create task instances to execute user defined processing logic. Correspondingly, StreamTaskFactory and AsyncStreamTaskFactory are used to create StreamTask and AsyncStreamTask respectively. The [StreamTaskFactory](javadocs/org/apache/samza/task/StreamTaskFactory.html) for the above example is shown below:

{% highlight java %}

    package com.example.samza;

    public class BadPageViewTaskFactory implements StreamTaskFactory {
      @Override
      public StreamTask createInstance() {
        // Add input, output streams and tables
        return new BadPageViewFilterTask();
      }
    }
    
{% endhighlight %}

Similarly, here is an example of [AsyncStreamTaskFactory](javadocs/org/apache/samza/task/AsyncStreamTaskFactory.html):

{% highlight java %}
    
    package com.example.samza;

    public class BadPageViewAsyncTaskFactory implements AsyncStreamTaskFactory {
      @Override
      public AsyncStreamTask createInstance() {
        // Add input, output streams and tables
        return new BadPageViewAsyncFilterTask();
      }
    }
{% endhighlight %}

## Task classes

The actual processing logic is implemented in [StreamTask](javadocs/org/apache/samza/task/StreamTask.html) or [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask.html) classes.

### StreamTask
You should implement [StreamTask](javadocs/org/apache/samza/task/StreamTask.html) for synchronous process, where the message processing is complete after the process method returns. An example of StreamTask is a computation that does not involve remote calls:

{% highlight java %}
    
    package com.example.samza;

    public class BadPageViewFilterTask implements StreamTask {
      @Override
      public void process(IncomingMessageEnvelope envelope,
                          MessageCollector collector,
                          TaskCoordinator coordinator) {
        // process message synchronously
      }
    }
{% endhighlight %}

### AsyncStreamTask
The [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask.html) interface, on the other hand, supports asynchronous process, where the message processing may not be complete after the processAsync method returns. Various concurrent libraries like Java NIO, ParSeq and Akka can be used here to make asynchronous calls, and the completion is marked by invoking the [TaskCallback](javadocs/org/apache/samza/task/TaskCallback.html). Samza will continue to process next message or shut down the container based on the callback status. An example of AsyncStreamTask is a computation that make remote calls but don’t block on the call completion:

{% highlight java %}
    
    package com.example.samza;

    public class BadPageViewAsyncFilterTask implements AsyncStreamTask {
      @Override
      public void processAsync(IncomingMessageEnvelope envelope,
                               MessageCollector collector,
                               TaskCoordinator coordinator,
                               TaskCallback callback) {
        // process message with asynchronous calls
        // fire callback upon completion, e.g. invoking callback from asynchronous call completion thread
      }
    }
{% endhighlight %}

# Runtime Objects

## Task Instances in Runtime
When you run your job, Samza will create many instances of your task class (potentially on multiple machines). These task instances process the messages from the input streams.

## Messages from Input Streams

For each message that Samza receives from the task’s input streams, the [process](javadocs/org/apache/samza/task/StreamTask.html#process-org.apache.samza.system.IncomingMessageEnvelope-org.apache.samza.task.MessageCollector-org.apache.samza.task.TaskCoordinator-) or [processAsync](javadocs/org/apache/samza/task/AsyncStreamTask.html#processAsync-org.apache.samza.system.IncomingMessageEnvelope-org.apache.samza.task.MessageCollector-org.apache.samza.task.TaskCoordinator-org.apache.samza.task.TaskCallback-) method is called. The [envelope](javadocs/org/apache/samza/system/IncomingMessageEnvelope.html) contains three things of importance: the message, the key, and the stream that the message came from.

{% highlight java %}
    
    /** Every message that is delivered to a StreamTask is wrapped
     * in an IncomingMessageEnvelope, which contains metadata about
     * the origin of the message. */
    public class IncomingMessageEnvelope {
      /** A deserialized message. */
      Object getMessage() { ... }

      /** A deserialized key. */
      Object getKey() { ... }

      /** The stream and partition that this message came from. */
      SystemStreamPartition getSystemStreamPartition() { ... }
    }
{% endhighlight %}

The key and value are declared as Object, and need to be cast to the correct type. The serializer/deserializer are defined via InputDescriptor, as described [here](high-level-api.md#data-serialization). A deserializer can convert these bytes into any other type, for example the JSON deserializer mentioned above parses the byte array into java.util.Map, java.util.List and String objects.

The [getSystemStreamPartition()](javadocs/org/apache/samza/system/IncomingMessageEnvelope.html#getSystemStreamPartition--) method returns a [SystemStreamPartition](javadocs/org/apache/samza/system/SystemStreamPartition.html) object, which tells you where the message came from. It consists of three parts:
1. The *system*: the name of the system from which the message came, as defined as SystemDescriptor in your TaskApplication. You can have multiple systems for input and/or output, each with a different name.
2. The *stream name*: the name of the stream (topic, queue) within the source system. This is also defined as InputDescriptor in the TaskApplication.
3. The [*partition*](javadocs/org/apache/samza/Partition.html): a stream is normally split into several partitions, and each partition is assigned to one task instance by Samza.

The API looks like this:

{% highlight java %}
    
    /** A triple of system name, stream name and partition. */
    public class SystemStreamPartition extends SystemStream {

      /** The name of the system which provides this stream. It is
          defined in the Samza job's configuration. */
      public String getSystem() { ... }

      /** The name of the stream/topic/queue within the system. */
      public String getStream() { ... }

      /** The partition within the stream. */
      public Partition getPartition() { ... }
    }
{% endhighlight %}

In the example user-implemented TaskApplication above, the system name is “kafka”, the stream name is “pageViewEvent”. (The name “kafka” isn’t special — you can give your system any name you want.) If you have several input streams feeding into your StreamTask or AsyncStreamTask, you can use the SystemStreamPartition to determine what kind of message you’ve received.

## Messages to Output Streams
What about sending messages? If you take a look at the [process()](javadocs/org/apache/samza/task/StreamTask.html#process-org.apache.samza.system.IncomingMessageEnvelope-org.apache.samza.task.MessageCollector-org.apache.samza.task.TaskCoordinator-) method in StreamTask, you’ll see that you get a [MessageCollector](javadocs/org/apache/samza/task/MessageCollector.html). Similarly, you will get it in [processAsync()](javadocs/org/apache/samza/task/AsyncStreamTask.html#processAsync-org.apache.samza.system.IncomingMessageEnvelope-org.apache.samza.task.MessageCollector-org.apache.samza.task.TaskCoordinator-org.apache.samza.task.TaskCallback-) method in AsyncStreamTask as well.

{% highlight java %}
    
    /** When a task wishes to send a message, it uses this interface. */
    public interface MessageCollector {
      void send(OutgoingMessageEnvelope envelope);
    }
    
{% endhighlight %}

To send a message, you create an [OutgoingMessageEnvelope](javadocs/org/apache/samza/system/OutgoingMessageEnvelope.html) object and pass it to the MessageCollector. At a minimum, the envelope specifies the message you want to send, and the system and stream name to send it to. Optionally you can specify the partitioning key and other parameters. See the [javadoc](javadocs/org/apache/samza/system/OutgoingMessageEnvelope.html) for details.

**NOTE**: Please only use the MessageCollector object within the process() or processAsync() method. If you hold on to a MessageCollector instance and use it again later, your messages may not be sent correctly.

For example, here’s a simple example to send out “Good PageViewEvents” in the BadPageViewFilterTask:

{% highlight java %}
    
    public class BadPageViewFilterTask implements StreamTask {

      // Send outgoing messages to a stream called "words"
      // in the "kafka" system.
      private final SystemStream OUTPUT_STREAM =
        new SystemStream("kafka", "goodPageViewEvent");
      @Override
      public void process(IncomingMessageEnvelope envelope,
                          MessageCollector collector,
                          TaskCoordinator coordinator) {
        if (isBadPageView(envelope)) {
          // skip the message, increment the counter, do not send it
          return;
        }
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, envelope.getKey(), envelope.getValue()));
      }
    }
    
{% endhighlight %}

## Accessing Tables
There are many cases that you will need to lookup a table when processing an incoming message. Samza allows access to tables by a unique name through [TaskContext.getTable()](javadocs/org/apache/samza/task/TaskContext.html#getTable-java.lang.String-) method. [TaskContext](javadocs/org/apache/samza/task/TaskContext.html) is accessed via [Context.getTaskContext()](javadocs/org/apache/samza/context/Context.html#getTaskContext--) in the [InitiableTask’s init()]((javadocs/org/apache/samza/task/InitableTask.html#init-org.apache.samza.context.Context-)) method. A user code example to access a table in the above TaskApplication example is here:

{% highlight java %}

    public class BadPageViewFilter implements StreamTask, InitableTask {
      private final SystemStream OUTPUT_STREAM = new SystemStream(“kafka”, “goodPageViewEvent”);
      private ReadWriteTable<String, Integer> badPageUrlTable;
      @Override
      public void init(Context context) {
        badPageUrlTable = (ReadWriteTable<String, Integer>) context.getTaskContext().getTable("badPageUrlTable");
      }

      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String key = (String)message.getKey();
        if (badPageUrlTable.containsKey(key)) {
          // skip the message, increment the counter, do not send it
          badPageUrlTable.put(key, badPageUrlTable.get(key) + 1);
          return;
        }
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, key, message.getValue()));
      }
    }

{% endhighlight %}

For more detailed AsyncStreamTask example, follow the tutorial in [Samza Async API and Multithreading User Guide](../../../tutorials/{{site.version}}/samza-async-user-guide.html). For more details on APIs, please refer to [Configuration](../jobs/configuration.md) and [Javadocs](javadocs).

# Other Task Interfaces

There are other task interfaces to allow additional processing logic to be applied, besides the main per-message processing logic defined in StreamTask and AsyncStreamTask. You will need to implement those task interfaces in addition to StreamTask or AsyncStreamTask.

## InitiableTask
This task interface allows users to initialize objects that are accessed within a task instance.

{% highlight java %}
    
    public interface InitableTask {
      void init(Context context) throws Exception;
    }
{% endhighlight %}

## WindowableTask
This task interface allows users to define a processing logic that is invoked periodically within a task instance.

{% highlight java %}
    
    public interface WindowableTask {
      void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception;
    }

{% endhighlight %}

## ClosableTask
This task interface defines the additional logic when closing a task. Usually, it is in pair with InitableTask to release system resources allocated for this task instance.

{% highlight java %}

    public interface ClosableTask {
      void close() throws Exception;
    }
    
{% endhighlight %}

## EndOfStreamListenerTask
This task interface defines the additional logic when a task instance has reached the end of all input SystemStreamPartitions (see Samza as a batch job).

{% highlight java %}

    public interface EndOfStreamListenerTask {
      void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) throws Exception;
    }
    
{% endhighlight %}

# Legacy Task Application

For legacy task application which do not implement TaskApplication interface, you may specify the system, stream, and local stores in your job’s configuration, in addition to task.class. An incomplete example of configuration for legacy task application could look like this (see the [configuration](../jobs/configuration.md) documentation for more detail):

{% highlight jproperties %}

    # This is the class above, which Samza will instantiate when the job is run
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