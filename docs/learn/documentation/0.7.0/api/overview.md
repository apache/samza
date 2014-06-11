---
layout: page
title: API Overview
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

When writing a stream processor for Samza, you must implement the [StreamTask](javadocs/org/apache/samza/task/StreamTask.html) interface:

    package com.example.samza;

    public class MyTaskClass implements StreamTask {

      public void process(IncomingMessageEnvelope envelope,
                          MessageCollector collector,
                          TaskCoordinator coordinator) {
        // process message
      }
    }

When you run your job, Samza will create several instances of your class (potentially on multiple machines). These task instances process the messages in the input streams.

In your job's configuration you can tell Samza which streams you want to consume. An incomplete example could look like this (see the [configuration documentation](../jobs/configuration.html) for more detail):

    # This is the class above, which Samza will instantiate when the job is run
    task.class=com.example.samza.MyTaskClass

    # Define a system called "kafka" (you can give it any name, and you can define
    # multiple systems if you want to process messages from different sources)
    systems.kafka.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory

    # The job consumes a topic called "PageViewEvent" from the "kafka" system
    task.inputs=kafka.PageViewEvent

    # Define a serializer/deserializer called "json" which parses JSON messages
    serializers.registry.json.class=org.apache.samza.serializers.JsonSerdeFactory

    # Use the "json" serializer for messages in the "PageViewEvent" topic
    systems.kafka.streams.PageViewEvent.samza.msg.serde=json

For each message that Samza receives from the task's input streams, the *process* method is called. The [envelope](javadocs/org/apache/samza/system/IncomingMessageEnvelope.html) contains three things of importance: the message, the key, and the stream that the message came from.

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

The key and value are declared as Object, and need to be cast to the correct type. If you don't configure a [serializer/deserializer](../container/serialization.html), they are typically Java byte arrays. A deserializer can convert these bytes into any other type, for example the JSON deserializer mentioned above parses the byte array into java.util.Map, java.util.List and String objects.

The getSystemStreamPartition() method returns a [SystemStreamPartition](javadocs/org/apache/samza/system/SystemStreamPartition.html) object, which tells you where the message came from. It consists of three parts:

1. The *system*: the name of the system from which the message came, as defined in your job configuration. You can have multiple systems for input and/or output, each with a different name.
2. The *stream name*: the name of the stream (topic, queue) within the source system. This is also defined in the job configuration.
3. The [*partition*](javadocs/org/apache/samza/Partition.html): a stream is normally split into several partitions, and each partition is assigned to one StreamTask instance by Samza.

The API looks like this:

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

In the example job configuration above, the system name is "kafka", the stream name is "PageViewEvent". (The name "kafka" isn't special &mdash; you can give your system any name you want.) If you have several input streams feeding into your StreamTask, you can use the SystemStreamPartition to determine what kind of message you've received.

What about sending messages? If you take a look at the process() method in StreamTask, you'll see that you get a [MessageCollector](javadocs/org/apache/samza/task/MessageCollector.html).

    /** When a task wishes to send a message, it uses this interface. */
    public interface MessageCollector {
      void send(OutgoingMessageEnvelope envelope);
    }

To send a message, you create an [OutgoingMessageEnvelope](javadocs/org/apache/samza/system/OutgoingMessageEnvelope.html) object and pass it to the message collector. At a minimum, the envelope specifies the message you want to send, and the system and stream name to send it to. Optionally you can specify the partitioning key and other parameters. See the [javadoc](javadocs/org/apache/samza/system/OutgoingMessageEnvelope.html) for details.

**NOTE:** Please only use the MessageCollector object within the process() method. If you hold on to a MessageCollector instance and use it again later, your messages may not be sent correctly.

For example, here's a simple task that splits each input message into words, and emits each word as a separate message:

    public class SplitStringIntoWords implements StreamTask {

      // Send outgoing messages to a stream called "words"
      // in the "kafka" system.
      private final SystemStream OUTPUT_STREAM =
        new SystemStream("kafka", "words");

      public void process(IncomingMessageEnvelope envelope,
                          MessageCollector collector,
                          TaskCoordinator coordinator) {
        String message = (String) envelope.getMessage();

        for (String word : message.split(" ")) {
          // Use the word as the key, and 1 as the value.
          // A second task can add the 1's to get the word count.
          collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, word, 1));
        }
      }
    }

## [SamzaContainer &raquo;](../container/samza-container.html)
