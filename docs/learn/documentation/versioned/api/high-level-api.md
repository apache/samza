---
layout: page
title: High Level Streams API
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
  - [StreamApplication](#streamapplication)
  - [MessageStream](#messagestream)
  - [Table](#table)
- [Operators](#operators)
  - [Map](#map)
  - [FlatMap](#flatmap)
  - [Filter](#filter)
  - [PartitionBy](#partitionby)
  - [Merge](#merge)
  - [Broadcast](#broadcast)
  - [SendTo (Stream)](#sendto-stream)
  - [SendTo (Table)](#sendto-table)
  - [Sink](#sink)
  - [Join (Stream-Stream)](#join-stream-stream)
  - [Join (Stream-Table)](#join-stream-table)
  - [Window](#window)
      - [Windowing Concepts](#windowing-concepts)
      - [Window Types](#window-types)
- [Operator IDs](#operator-ids)
- [Data Serialization](#data-serialization)
- [Application Serialization](#application-serialization)

### Introduction

Samza's flexible High Level Streams API lets you describe your complex stream processing pipeline in the form of a Directional Acyclic Graph (DAG) of operations on [MessageStream](javadocs/org/apache/samza/operators/MessageStream). It provides a rich set of built-in operators that simplify common stream processing operations such as filtering, projection, repartitioning, stream-stream and stream-table joins, and windowing. 

### Code Examples

[The Samza Cookbook](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/cookbook) contains various recipes using the Samza High Level Streams API. These include:

- The [Filter example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/FilterExample.java) demonstrates how to perform stateless operations on a stream. 

- The [Join example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/JoinExample.java]) demonstrates how you can join a Kafka stream of page-views with a stream of ad-clicks

- The [Stream-Table Join example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/RemoteTableJoinExample.java) demonstrates how to use the Samza Table API. It joins a Kafka stream with a remote dataset accessed through a REST service.

- The [SessionWindow](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/SessionWindowExample.java) and [TumblingWindow](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/TumblingWindowExample.java) examples illustrate Samza's rich windowing and triggering capabilities.

### Key Concepts
#### StreamApplication
A [StreamApplication](javadocs/org/apache/samza/application/StreamApplication) describes the inputs, outputs, state, configuration and the processing logic for an application written using Samza's High Level Streams API.

A typical StreamApplication implementation consists of the following stages:

 1. Configuring the inputs, outputs and state (tables) using the appropriate [SystemDescriptor](javadocs/org/apache/samza/system/descriptors/SystemDescriptor)s, [InputDescriptor](javadocs/org/apache/samza/descriptors/InputDescriptor)s, [OutputDescriptor](javadocs/org/apache/samza/system/descriptors/OutputDescriptor)s and [TableDescriptor](javadocs/org/apache/samza/table/descriptors/TableDescriptor)s.
 2. Obtaining the corresponding [MessageStream](javadocs/org/apache/samza/operators/MessageStream)s, [OutputStream](javadocs/org/apache/samza/operators/OutputStream)s and [Table](javadocs/org/apache/samza/table/Table)s from the provided [StreamApplicationDescriptor](javadocs/org/apache/samza/application/descriptors/StreamApplicationDescriptor)
 3. Defining the processing logic using operators and functions on the streams and tables thus obtained.

The following example StreamApplication removes page views older than 1 hour from the input stream:
 
{% highlight java %}
   
    public class PageViewFilter implements StreamApplication {
      public void describe(StreamApplicationDescriptor appDescriptor) {
        // Step 1: configure the inputs and outputs using descriptors
        KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("kafka")
            .withConsumerZkConnect(ImmutableList.of("..."))
            .withProducerBootstrapServers(ImmutableList.of("...", "..."));
        KafkaInputDescriptor<PageViewEvent> kid = 
            ksd.getInputDescriptor("pageViewEvent", new JsonSerdeV2<>(PageViewEvent.class));
        KafkaOutputDescriptor<PageViewEvent>> kod = 
            ksd.getOutputDescriptor("recentPageViewEvent", new JsonSerdeV2<>(PageViewEvent.class)));
  
        // Step 2: obtain the message strems and output streams 
        MessageStream<PageViewEvent> pageViewEvents = appDescriptor.getInputStream(kid);
        OutputStream<PageViewEvent> recentPageViewEvents = appDescriptor.getOutputStream(kod);
  
        // Step 3: define the processing logic
        pageViewEvents
            .filter(m -> m.getCreationTime() > 
                System.currentTimeMillis() - Duration.ofHours(1).toMillis())
            .sendTo(recentPageViewEvents);
      }
    }
  
{% endhighlight %}


#### MessageStream
A [MessageStream](javadocs/org/apache/samza/operators/MessageStream), as the name implies, represents a stream of messages. A StreamApplication is described as a Directed Acyclic Graph (DAG) of transformations on MessageStreams. You can get a MessageStream in two ways:

1. Calling StreamApplicationDescriptor#getInputStream() with an [InputDescriptor](javadocs/org/apache/samza/system/descriptors/InputDescriptor) obtained from a [SystemDescriptor](javadocs/org/apache/samza/system/descriptors/SystemDescriptor).
2. By transforming an existing MessageStream using operators like map, filter, window, join etc.

#### Table
A [Table](javadocs/org/apache/samza/table/Table) is an abstraction for data sources that support random access by key. It is an evolution of the older [KeyValueStore](javadocs/org/apache/samza/storage/kv/KeyValueStore) API. It offers support for both local and remote data sources and composition through hybrid tables. For remote data sources, a [RemoteTable] provides optimized access with caching, rate-limiting, and retry support. Depending on the implementation, a Table can be a [ReadableTable](javadocs/org/apache/samza/table/ReadableTable) or a [ReadWriteTable](javadocs/org/apache/samza/table/ReadWriteTable).
 
In the High Level Streams API, you can obtain and use a Table as follows:

1. Use the appropriate TableDescriptor to specify the table properties.
2. Register the TableDescriptor with the StreamApplicationDescriptor. This returns a Table reference, which can be used for populate the table using the [Send To Table](#sendto-table) operator, or for joining a stream with the table using the [Stream-Table Join](#join-stream-table) operator.
3. Alternatively, you can obtain a Table reference within an operator's [InitableFunction](javadocs/org/apache/samza/operators/functions/InitableFunction) using the provided [TaskContext](javadocs/org/apache/samza/context/TaskContext).

### Operators
The High Level Streams API provides common operations like map, flatmap, filter, merge, broadcast, joins, and windows on MessageStreams. Most of these operators accept their corresponding Functions as an argument. 

#### Map
Applies the provided 1:1 [MapFunction](javadocs/org/apache/samza/operators/functions/MapFunction) to each element in the MessageStream and returns the transformed MessageStream. The MapFunction takes in a single message and returns a single message (potentially of a different type).

{% highlight java %}

    MessageStream<Integer> numbers = ...
    MessageStream<Integer> tripled = numbers.map(m -> m * 3);
    MessageStream<String> stringified = numbers.map(m -> String.valueOf(m));

{% endhighlight %}

#### FlatMap
Applies the provided 1:n [FlatMapFunction](javadocs/org/apache/samza/operators/functions/FlatMapFunction) to each element in the MessageStream and returns the transformed MessageStream. The FlatMapFunction takes in a single message and returns zero or more messages.

{% highlight java %}
    
    MessageStream<String> sentence = ...
    // Parse the sentence into its individual words splitting on space
    MessageStream<String> words = sentence.flatMap(sentence ->
        Arrays.asList(sentence.split(“ ”))

{% endhighlight %}

#### Filter
Applies the provided [FilterFunction](javadocs/org/apache/samza/operators/functions/FilterFunction) to the MessageStream and returns the filtered MessageStream. The FilterFunction is a predicate that specifies whether a message should be retained in the filtered stream. Messages for which the FilterFunction returns false are filtered out.

{% highlight java %}
    
    MessageStream<String> words = ...
    // Extract only the long words
    MessageStream<String> longWords = words.filter(word -> word.size() > 15);
    // Extract only the short words
    MessageStream<String> shortWords = words.filter(word -> word.size() < 3);
    
{% endhighlight %}

#### PartitionBy
Re-partitions this MessageStream using the key returned by the provided keyExtractor and returns the transformed MessageStream. Messages are sent through an intermediate stream during repartitioning.

{% highlight java %}
    
    MessageStream<PageView> pageViews = ...
    
    // Repartition PageViews by userId.
    MessageStream<KV<String, PageView>> partitionedPageViews = 
        pageViews.partitionBy(
            pageView -> pageView.getUserId(), // key extractor
            pageView -> pageView, // value extractor
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class)), // serdes
            "partitioned-page-views"); // operator ID    
        
{% endhighlight %}

The operator ID should be unique for each operator within the application and is used to identify the streams and stores created by the operator.

#### Merge
Merges the MessageStream with all the provided MessageStreams and returns the merged stream.
{% highlight java %}
    
    MessageStream<LogEvent> log1 = ...
    MessageStream<LogEvent> log2 = ...
    
    // Merge individual “LogEvent” streams and create a new merged MessageStream
    MessageStream<LogEvent> mergedLogs = log1.merge(log2);
    
    // Alternatively, use mergeAll to merge multiple streams
    MessageStream<LogEvent> mergedLogs = MessageStream.mergeAll(ImmutableList.of(log1, log2, ...));
    
{% endhighlight %}

The merge transform preserves the order of messages within each MessageStream. If message <code>m1</code> appears before <code>m2</code> in any provided stream, then, <code>m1</code> will also appears before <code>m2</code> in the merged stream.

#### Broadcast
Broadcasts the contents of the MessageStream to every *instance* of downstream operators via an intermediate stream.

{% highlight java %}

    MessageStream<VersionChange> versionChanges = ...
    
    // Broadcast version change event to all downstream operator instances.
    versionChanges
        .broadcast(
            new JsonSerdeV2<>(VersionChange.class), // serde
            "version-change-broadcast"); // operator ID
        .map(vce -> /* act on version change event in each instance */ );
         
{% endhighlight %}

#### SendTo (Stream)
Sends all messages in this MessageStream to the provided OutputStream. You can specify the key and the value to be used for the outgoing messages.

{% highlight java %}
    
    // Obtain the OutputStream using an OutputDescriptor
    KafkaOutputDescriptor<KV<String, String>> kod = 
        ksd.getOutputDescriptor(“user-country”, KVSerde.of(new StringSerde(), new StringSerde());
    OutputStream<KV<String, String>> userCountries = appDescriptor.getOutputStream(od)
    
    MessageStream<PageView> pageViews = ...
    // Send a new message with userId as the key and their country as the value to the “user-country” stream.
    pageViews
      .map(pageView -> KV.of(pageView.getUserId(), pageView.getCountry()));
      .sendTo(userCountries);

{% endhighlight %}

#### SendTo (Table)
Sends all messages in this MessageStream to the provided Table. The expected message type is [KV](javadocs/org/apache/samza/operators/KV).

{% highlight java %}

    MessageStream<Profile> profilesStream = ...
    Table<KV<Long, Profile>> profilesTable = 
    
    profilesStream
        .map(profile -> KV.of(profile.getMemberId(), profile))
        .sendTo(profilesTable);
        
{% endhighlight %}

#### Sink
Allows sending messages from this MessageStream to an output system using the provided [SinkFunction](javadocs/org/apache/samza/operators/functions/SinkFunction.html).

This offers more control than [SendTo (Stream)](#sendto-stream) since the SinkFunction has access to the MessageCollector and the TaskCoordinator. For example, you can choose to manually commit offsets, or shut-down the job using the TaskCoordinator APIs. This operator can also be used to send messages to non-Samza systems (e.g. a remote databases, REST services, etc.)

{% highlight java %}
    
    MessageStream<PageView> pageViews = ...
    
    pageViews.sink((msg, collector, coordinator) -> {
        // Construct a new outgoing message, and send it to a kafka topic named TransformedPageViewEvent.
        collector.send(new OutgoingMessageEnvelope(new SystemStream(“kafka”, “TransformedPageViewEvent”), msg));
    } );
        
{% endhighlight %}

#### Join (Stream-Stream)
The Stream-Stream Join operator joins messages from two MessageStreams using the provided pairwise [JoinFunction](javadocs/org/apache/samza/operators/functions/JoinFunction.html). Messages are joined when the key extracted from a message from the first stream matches the key extracted from a message in the second stream. Messages in each stream are retained for the provided ttl duration and join results are emitted as matches are found. Join only retains the latest message for each input stream.

{% highlight java %}
    
    // Joins a stream of OrderRecord with a stream of ShipmentRecord by orderId with a TTL of 20 minutes.
    // Results are produced to a new stream of FulfilledOrderRecord.
    MessageStream<OrderRecord> orders = …
    MessageStream<ShipmentRecord> shipments = …

    MessageStream<FulfilledOrderRecord> shippedOrders = orders.join(
        shipments, // other stream
        new OrderShipmentJoiner(), // join function
        new StringSerde(), // serde for the join key
        new JsonSerdeV2<>(OrderRecord.class), new JsonSerdeV2<>(ShipmentRecord.class), // serde for both streams
        Duration.ofMinutes(20), // join TTL
        "shipped-order-stream") // operator ID

    // Constructs a new FulfilledOrderRecord by extracting the order timestamp from the OrderRecord and the shipment timestamp from the ShipmentRecord.
    class OrderShipmentJoiner implements JoinFunction<String, OrderRecord, ShipmentRecord, FulfilledOrderRecord> {
      @Override
      public FulfilledOrderRecord apply(OrderRecord message, ShipmentRecord otherMessage) {
        return new FulfilledOrderRecord(message.orderId, message.orderTimestamp, otherMessage.shipTimestamp);
      }

      @Override
      public String getFirstKey(OrderRecord message) {
        return message.orderId;
      }

      @Override
      public String getSecondKey(ShipmentRecord message) {
        return message.orderId;
      }
    }
    
{% endhighlight %}

#### Join (Stream-Table)
The Stream-Table Join operator joins messages from a MessageStream with messages in a Table using the provided [StreamTableJoinFunction](javadocs/org/apache/samza/operators/functions/StreamTableJoinFunction.html). Messages are joined when the key extracted from a message in the stream matches the key for a record in the table. The join function is invoked with both the message and the record. If a record is not found in the table, a null value is provided. The join function can choose to return null for an inner join, or an output message for a left outer join. For join correctness, it is important to ensure the input stream and table are partitioned using the same key (e.g., using the partitionBy operator) as this impacts the physical placement of data.

{% highlight java %}

    pageViews
        .partitionBy(pv -> pv.getMemberId, pv -> pv, "page-views-by-memberid")
        .join(profiles, new PageViewToProfileTableJoiner())
        ...
    
    public class PageViewToProfileTableJoiner implements 
        StreamTableJoinFunction<Integer, KV<Integer, PageView>, KV<Integer, Profile>, EnrichedPageView> {
      
      @Override
      public EnrichedPageView apply(KV<Integer, PageView> m, KV<Integer, Profile> r) {
        return r != null ? new EnrichedPageView(...) : null;
      }
       
      @Override
      public Integer getMessageKey(KV<Integer, PageView> message) {
        return message.getKey();
      }

      @Override
      public Integer getRecordKey(KV<Integer, Profile> record) {
        return record.getKey();
      }
    }
    
{% endhighlight %}

### Window
#### Windowing Concepts
**Windows, Triggers, and WindowPanes**: The window operator groups incoming messages in the MessageStream into finite windows. Each emitted result contains one or more messages in the window and is called a WindowPane.

A window can have one or more associated triggers which determine when results from the window are emitted. Triggers can be either [early triggers](javadocs/org/apache/samza/operators/windows/Window.html#setEarlyTrigger-org.apache.samza.operators.triggers.Trigger-) that allow emitting results speculatively before all data for the window has arrived, or late triggers that allow handling late messages for the window.

**Aggregator Function**: By default, the emitted WindowPane will contain all the messages for the window. Instead of retaining all messages, you typically define a more compact data structure for the WindowPane and update it incrementally as new messages arrive, e.g. for keeping a count of messages in the window. To do this, you can provide an aggregating [FoldLeftFunction](javadocs/org/apache/samza/operators/functions/FoldLeftFunction.html) which is invoked for each incoming message added to the window and defines how to update the WindowPane for that message.

**Accumulation Mode**: A window’s accumulation mode determines how results emitted from a window relate to previously emitted results for the same window. This is particularly useful when the window is configured with early or late triggers. The accumulation mode can either be discarding or accumulating.

A discarding window clears all state for the window at every emission. Each emission will only correspond to new messages that arrived since the previous emission for the window.

An accumulating window retains window results from previous emissions. Each emission will contain all messages that arrived since the beginning of the window.

#### Window Types
The Samza High Level Streams API currently supports tumbling and session windows.

**Tumbling Window**: A tumbling window defines a series of contiguous, fixed size time intervals in the stream.

Examples:

{% highlight java %}
    
    // Group the pageView stream into 30 second tumbling windows keyed by the userId.
    MessageStream<PageView> pageViews = ...
    MessageStream<WindowPane<String, Collection<PageView>>> = pageViews.window(
        Windows.keyedTumblingWindow(
            pageView -> pageView.getUserId(), // key extractor
            Duration.ofSeconds(30), // window duration
            new StringSerde(), new JsonSerdeV2<>(PageView.class)));

    // Compute the maximum value over tumbling windows of 30 seconds.
    MessageStream<Integer> integers = …
    Supplier<Integer> initialValue = () -> Integer.MIN_VALUE;
    FoldLeftFunction<Integer, Integer> aggregateFunction = 
        (msg, oldValue) -> Math.max(msg, oldValue);
    
    MessageStream<WindowPane<Void, Integer>> windowedStream = integers.window(
       Windows.tumblingWindow(
            Duration.ofSeconds(30), 
            initialValue, 
            aggregateFunction, 
            new IntegerSerde()));
   
{% endhighlight %}

**Session Window**: A session window groups a MessageStream into sessions. A session captures a period of activity over a MessageStream and is defined by a gap. A session is closed and results are emitted if no new messages arrive for the window for the gap duration.

Examples:

{% highlight java %}

    // Sessionize a stream of page views, and count the number of page-views in a session for every user.
    MessageStream<PageView> pageViews = …
    Supplier<Integer> initialValue = () -> 0
    FoldLeftFunction<PageView, Integer> countAggregator = (pageView, oldCount) -> oldCount + 1;
    Duration sessionGap = Duration.ofMinutes(3);
    
    MessageStream<WindowPane<String, Integer> sessionCounts = pageViews.window(
        Windows.keyedSessionWindow(
            pageView -> pageView.getUserId(), 
            sessionGap, 
            initialValue, 
            countAggregator,
            new StringSerde(), new IntegerSerde()));

    // Compute the maximum value over tumbling windows of 30 seconds.
    MessageStream<Integer> integers = …
    Supplier<Integer> initialValue = () -> Integer.MAX_INT
    FoldLeftFunction<Integer, Integer> aggregateFunction = (msg, oldValue) -> Math.max(msg, oldValue)
    
    MessageStream<WindowPane<Void, Integer>> windowedStream = integers.window(
        Windows.tumblingWindow(
            Duration.ofSeconds(30), 
            initialValue, 
            aggregateFunction,
            new IntegerSerde()));
         
{% endhighlight %}

### Operator IDs
Each operator in the StreamApplication is associated with a globally unique identifier. By default, each operator is assigned an ID by the framework based on its position in the operator DAG for the application. Some operators that create and use external resources require you to provide an explicit ID for them. Examples of such operators are partitionBy and broadcast with their intermediate streams, and window and join with their local stores and changelogs. It's strongly recommended to provide meaningful IDs for such operators. 

These IDs help you manage the underlying resources when you make changes to the application logic that change the position of the operator within the DAG and:

1. You wish to retain the previous state for the operator, since the changes to the DAG don't affect the operator semantics. For example, you added a map operator before a partitionBy operator to log the incoming message. In this case, you can retain previous the operator ID.

2. You wish to discard the previous state for the operator, since the changes to the DAG change the operator semantics. For example, you added a filter operator before a partitionBy operator that discards some of the messages. In this case, you should change the operator ID. Note that by doing so you will lose any previously checkpointed messages that haven't been completely processed by the downstream operators yet.

An operator ID is of the format: **jobName-jobId-opCode-opId**

- **jobName** is the name of your application, as specified using the configuration "app.name"
- **jobId** is the id of your application, as specified using the configuration "app.id"
- **opCode** is a pre-defined identifier for the type of the operator, e.g. map/filter/join
- **opId** is either auto-generated by the framework based on the position of the operator within the DAG, or can be provided by you for operators that manage external resources.

### Data Serialization
Producing data to and consuming data from streams and tables require serializing and de-serializing it. In addition, some stateful operators like joins and windows store data locally for durability across restarts. Such operations require you to provide a [Serde](javadocs/org/apache/samza/serializers/Serde) implementation when using them. This also helps Samza infer the type of the data in your application, thus allowing the operator transforms to be checked for type safety at compile time. Samza provides the following Serde implementations that you can use out of the box:

- Common Types: Serdes for common Java data types, such as ByteBuffer, Double, Long, Integer, Byte, String.
- [SerializableSerde](javadocs/org/apache/samza/serializers/SerializableSerde): A Serde for Java classes that implement the java.io.Serializable interface. 
- [JsonSerdeV2](javadocs/org/apache/samza/serializers/JsonSerdeV2): a Jackson based type safe JSON Serde that allows serializing from and deserializing to a POJO.
- [KVSerde](javadocs/org/apache/samza/serializers/KVSerde): A pair of Serdes, first for the keys, and the second for the values in the incoming/outgoing message, a table record, or a [KV](javadocs/org/apache/samza/operators/KV) object.
- [NoOpSerde](javadocs/org/apache/samza/serializers/NoOpSerde): A marker serde that indicates that the framework should not attempt any serialization/deserialization of the data. This is useful in some cases where the SystemProducer or SystemConsumer handles serialization and deserialization of the data itself.

### Application Serialization
Samza uses Java Serialization to distribute an application's processing logic to the processors. For this to work, all application logic, including any Function implementations passed to operators, needs to be serializable. If you need to use any non-serializable objects at runtime, you can use the [ApplicationContainerContext](javadocs/org/apache/samza/context/ApplicationContainerContext) and [ApplicationTaskContext](javadocs/org/apache/samza/context/ApplicationContainerContext) APIs to manage their lifecycle.