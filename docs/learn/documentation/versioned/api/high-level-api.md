---
layout: page
title: High-level API
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

The high level API provides the libraries to define your application logic. The StreamApplication is the central abstraction which your application must implement. You start by declaring your inputs as instances of MessageStream. Then you can apply operators on each MessageStream like map, filter, window, and join to define the whole end-to-end data processing in a single program.

Since the 0.13.0 release, Samza provides a new high level API that simplifies your applications. This API supports operations like re-partitioning, windowing, and joining on streams. You can now express your application logic concisely in few lines of code and accomplish what previously required multiple jobs.
# Code Examples

Check out some examples to see the high-level API in action.
1. PageView AdClick Joiner demonstrates joining a stream of PageViews with a stream of AdClicks, e.g. to analyze which pages get the most ad clicks.
2. PageView Repartitioner illustrates re-partitioning the incoming stream of PageViews.
3. PageView Sessionizer groups the incoming stream of events into sessions based on user activity.
4. PageView by Region counts the number of views per-region over tumbling time intervals.

# Key Concepts
## StreamApplication
When writing your stream processing application using the Samza high-level API, you implement a StreamApplication and define your processing logic in the describe method.

{% highlight java %}

    public void describe(StreamApplicationDescriptor appDesc) { … }

{% endhighlight %}

For example, here is a StreamApplication that validates and decorates page views with viewer’s profile information.

{% highlight java %}

    public class BadPageViewFilter implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafka = new KafkaSystemDescriptor();
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> outputPageViews = kafka.getOutputDescriptor( “decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));    
        MessageStream<PageView> pageViews = appDesc.getInputStream(pageViewInput);
        pageViews.filter(this::isValidPageView)
            .map(this::addProfileInformation)
            .sendTo(appDesc.getOutputStream(outputPageViews));
      }
    }
    
{% endhighlight %}

## MessageStream
A MessageStream, as the name implies, represents a stream of messages. A StreamApplication is described as a series of transformations on MessageStreams. You can get a MessageStream in two ways:
1. Using StreamApplicationDescriptor.getInputStream to get the MessageStream for a given input stream (e.g., a Kafka topic).
2. By transforming an existing MessageStream using operations like map, filter, window, join etc.
## Table
A Table represents a dataset that can be accessed by keys, and is one of the building blocks of the Samza high level API; the main motivation behind it is to support stream-table joins. The current K/V store is leveraged to provide backing store for local tables. More variations such as direct access and composite tables will be supported in the future. The usage of a table typically follows three steps:
1. Create a table
2. Populate the table using the sendTo() operator
3. Join a stream with the table using the join() operator

{% highlight java %}

    final StreamApplication app = (streamAppDesc) -> {
      Table<KV<Integer, Profile>> table = streamAppDesc.getTable(new InMemoryTableDescriptor("t1")
          .withSerde(KVSerde.of(new IntegerSerde(), new ProfileJsonSerde())));
      ...
    };

{% endhighlight %}

Example above creates a TableDescriptor object, which contains all information about a table. The currently supported table types are InMemoryTableDescriptor and RocksDbTableDescriptor. Notice the type of records in a table is KV, and Serdes for both key and value of records needs to be defined (line 4). Additional parameters can be added based on individual table types.

More details about step 2 and 3 can be found at [operator section](#operators).

# Anatomy of a typical StreamApplication
There are 3 simple steps to write your stream processing logic using the Samza high-level API.
## Step 1: Obtain the input streams
You can obtain the MessageStream for your input stream ID (“page-views”) using StreamApplicationDescriptor.getInputStream.

{% highlight java %}

    KafkaSystemDescriptor sd = new KafkaSystemDescriptor("kafka")
        .withConsumerZkConnect(ImmutableList.of("localhost:2181"))
        .withProducerBootstrapServers(ImmutableList.of("localhost:9092"));

    KafkaInputDescriptor<KV<String, Integer>> pageViewInput =
        sd.getInputDescriptor("page-views", KVSerde.of(new StringSerde(), new JsonSerdeV2(PageView.class)));
    
    MessageStream<PageView> pageViews = streamAppDesc.getInputStream(pageViewInput);

{% endhighlight %}

The parameter {% highlight java %}pageViewInput{% endhighlight %} is the [InputDescriptor](javadocs/org/apache/samza/system/descriptors/InputDescriptor.html). Each InputDescriptor includes the full information of an input stream, including the stream ID, the serde to deserialize the input messages, and the system. By default, Samza uses the stream ID as the physical stream name and accesses the stream on the default system which is specified with the property “job.default.system”. However, the physical name and system properties can be overridden in configuration. For example, the following configuration defines the stream ID “page-views” as an alias for the PageViewEvent topic in a local Kafka cluster.

{% highlight jproperties %}

    streams.page-views.samza.physical.name=PageViewEvent

{% endhighlight %}

## Step 2: Define your transformation logic
You are now ready to define your StreamApplication logic as a series of transformations on MessageStreams.

{% highlight java %}

    MessageStream<DecoratedPageViews> decoratedPageViews = pageViews.filter(this::isValidPageView)
        .map(this::addProfileInformation);

{% endhighlight %}

## Step 3: Write to output streams

Finally, you can create an OutputStream using StreamApplicationDescriptor.getOutputStream and send the transformed messages through it.

{% highlight java %}

    KafkaOutputDescriptor<DecoratedPageViews> outputPageViews =
        sd.getInputDescriptor("page-views", new JsonSerdeV2(DecoratedPageViews.class));
  
    // Send messages with userId as the key to “decorated-page-views”.
    decoratedPageViews.sendTo(streamAppDesc.getOutputStream(outputPageViews));

{% endhighlight %}

The parameter {% highlight java %}outputPageViews{% endhighlight %} is the [OutputDescriptor](javadocs/org/apache/samza/system/descriptors/OutputDescriptor.html), which includes the stream ID, the serde to serialize the outgoing messages, the physical name and the system. Similarly, the properties for this stream can be overridden just like the stream IDs for input streams. For example:

{% highlight jproperties %}

    streams.decorated-page-views.samza.physical.name=DecoratedPageViewEvent

{% endhighlight %}

# Operators
The high level API supports common operators like map, flatmap, filter, merge, joins, and windowing on streams. Most of these operators accept corresponding Functions, which are Initable and Closable.
## Map
Applies the provided 1:1 MapFunction to each element in the MessageStream and returns the transformed MessageStream. The MapFunction takes in a single message and returns a single message (potentially of a different type).

{% highlight java %}
    
    MessageStream<Integer> numbers = ...
    MessageStream<Integer> tripled = numbers.map(m -> m * 3);
    MessageStream<String> stringified = numbers.map(m -> String.valueOf(m));

{% endhighlight %}
## FlatMap
Applies the provided 1:n FlatMapFunction to each element in the MessageStream and returns the transformed MessageStream. The FlatMapFunction takes in a single message and returns zero or more messages.

{% highlight java %}
    
    MessageStream<String> sentence = ...
    // Parse the sentence into its individual words splitting by space
    MessageStream<String> words = sentence.flatMap(sentence ->
        Arrays.asList(sentence.split(“ ”))

{% endhighlight %}
## Filter
Applies the provided FilterFunction to the MessageStream and returns the filtered MessageStream. The FilterFunction is a predicate that specifies whether a message should be retained in the filtered stream. Messages for which the FilterFunction returns false are filtered out.

{% highlight java %}
    
    MessageStream<String> words = ...
    // Extract only the long words
    MessageStream<String> longWords = words.filter(word -> word.size() > 15);
    // Extract only the short words
    MessageStream<String> shortWords = words.filter(word -> word.size() < 3);
    
{% endhighlight %}
## PartitionBy
Re-partitions this MessageStream using the key returned by the provided keyExtractor and returns the transformed MessageStream. Messages are sent through an intermediate stream during repartitioning.
{% highlight java %}
    
    MessageStream<PageView> pageViews = ...
    // Repartition pageView by userId.
    MessageStream<KV<String, PageView>> partitionedPageViews = pageViews.partitionBy(
        pageView -> pageView.getUserId(), // key extractor
        pageView -> pageView, // value extractor
        KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class)), // serdes
        "partitioned-page-views"); // operator ID    
        
{% endhighlight %}

The operator ID should be unique for an operator within the application and is used to identify the streams and stores created by the operator.

## Merge
Merges the MessageStream with all the provided MessageStreams and returns the merged stream.
{% highlight java %}
    
    MessageStream<ServiceCall> serviceCall1 = ...
    MessageStream<ServiceCall> serviceCall2 = ...
    // Merge individual “ServiceCall” streams and create a new merged MessageStream
    MessageStream<ServiceCall> serviceCallMerged = serviceCall1.merge(serviceCall2);
    
{% endhighlight %}

The merge transform preserves the order of each MessageStream, so if message {% highlight java %}m1{% endhighlight %} appears before {% highlight java %}m2{% endhighlight %} in any provided stream, then, {% highlight java %}m1{% endhighlight %} also appears before {% highlight java %}m2{% endhighlight %} in the merged stream.
As an alternative to the merge instance method, you also can use the [MessageStream#mergeAll](javadocs/org/apache/samza/operators/MessageStream.html#mergeAll-java.util.Collection-) static method to merge MessageStreams without operating on an initial stream.

## Broadcast
Broadcasts the MessageStream to all instances of down-stream transformation operators via the intermediate stream.
{% highlight java %}

    MessageStream<VersionChange> verChanges = ...
    // Broadcast input data version change event to all operator instances.
    MessageStream<VersionChange> broadcastVersionChanges = 
        verChanges.broadcast(new JsonSerdeV2<>(VersionChange.class), // serde
                             "broadcast-version-changes"); // operator ID
{% endhighlight %}

## SendTo(Stream)
Sends all messages from this MessageStream to the provided OutputStream. You can specify the key and the value to be used for the outgoing message.

{% highlight java %}
    
    // Output a new message with userId as the key and region as the value to the “user-region” stream.
    OutputDescriptor<KV<String, String>> outputRegions = 
        kafka.getOutputDescriptor(“user-region”, KVSerde.of(new StringSerde(), new StringSerde());
    MessageStream<PageView> pageViews = ...
    MessageStream<KV<String, PageView>> keyedPageViews = pageViews.map(KV.of(pageView.getUserId(), pageView.getRegion()));
    keyedPageViews.sendTo(appDesc.getOutputStream(outputRegions));

{% endhighlight %}
## SendTo(Table)
Sends all messages from this MessageStream to the provided table, the expected message type is KV.

{% highlight java %}
    
    // Write a new message with memberId as the key and profile as the value to a table.
    appDesc.getInputStream(kafka.getInputDescriptor("Profile", new NoOpSerde<Profile>()))
        .map(m -> KV.of(m.getMemberId(), m))
        .sendTo(table);
        
{% endhighlight %}

## Sink
Allows sending messages from this MessageStream to an output system using the provided [SinkFunction](javadocs/org/apache/samza/operators/functions/SinkFunction.html).

This offers more control than {% highlight java %}sendTo{% endhighlight %} since the SinkFunction has access to the MessageCollector and the TaskCoordinator. For instance, you can choose to manually commit offsets, or shut-down the job using the TaskCoordinator APIs. This operator can also be used to send messages to non-Samza systems (e.g. remote databases, REST services, etc.)

{% highlight java %}
    
    // Repartition pageView by userId.
    MessageStream<PageView> pageViews = ...
    pageViews.sink( (msg, collector, coordinator) -> {
        // Construct a new outgoing message, and send it to a kafka topic named TransformedPageViewEvent.
        collector.send(new OutgoingMessageEnvelope(new SystemStream(“kafka”,
                       “TransformedPageViewEvent”), msg));
    } );
        
{% endhighlight %}

## Join(Stream-Stream)
The stream-stream Join operator joins messages from two MessageStreams using the provided pairwise [JoinFunction](javadocs/org/apache/samza/operators/functions/JoinFunction.html). Messages are joined when the keys extracted from messages from the first stream match keys extracted from messages in the second stream. Messages in each stream are retained for the provided ttl duration and join results are emitted as matches are found.

{% highlight java %}
    
    // Joins a stream of OrderRecord with a stream of ShipmentRecord by orderId with a TTL of 20 minutes.
    // Results are produced to a new stream of FulfilledOrderRecord.
    MessageStream<OrderRecord> orders = …
    MessageStream<ShipmentRecord> shipments = …

    MessageStream<FulfilledOrderRecord> shippedOrders = orders.join(shipments, new OrderShipmentJoiner(),
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

## Join(Stream-Table)
The stream-table Join operator joins messages from a MessageStream using the provided [StreamTableJoinFunction](javadocs/org/apache/samza/operators/functions/StreamTableJoinFunction.html). Messages from the input stream are joined with record in table using key extracted from input messages. The join function is invoked with both the message and the record. If a record is not found in the table, a null value is provided; the join function can choose to return null (inner join) or an output message (left outer join). For join to function properly, it is important to ensure the input stream and table are partitioned using the same key as this impacts the physical placement of data.

{% highlight java %}
   
    streamAppDesc.getInputStream(kafk.getInputDescriptor("PageView", new NoOpSerde<PageView>()))
        .partitionBy(PageView::getMemberId, v -> v, "p1")
        .join(table, new PageViewToProfileJoinFunction())
        ...
    
    public class PageViewToProfileJoinFunction implements StreamTableJoinFunction
        <Integer, KV<Integer, PageView>, KV<Integer, Profile>, EnrichedPageView> {
      
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

## Window
### Windowing Concepts
**Windows, Triggers, and WindowPanes**: The window operator groups incoming messages in the MessageStream into finite windows. Each emitted result contains one or more messages in the window and is called a WindowPane.

A window can have one or more associated triggers which determine when results from the window are emitted. Triggers can be either [early triggers](javadocs/org/apache/samza/operators/windows/Window.html#setEarlyTrigger-org.apache.samza.operators.triggers.Trigger-) that allow emitting results speculatively before all data for the window has arrived, or late triggers that allow handling late messages for the window.

**Aggregator Function**: By default, the emitted WindowPane will contain all the messages for the window. Instead of retaining all messages, you typically define a more compact data structure for the WindowPane and update it incrementally as new messages arrive, e.g. for keeping a count of messages in the window. To do this, you can provide an aggregating [FoldLeftFunction](javadocs/org/apache/samza/operators/functions/FoldLeftFunction.html) which is invoked for each incoming message added to the window and defines how to update the WindowPane for that message.

**Accumulation Mode**: A window’s accumulation mode determines how results emitted from a window relate to previously emitted results for the same window. This is particularly useful when the window is configured with early or late triggers. The accumulation mode can either be discarding or accumulating.

A discarding window clears all state for the window at every emission. Each emission will only correspond to new messages that arrived since the previous emission for the window.

An accumulating window retains window results from previous emissions. Each emission will contain all messages that arrived since the beginning of the window.

### Window Types
The Samza high-level API currently supports tumbling and session windows.

**Tumbling Window**: A tumbling window defines a series of contiguous, fixed size time intervals in the stream.

Examples:

{% highlight java %}
    
    // Group the pageView stream into 3 second tumbling windows keyed by the userId.
    MessageStream<PageView> pageViews = ...
    MessageStream<WindowPane<String, Collection<PageView>>> =
        pageViews.window(
            Windows.keyedTumblingWindow(pageView -> pageView.getUserId(), // key extractor
            Duration.ofSeconds(30), // window duration
            new StringSerde(), new JsonSerdeV2<>(PageView.class)));

    // Compute the maximum value over tumbling windows of 3 seconds.
    MessageStream<Integer> integers = …
    Supplier<Integer> initialValue = () -> Integer.MIN_VALUE;
    FoldLeftFunction<Integer, Integer> aggregateFunction = (msg, oldValue) -> Math.max(msg, oldValue);
    MessageStream<WindowPane<Void, Integer>> windowedStream =
        integers.window(Windows.tumblingWindow(Duration.ofSeconds(30), initialValue, aggregateFunction, new IntegerSerde()));
   
{% endhighlight %}

**Session Window**: A session window groups a MessageStream into sessions. A session captures a period of activity over a MessageStream and is defined by a gap. A session is closed and results are emitted if no new messages arrive for the window for the gap duration.

Examples:

{% highlight java %}

    // Sessionize a stream of page views, and count the number of page-views in a session for every user.
    MessageStream<PageView> pageViews = …
    Supplier<Integer> initialValue = () -> 0
    FoldLeftFunction<PageView, Integer> countAggregator = (pageView, oldCount) -> oldCount + 1;
    Duration sessionGap = Duration.ofMinutes(3);
    MessageStream<WindowPane<String, Integer> sessionCounts = pageViews.window(Windows.keyedSessionWindow(
        pageView -> pageView.getUserId(), sessionGap, initialValue, countAggregator,
            new StringSerde(), new IntegerSerde()));

    // Compute the maximum value over tumbling windows of 3 seconds.
    MessageStream<Integer> integers = …
    Supplier<Integer> initialValue = () -> Integer.MAX_INT

    FoldLeftFunction<Integer, Integer> aggregateFunction = (msg, oldValue) -> Math.max(msg, oldValue)
    MessageStream<WindowPane<Void, Integer>> windowedStream =
       integers.window(Windows.tumblingWindow(Duration.ofSeconds(3), initialValue, aggregateFunction,
           new IntegerSerde()));
         
{% endhighlight %}

# Operator IDs
Each operator in your application is associated with a globally unique identifier. By default, each operator is assigned an ID based on its usage in the application. Some operators that create and use external resources (e.g., intermediate streams for partitionBy and broadcast, stores and changelogs for joins and windows, etc.) require you to provide an explicit ID for them. It's highly recommended to provide meaningful IDs for such operators. These IDs help you control the underlying resources when you make changes to the application logic that change the position of the operator within the DAG, and
1. You wish to retain the previous state for the operator, since the changes to the DAG don't affect the operator semantics. For example, you added a map operator before a partitionBy operator to log the incoming message. In this case, you can retain previous the operator ID.
2. You wish to discard the previous state for the operator, since the changes to the DAG change the operator semantics. For example, you added a filter operator before a partitionBy operator that discards some of the messages. In this case, you should change the operator ID. Note that by doing so you will lose any previously checkpointed messages that haven't been completely processed by the downstream operators yet.

An operator ID is of the format: **jobName-jobId-opCode-opId**
- **jobName** is the name of your job, as specified using the configuration "job.name"
- **jobId** is the name of your job, as specified using the configuration "job.id"
- **opCode** is an identifier for the type of the operator, e.g. map/filter/join
- **opId** is either auto-generated by the framework based on the position of the operator within the DAG, or can be provided by you for operators that manage external resources.

# Application Serialization
Samza relies on Java Serialization to distribute your application logic to the processors. For this to work, all of your custom application logic needs to be Serializable. For example, all the Function interfaces implement Serializable, and your implementations need to be serializable as well. It's recommended to use the Context APIs to set up any non-serializable context that your Application needs at Runtime.

# Data Serialization
Producing and consuming from streams and tables require serializing and deserializing data. In addition, some operators like joins and windows store data in a local store for durability across restarts. Such operations require you to provide a Serde implementation when using them. This also helps Samza infer the type of the data in your application, thus allowing the operator transforms to be type safe. Samza provides the following Serde implementations that you can use out of the box:

- KVSerde: A pair of Serdes, first for the keys, and the second for the values in the incoming/outgoing message or a table record.
- NoOpSerde: A serde implementation that indicates that the framework should not attempt any serialization/deserialization of the data. Useful in some cases when the SystemProducer/SystemConsumer handle serialization/deserialization themselves.
- JsonSerdeV2: a type-specific Json serde that allows directly deserializing the Json bytes into to specific POJO type.
- Serdes for primitive types: serdes for primitive types, such as ByteBuffer, Double, Long, Integer, Byte, String, etc.
