---
layout: page
title: Event Hubs Connector
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

### EventHubs I/O: QuickStart

The Samza EventHubs connector provides access to [Azure EventHubs](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features), Microsoft’s data streaming service on Azure. An eventhub is similar to a Kafka topic and can have multiple partitions with producers and consumers. Each message produced or consumed from an event hub is an instance of [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data). 

The [hello-samza](https://github.com/apache/samza-hello-samza) project includes an [example](../../../tutorials/versioned/samza-event-hubs-standalone.md) of reading and writing to EventHubs.

### Concepts

####EventHubsSystemDescriptor

Samza refers to any IO source (eg: Kafka) it interacts with as a _system_, whose properties are set using a corresponding `SystemDescriptor`. The `EventHubsSystemDescriptor` allows you to configure various properties for the `EventHubsClient` used by Samza.
{% highlight java %}
 1  EventHubsSystemDescriptor eventHubsSystemDescriptor = new EventHubsSystemDescriptor("eventhubs").withNumClientThreads(5);
{% endhighlight %}

####EventHubsInputDescriptor

The EventHubsInputDescriptor allows you to specify the properties of each EventHubs stream your application should read from. For each of your input streams, you should create a corresponding instance of EventHubsInputDescriptor by providing a topic-name and a serializer.

{% highlight java %}
    EventHubsInputDescriptor<KV<String, String>> inputDescriptor = 
        systemDescriptor.getInputDescriptor(streamId, "eventhubs-namespace", "eventhubs-name", new StringSerde())
          .withSasKeyName("secretkey")
          .withSasKey("sasToken-123")
          .withConsumerGroup("$notdefault");
{% endhighlight %}

By default, messages are sent and received as byte arrays. Samza then de-serializes them to typed objects using your provided Serde. For example, the above uses a `StringSerde` to de-serialize messages.


####EventHubsOutputDescriptor

Similarly, the `EventHubsOutputDescriptor` allows you to specify the output streams for your application. For each output stream you write to in EventHubs, you should create an instance of `EventHubsOutputDescriptor`.

{% highlight java %}
    EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
        systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, new StringSerde();)
            .withSasKeyName(..)
            .withSasKey(..);
{% endhighlight %}

####Security Model
Each EventHubs stream is scoped to a container called a _namespace_, which uniquely identifies an EventHubs in a region. EventHubs's [security model](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-authentication-and-security-model-overview) is based on Shared Access Signatures(SAS). 
Hence, you should also provide your SAS keys and tokens to access the stream. You can generate your SAS tokens using the 

####Data Model
Each event produced and consumed from an EventHubs stream is an instance of [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data), which wraps a byte-array payload. When producing to EventHubs, Samza serializes your object into an `EventData` payload before sending it over the wire. Likewise, when consuming messages from EventHubs, messages are de-serialized into typed objects using the provided Serde. 

### Configuration

####Producer partitioning

You can use `#withPartitioningMethod` to control how outgoing messages are partitioned. The following partitioning schemes are supported:

1. EVENT\_HUB\_HASHING: By default, Samza computes the partition for an outgoing message based on the hash of its partition-key. This ensures that events with the same key are sent to the same partition. If this option is chosen, the partition key should be a string. If the partition key is not set, the key in the message is used for partitioning.

2. PARTITION\_KEY\_AS\_PARTITION: In this method, each message is sent to the partition specified by its partition key. This requires the partition key to be an integer. If the key is greater than the number of partitions, a modulo operation will be performed on the key. Similar to EVENT\_HUB\_HASHING, the key in the message is used if the partition key is not specified.

3. ROUND\_ROBIN: In this method, outgoing messages are distributed in a round-robin across all partitions. The key and the partition key in the message are ignored.

{% highlight java %}
EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs")
        .withPartitioningMethod(PartitioningMethod.EVENT_HUB_HASHING);
{% endhighlight %}


#### Consumer groups

Event Hubs supports the notion of [consumer groups](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#consumer-groups) which enable multiple applications to have their own view of the event stream. Each partition is exclusively consumed by one consumer in the group. Each event hub stream has a pre-defined consumer group named $Default. You can define your own consumer group for your job using `withConsumerGroup`.

{% highlight java %}
EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");
EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
        systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
            .withConsumerGroup("my-group");
{% endhighlight %}


#### Consumer buffer size

When the consumer reads a message from EventHubs, it appends them to a shared producer-consumer queue corresponding to its partition. This config determines the per-partition queue size. Setting a higher value for this config typically achieves a higher throughput at the expense of increased on-heap memory.

{% highlight java %}
 EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs")
        .withReceiveQueueSize(10);
{% endhighlight %}

### Code walkthrough

In this section, we will walk through a simple pipeline that reads from one EventHubs stream and copies each message to another output stream. 

{% highlight java %}
1    EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs").withNumClientThreads(5);

2    EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
        systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, new StringSerde())
            .withSasKeyName(..)
            .withSasKey(..));

3    EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
        systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, serde)
            .withSasKeyName(..))
            .withSasKey(..));

4    MessageStream<KV<String, String>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
5    OutputStream<KV<String, String>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);

    // Define the execution flow with the High Level Streams API
6    eventhubInput
7        .map((message) -> {
8          System.out.println("Received Key: " + message.getKey());
9          System.out.println("Received Message: " + message.getValue());
10          return message;
11        })
12        .sendTo(eventhubOutput);
{% endhighlight %}

-Line 1 instantiates an `EventHubsSystemDescriptor` configuring an EventHubsClient with 5 threads. To consume from other input sources like Kafka, you can define their corresponding descriptors. 

-Line 2 creates an `EventHubsInputDescriptor` with a String serde for its values. Recall that Samza follows a KV data-model for input messages. In the case of EventHubs, the key is a string which is set to the [partitionKey](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data._system_properties.getpartitionkey?view=azure-java-stable#com_microsoft_azure_eventhubs__event_data__system_properties_getPartitionKey__) in the message. Hence, no separate key serde is required. 

-Line 3 creates an `EventHubsOutputDescriptor` to write to an EventHubs stream with the given credentials.

-Line 4 obtains a `MessageStream` from the input descriptor that you can later chain operations on. 

-Line 5 creates an `OutputStream` with the previously defined `EventHubsOutputDescriptor` that you can send messages to.

-Line 7-12 define a simple pipeline that copies message from one EventHubs stream to another