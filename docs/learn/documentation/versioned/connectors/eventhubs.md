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

## Overview

The Samza Event Hubs connector provides access to [Azure Event Hubs](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features), Microsoft’s data streaming service on Azure. An event hub is similar to a Kafka topic and can have multiple partitions with producers and consumers. Each message produced or consumed from an event hub is an instance of [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data). 

You may find an [example](../../../tutorials/versioned/samza-event-hubs-standalone.md) using this connector in the [hello-samza](https://github.com/apache/samza-hello-samza) project.

## Consuming from Event Hubs

Samza’s [EventHubSystemConsumer](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubSystemConsumer.java) wraps the EventData into an [EventHubIncomingMessageEnvelope](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubIncomingMessageEnvelope.java). Samza's Event Hubs consumer wraps each message from Event Hubs into an EventHubMessageEnvelope. The envelope has two fields of interest - the key, which is set to the event's String partition key and the message, which is set to the actual data in the event.

You can describe your Samza jobs to process data from Azure Event Hubs. To set Samza to consume from Event Hubs streams:

{% highlight java %}
 1  public void describe(StreamApplicationDescriptor appDescriptor) {
 2  // Define your system here
 3  EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");
 4  
 5  // Choose your serializer/deserializer for the EventData payload
 6  StringSerde serde = new StringSerde();
 7  
 8  // Define the input descriptors with respective descriptors
 9  EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
10    systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
11        .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
12        .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);
13  
14  // Define the input streams with descriptors
15  MessageStream<KV<String, String>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
16  
17  //...
18  }
{% endhighlight %}

In the code snippet above, we create the input and output streams that can consume and produce from the configured Event Hubs entities.

1. Line 3: A `EventHubsSystemDescriptor` is created with the name "eventhubs". You may set different system descriptors here. 
2. Line 6: Event Hubs messages are consumed as key value pairs. The [serde](../../documentation/versioned/container/serialization.html) is defined for the value of the incoming payload of the Event Hubs' EventData. You may use any of the serdes that samza ships with out of the box or define your own.
The serde for the key is not set since it will always the String from the EventData [partitionKey](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data._system_properties.getpartitionkey?view=azure-java-stable#com_microsoft_azure_eventhubs__event_data__system_properties_getPartitionKey__).
3. Line 8-12: An `EventHubsInputDescriptor` is created with the required descriptors to gain access of the Event Hubs entity (`STREAM_ID`, `EVENTHUBS_NAMESPACE`, `EVENTHUBS_ENTITY`, `EVENTHUBS_SAS_KEY_NAME`, `EVENTHUBS_SAS_KEY_TOKEN`).
These must be set to the credentials of the entities you wish to connect to.
4. Line 15: creates an `InputStream` with the previously defined `EventHubsInputDescriptor`.

Alternatively, you can set these properties in the `.properties` file of the application.
Note: the keys set in the `.properties` file will override the ones set in code with descriptors.
Refer to the [Event Hubs configuration reference](../../documentation/versioned/jobs/samza-configurations.html#eventhubs) for the complete list of configurations.

{% highlight jproperties %}
# define an event hub system factory with your identifier. eg: eh-system
systems.eh-system.samza.factory=org.apache.samza.system.eventhub.EventHubSystemFactory

# define your streams
systems.eh-system.stream.list=eh-input-stream
streams.eh-stream.samza.system=eh-system

# define required properties for your streams
streams.eh-input-stream.eventhubs.namespace=YOUR-STREAM-NAMESPACE
streams.eh-input-stream.eventhubs.entitypath=YOUR-ENTITY-NAME
streams.eh-input-stream.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
streams.eh-input-stream.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN
{% endhighlight %}

It is required to provide values for YOUR-STREAM-NAMESPACE, YOUR-ENTITY-NAME, YOUR-SAS-KEY-NAME, YOUR-SAS-KEY-TOKEN to read or write to the stream.

## Producing to Event Hubs

Each [OutgoingMessageEnvelope](https://samza.apache.org/learn/documentation/latest/api/javadocs/org/apache/samza/system/OutgoingMessageEnvelope.html) from Samza is converted into an [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data) instance whose body is set to the message in the envelope. Additionally, the key and the produce timestamp are set as properties in the EventData before sending it to Event Hubs.
Similarly, you can also configure your Samza job to write to Event Hubs. Follow the same descriptors defined in the Consuming from Event Hubs section to write to Event Hubs:

{% highlight java %}
 1  public void describe(StreamApplicationDescriptor appDescriptor) {
 2  // Define your system here
 3  EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");
 4  
 5  // Choose your serializer/deserializer for the EventData payload
 6  StringSerde serde = new StringSerde();
 7  
 8  // Define the input and output descriptors with respective descriptors
 9  EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
10    systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, serde)
11        .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
12        .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);
13  
14  // Define the output streams with descriptors
15  OutputStream<KV<String, String>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);
16  
17  //...
18  }
{% endhighlight %}

In the code snippet above, we create the input and output streams that can consume and produce from the configured Event Hubs entities.

1. Line 3: A `EventHubsSystemDescriptor` is created with the name "eventhubs". You may set different system descriptors here. 
2. Line 6: Event Hubs messages are produced as key value pairs. The [serde](../../documentation/versioned/container/serialization.html) is defined for the value of the payload of the outgoing Event Hubs' EventData. You may use any of the serdes that samza ships with out of the box or define your own.
The serde for the key is not set since it will always the String from the EventData [partitionKey](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data._system_properties.getpartitionkey?view=azure-java-stable#com_microsoft_azure_eventhubs__event_data__system_properties_getPartitionKey__).
3. Line 9-12: An `EventHubsOutputDescriptor` is created with the required descriptors to gain access of the Event Hubs entity (`STREAM_ID`, `EVENTHUBS_NAMESPACE`, `EVENTHUBS_ENTITY`, `EVENTHUBS_SAS_KEY_NAME`, `EVENTHUBS_SAS_KEY_TOKEN`).
These must be set to the credentials of the entities you wish to connect to.
4. Line 15: creates an `OutputStream` with the previously defined `EventHubsOutputDescriptor`.

Alternatively, you can set these properties in the `.properties` file of the application.
Note: the keys set in the `.properties` file will override the ones set in code with descriptors.
Refer to the [Event Hubs configuration reference](../../documentation/versioned/jobs/samza-configurations.html#eventhubs) for the complete list of configurations.

{% highlight jproperties %}
# define an event hub system factory with your identifier. eg: eh-system
systems.eh-system.samza.factory=org.apache.samza.system.eventhub.EventHubSystemFactory

# define your streams
systems.eh-system.stream.list=eh-output-stream
streams.eh-stream.samza.system=eh-system

streams.eh-output-stream.eventhubs.namespace=YOUR-STREAM-NAMESPACE
streams.eh-output-stream.eventhubs.entitypath=YOUR-ENTITY-NAME
streams.eh-output-stream.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
streams.eh-output-stream.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN
{% endhighlight %}

Then you can consume and produce a message to Event Hubs in your code as below:

{% highlight java %}
MessageStream<KV<String, String>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
OutputStream<KV<String, String>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);
eventhubInput.sendTo(eventhubOutput)
{% endhighlight %}

## Advanced configuration

###Producer partitioning

The partition.method property determines how outgoing messages are partitioned. Valid values for this config are EVENT\_HUB\_HASHING, PARTITION\_KEY\_AS_PARTITION or ROUND\_ROBIN.

1. EVENT\_HUB\_HASHING: By default, Samza computes the partition for an outgoing message based on the hash of its partition-key. This ensures that events with the same key are sent to the same partition. If this option is chosen, the partition key should be a string. If the partition key is not set, the key in the message is used for partitioning.

2. PARTITION\_KEY\_AS\_PARTITION: In this method, each message is sent to the partition specified by its partition key. This requires the partition key to be an integer. If the key is greater than the number of partitions, a modulo operation will be performed on the key. Similar to EVENT\_HUB\_HASHING, the key in the message is used if the partition key is not specified.

3. ROUND\_ROBIN: In this method, outgoing messages are distributed in a round-robin across all partitions. The key and the partition key in the message are ignored.

##### Using descriptors
{% highlight java %}
EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs")
        .withPartitioningMethod(PartitioningMethod.EVENT_HUB_HASHING);
{% endhighlight %}

##### Using config properties
{% highlight jproperties %}
systems.eh-system.partition.method = EVENT_HUB_HASHING
{% endhighlight %}

### Consumer groups

Event Hubs supports the notion of [consumer groups](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#consumer-groups) which enable multiple applications to have their own view of the event stream. Each partition is exclusively consumed by one consumer in the consumer group. Each event hub stream has a pre-defined consumer group named $Default. You can define your own consumer group for your job by configuring an Event Hubs.consumer.group

##### Using descriptors
{% highlight java %}
EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");
EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
        systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
            .withConsumerGroup("my-group");
{% endhighlight %}

##### Using config properties
{% highlight jproperties %}
streams.eh-input-stream.eventhubs.consumer.group = my-group
{% endhighlight %}

### Serde

By default, the messages from Event Hubs are sent and received as byte arrays. You can configure a serializer and deserializer for your message by setting a value for msg.serde for your stream.

##### Using descriptors
{% highlight java %}
JsonSerde inputSerde = new JsonSerde();
EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
    systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, inputSerde);
JsonSerde outputSerde = new JsonSerde();
EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
     systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, outputSerde);
{% endhighlight %}

##### Using config properties
{% highlight jproperties %}
streams.input0.samza.msg.serde = json
streams.output0.samza.msg.serde = json
{% endhighlight %}

### Consumer buffer size

When the consumer reads a message from event hubs, it appends them to a shared producer-consumer queue corresponding to its partition. This config determines the per-partition queue size. Setting a higher value for this config typically achieves a higher throughput at the expense of increased on-heap memory.
##### Using descriptors
{% highlight java %}
 EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs")
        .withReceiveQueueSize(10);
{% endhighlight %}

##### Using config properties
{% highlight jproperties %}
systems.eh-system.eventhubs.receive.queue.size = 10
{% endhighlight %}