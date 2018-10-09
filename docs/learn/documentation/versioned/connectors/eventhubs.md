---
layout: page
title: Eventhubs Connector
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

The Samza EventHubs connector provides access to [Azure Eventhubs](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features), Microsoft’s data streaming service on Azure. An event hub is similar to a Kafka topic and can have multiple partitions with producers and consumers. Each message produced or consumed from an event hub is an instance of [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data).

## Consuming from EventHubs

Samza’s [EventHubSystemConsumer](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubSystemConsumer.java) wraps the EventData into an [EventHubIncomingMessageEnvelope](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubIncomingMessageEnvelope.java). Samza's eventhubs consumer wraps each message from Eventhubs into an EventHubMessageEnvelope. The envelope has two fields of interest - the key, which is set to the event's partition key and the message, which is set to the actual data in the event.

You can configure your Samza jobs to process data from Azure Eventhubs. To configure Samza to consume from EventHub streams:

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

## Producing to EventHubs

Similarly, you can also configure your Samza job to write to EventHubs. Follow the same configs defined in the Consuming from EventHubs section to write to EventHubs:

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

Then you can create and produce a message to eventhubs in your code as below:

{% highlight java %}
OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(new SystemStream("eh-system", "output0"), key, message); 
collector.send(envelope);
{% endhighlight %}

Each [OutgoingMessageEnvelope](https://samza.apache.org/learn/documentation/latest/api/javadocs/org/apache/samza/system/OutgoingMessageEnvelope.html) is converted into an [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data) instance whose body is set to the message in the envelope. Additionally, the key and the produce timestamp are set as properties in the EventData before sending it to EventHubs.

## Advanced configuration

###Producer partitioning

The partition.method property determines how outgoing messages are partitioned. Valid values for this config are EVENT\_HUB\_HASHING, PARTITION\_KEY\_AS_PARTITION or ROUND\_ROBIN.

1. EVENT\_HUB\_HASHING: By default, Samza computes the partition for an outgoing message based on the hash of its partition-key. This ensures that events with the same key are sent to the same partition. If this option is chosen, the partition key should be a string. If the partition key is not set, the key in the message is used for partitioning.

2. PARTITION\_KEY\_AS\_PARTITION: In this method, each message is sent to the partition specified by its partition key. This requires the partition key to be an integer. If the key is greater than the number of partitions, a modulo operation will be performed on the key. Similar to EVENT\_HUB\_HASHING, the key in the message is used if the partition key is not specified.

3. ROUND\_ROBIN: In this method, outgoing messages are distributed in a round-robin across all partitions. The key and the partition key in the message are ignored.

{% highlight jproperties %}
systems.eh-system.partition.method = EVENT_HUB_HASHING
{% endhighlight %}

### Consumer groups

Eventhub supports the notion of [consumer groups](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#consumer-groups) which enable multiple applications to have their own view of the event stream. Each partition is exclusively consumed by one consumer in the consumer group. Each event hub stream has a pre-defined consumer group named $Default. You can define your own consumer group for your job by configuring a eventhubs.consumer.group

{% highlight jproperties %}
streams.eh-input-stream.eventhubs.consumer.group = my-group
{% endhighlight %}

### Serde

By default, the messages from EventHubs are sent and received as byte arrays. You can configure a serializer and deserializer for your message by setting a value for msg.serde for your stream.

{% highlight jproperties %}
streams.input0.samza.msg.serde = json
streams.output0.samza.msg.serde = json
{% endhighlight %}

### Consumer buffer size

When the consumer reads a message from event hubs, it appends them to a shared producer-consumer queue corresponding to its partition. This config determines the per-partition queue size. Setting a higher value for this config typically achieves a higher throughput at the expense of increased on-heap memory.

{% highlight jproperties %}
systems.eh-system.eventhubs.receive.queue.size = 10
{% endhighlight %}