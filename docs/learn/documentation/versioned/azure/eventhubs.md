---
layout: page
title: Connecting to Eventhubs
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

You can configure your Samza jobs to process data from [Azure Eventhubs](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features), Microsoft's data streaming service. An `event hub` is similar to a Kafka topic and can have multiple partitions with producers and consumers. Each message produced or consumed from an event hub is an instance of [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data). 

### Consuming from EventHubs:

Samza's [EventHubSystemConsumer](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubSystemConsumer.java) wraps the EventData into an [EventHubIncomingMessageEnvelope](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubIncomingMessageEnvelope.java). The key of the message is set to the partition key of the EventData. The message is obtained from the EventData body. 

To configure Samza to configure from EventHub streams: 

```
# define an event hub system factory with your identifier. eg: eh-system
systems.eh-system.samza.factory=org.apache.samza.system.eventhub.EventHubSystemFactory

# define your streams
systems.eh-system.stream.list=input0, output0

# define required properties for your streams
systems.eh-system.streams.input0.eventhubs.namespace=YOUR-STREAM-NAMESPACE
systems.eh-system.streams.input0.eventhubs.entitypath=YOUR-ENTITY-NAME
systems.eh-system.streams.input0.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
systems.eh-system.streams.input0.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN

systems.eh-system.streams.output0.eventhubs.namespace=YOUR-STREAM-NAMESPACE
systems.eh-system.streams.output0.eventhubs.entitypath=YOUR-ENTITY-NAME
systems.eh-system.streams.output0.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
systems.eh-system.streams.output0.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN
```

The tuple required to access the Eventhubs entity per stream must be provided, namely the fields `YOUR-STREAM-NAMESPACE`, `YOUR-ENTITY-NAME`, `YOUR-SAS-KEY-NAME`, `YOUR-SAS-KEY-TOKEN`.

### Producing to EventHubs:

Similarly, you can also configure your Samza job to write to EventHubs.  
```
OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(new SystemStream("eh-system", "output0"), key, message);
collector.send(envelope);
```

Each [OutgoingMessageEnvelope](https://samza.apache.org/learn/documentation/latest/api/javadocs/org/apache/samza/system/OutgoingMessageEnvelope.html) is converted into an [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data) instance whose body is set to the `message` in the envelope. Additionally, the `key` and the `produce timestamp` are set as properties in the EventData before sending it to EventHubs.

### Advanced configuration:

##### Producer partitioning: 

The `partition.method` property determines how outgoing messages are partitioned. Valid values for this config are `EVENT_HUB_HASHING`, `PARTITION_KEY_AS_PARTITION` or `ROUND_ROBIN`. 

`EVENT_HUB_HASHING`: By default, Samza computes the partition for an outgoing message based on the hash of its partition-key. This ensures that events with the same key are sent to the same partition. If this option is chosen, the partition key should be a string. If the partition key is not set, the key in the message is used for partitioning.

`PARTITION_KEY_AS_PARTITION`: In this method, each message is sent to the partition specified by its partition key. This requires the partition key to be an integer. If the key is greater than the number of partitions, a modulo operation will be performed on the key. Similar to EVENT_HUB_HASHING, the key in the message is used if the partition key is not specified.

`ROUND_ROBIN`: In this method, outgoing messages are distributed in a round-robin across all partitions. The key and the partition key in the message are ignored.

![diagram-medium](/img/{{site.version}}/learn/documentation/azure/eventhub_send_methods.png)

```
systems.eh-system.partition.method = EVENT_HUB_HASHING
```

##### Consumer groups: 

Eventhub supports a notion of [consumer groups](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#consumer-groups) which enable multiple applications have their own view of the event stream. Each event hub stream has a pre-defined consumer group named `$Default`. You can define your own consumer group for your job and configure a `eventhubs.consumer.group`  

```
systems.eh-system.streams.eh-input0.eventhubs.consumer.group = my-group
```

##### Serde: 

By default, the messages from EventHubs are sent and received as byte arrays. You can configure a serializer and deserializer for your message by setting a value for `msg.serde` for your stream. 

```
streams.input0.samza.msg.serde = json
streams.output0.samza.msg.serde = json
```

##### Consumer buffer size: 

When the consumer reads a message from event hubs, it appends them to a shared producer-consumer buffer corresponding to its partition. This config determines the per-partition queue size. Setting a higher value for this config typically achieves a higher throughput at the expense of increased on-heap memory.

```
systems.eh-system.eventhubs.receive.queue.size = 10
```

For the list of all configs, check out the configuration table page [here](../jobs/configuration-table.html)
