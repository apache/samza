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

Samza's EventHubSystemConsumer wraps the EventData into an EventHubIncomingMessageEnvelope. The key of the message is set to the partition key of the EventData. If that is not present, then it will be set to the user defined `key` property map of the EventData. The message is obtained from the EventData body. 

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
The first parameter in the `OutgoingMessageEnvelope` is the name of the system and the output stream whose properties are specified in config. The `key` field in the envelope is used as the key in the `EventData` while the`message` field is used as its body.

The envelopes sent will be converted to an `EventData` that is accepted by EventHub. The envelope `message` will be the body of the `EventData` and the envelope "key" and "produce time stamp" will be included in the properties map as `key` and `produce-timestamp` respectively. Currently there are no ways to pass other metadata in the properties field of `EventData`.

### Advanced configuration:

##### Producer partitioning: 

The `partition.method` property determines how outgoing messages are partitioned. Valid values for this config are `EVENT_HUB_HASHING`, `PARTITION_KEY_AS_PARTITION` or `ROUND_ROBIN`. 

The `EVENT_HUB_HASHING` is the default setting for the producer. This method employs the hashing mechanism in EventHubs to determine, based on the key of the message, which partition the message should go. Using this method still ensures that all the events with the same key are sent to the same partition in the event hub. If this option is chosen, the partition key used for the hash should be a string. If the partition key is not set, the message key is used instead.

The `PARTITION_KEY_AS_PARTITION` method will use the integer key specified by the partition key or key of the message to a specific partition on Eventhub. If the integer key is greater than the number of partitions in the destination Eventhub, a modulo operation will be performed to determine the resulting partition. ie. if there are 6 partitions and the key is 9, the message will end up in partition 3. Similarly to `EVENT_HUB_HASHING`, if the partition key is not set the message key is used instead.

If the `ROUND_ROBIN` method is used, the message key and partition key are ignored and the message will be distributed in a round-robin fashion amongst all the partitions in the downstream EventHub.

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

When the consumer reads a message from event hubs, it appends them to a shared producer-consumer buffer corresponding to its partition. This config determines the per-partition queue size. Setting a higher value for this config achieves a higher throughput at the expense of increased on-heap memory.

```
systems.eh-system.eventhubs.receive.queue.size = 10
```

For the list of all configs, check out the configuration table page [here](../jobs/configuration-table.html)
