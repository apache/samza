---
layout: page
title: Consuming from Eventhubs
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

You can configure your Samza job to consume from Eventhub files. The [EventhubSystemConsumer](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubSystemConsumer.java) can read from Eventhub streams. Each Eventhub entity is read to a different Samza stream. Each partition in an Eventhub entity will naturally be mapped to a partition of the same mapped stream in Samza.

### Event format

The incoming message is parsed from the [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data). The key of the message is set to the partition key of the EventData. If that is not present, then it will be set to the user defined `key` property map of the EventData (Symmetrical to the EventhubSystemProducer). The message is obtained from the EventData body.

[EventhubSystemConsumer](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubSystemConsumer.java) receives [EventHubIncomingMessageEnvelope](https://github.com/apache/samza/blob/master/samza-azure/src/main/java/org/apache/samza/system/eventhub/consumer/EventHubIncomingMessageEnvelope.java), a special version of [IncomingMessageEnvelope](../../api/javadocs/org/apache/samza/system/IncomingMessageEnvelope.html) which contains [EventData](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data) that is provided by the Eventhub. The EventData allows the retrieval of extra system and user properties attached to the underlying AMQP message.


### Consumer groups

Consumer group of the stream can be configured on a stream level. EventHub documentation indicates that there can be at most [5 concurrent readers](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers) on a partition per consumer group. The number of available consumer groups available depends on the version of Azure subscription. Defaults to the `$Default` consumer group that exists for every stream (unless deleted). All the consumers in the same group share the upstream messages ie. the intersection of messages consumed between streams in the same group is always empty.

### Configuring an EventHubSystemConsumer

Here is a few of the basic configs to set up EventhubSystemConsumer:

```
# Set the SystemFactory implementation to instantiate EventhubSystemProducer where the name of the system is 'eh-system'
systems.eh-system.samza.factory=org.apache.samza.system.eventhub.EventHubSystemFactory

# Define a serializer/deserializer for the eh-inputstream system this can be specified in the StreamApplication for Fluent/High-level API
systems.eh-system.samza.msg.serde=SOME-SERDE-IMPL

# Define the stream lists by stream-ids that will be used with Eventhub, here we are defining 2 streams eh-inputstream0 and eh-inputstream1
systems.eh-system.stream.list=eh-inputstream0, eh-inputstream1

# Set the system of the streams, alternatively can be set with the job.default.system=eh-system config
streams.eh-inputstream0.samza.system=eh-system
streams.eh-inputstream1.samza.system=eh-system

# Enter your Eventhubs access credentials for each system stream
systems.eh-system.streams.eh-inputstream0.eventhubs.namespace=YOUR-STREAM-NAMESPACE
systems.eh-system.streams.eh-inputstream0.eventhubs.entitypath=YOUR-ENTITY-NAME
systems.eh-system.streams.eh-inputstream0.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
systems.eh-system.streams.eh-inputstream0.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN

# Must be done for all streams
systems.eh-system.streams.eh-inputstream1.eventhubs.namespace=YOUR-STREAM-NAMESPACE
systems.eh-system.streams.eh-inputstream1.eventhubs.entitypath=YOUR-ENTITY-NAME
systems.eh-system.streams.eh-inputstream1.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
systems.eh-system.streams.eh-inputstream1.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN

# Set the consumer group discussed above. The group defaults to `$Default` if not set
systems.eh-system.streams.eh-inputstream0.eventhubs.consumer.group=YOUR-CONSUMER-GROUP-FOR-STREAM
systems.eh-system.streams.eh-inputstream1.eventhubs.consumer.group=YOUR-CONSUMER-GROUP-FOR-STREAM

# Optional: Max size of the intermediate queue Eventhub client pushes to and Samza reads from. Defaults to 100.
# systems.eh-system.eventhubs.receive.queue.size=100

# Optional: Set the timeout for fetching the runtime metadata from an Eventhub entity in millis. Defaults to 60000
# systems.eh-system.eventhubs.runtime.info.timeout=60000
```

Make sure to provide the `SOME-SERDE-IMPL` in the config if not using an the Fluent API. 

Similarly to the Producer, the tuple required to access the Eventhubs entity per stream must be provided, namely the fields `YOUR-STREAM-NAMESPACE`, `YOUR-ENTITY-NAME`, `YOUR-SAS-KEY-NAME`, `YOUR-SAS-KEY-TOKEN`. Additionally the `YOUR-CONSUMER-GROUP-FOR-STREAM` should be configured if required, otherwise the `$Default` group will be used.

For the list of all configs, check out the configuration table page [here](../../jobs/configuration-table.html)

## [Security &raquo;](../operations/security.html)