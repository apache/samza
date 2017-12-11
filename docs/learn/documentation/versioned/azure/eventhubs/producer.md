---
layout: page
title: Producing to Eventhubs
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

The `samza-azure` module implements a Samza Producer to write to Azure EventHubs. Each Samza stream will produce to an EventHubs entity.

### EventData format

The envelopes sent will be converted to an `EventData` that is accepted by EventHub. The envelope message will be the body of the `EventData` and the message key and produce time stamp will be included in the properties map as `key` and `produce-timestamp` respectively. Currently there are no ways to pass other metadata in the properties field of `EventData`.

### Send Methods

There are three ways to partition the messages in the downstream EventHubs. They are either `EVENT_HUB_HASHING` or `PARTITION_KEY_AS_PARTITION`. 

If the `ROUND_ROBIN` method is used, the message key and partition key are ignored and the message will be distributed in a round-robin fashion amongst all the partitions in the downstream EventHub.

The `EVENT_HUB_HASHING` method employs the hashing mechanism in EventHubs to determine, based on the key of the message, which partition the message should go. Using this method still ensures that all the events with the same key are sent to the same partition in the event hub. If this option is chosen, the partition key used for the hash should be a string. If the partition key is not set, the message key is used instead.

The `PARTITION_KEY_AS_PARTITION` method will use the integer key specified by the partition key or key of the message to a specific partition on Eventhub. If the integer key is greater than the number of partitions in the destination Eventhub, a modulo operation will be performed to determine the resulting partition. ie. if there are 6 partitions and the key is 9, the message will end up in partition 3. Similarly to `EVENT_HUB_HASHING`, if the partition key is not set the message key is used instead.

![diagram-medium](/img/{{site.version}}/learn/documentation/azure/eventhub_send_methods.png)


### Configuring an EventHubSystemProducer

The configurations for the EventhubSystemProducer are set and read from the configuration keys and values set in a `job.properties` file.
Here is an example configuration of the system producer for use by your `StreamTasks` or `StreamApplication`:

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

# Assign the send method to determine the destination partitions of the envelopes, defaults to EVENT_HUB_HASHING if not set.
systems.eh-system.eventhubs.partition.method=ROUND_ROBIN
# Here are the other possible options described above
# systems.eh-system.eventhubs.partition.method=EVENT_HUB_HASHING
# systems.eh-system.eventhubs.partition.method=PARTITION_KEY_AS_PARTITION

# Optional: Sending the message key to the eventhub in the EventData properties is optional. Defaults to true if not set.
# systems.eh-system.eventhubs.send.key=true

# Optional: Set the timeout for fetching the runtime metadata from an Eventhub entity in millis. Defaults to 60000
# systems.eh-system.eventhubs.runtime.info.timeout=60000
```

Make sure to provide the `SOME-SERDE-IMPL` in the config if not using an the Fluent API. The tuple required to access the Eventhubs entity per stream must be provided, namely the fields `YOUR-STREAM-NAMESPACE`, `YOUR-ENTITY-NAME`, `YOUR-SAS-KEY-NAME`, `YOUR-SAS-KEY-TOKEN`. Since there can be namespace and entity levels of sas tokens each stream/entity requires a token. 

For the list of all configs, check out the configuration table page [here](../../jobs/configuration-table.html)

## [Reading from EventHubs &raquo;](../eventhubs/consumer.html)
