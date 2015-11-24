---
layout: page
title: Serialization
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

Every message that is read from or written to a [stream](streams.html) or a [persistent state store](state-management.html) needs to eventually be serialized to bytes (which are sent over the network or written to disk). There are various places where that serialization and deserialization can happen:

1. In the client library: for example, the library for publishing to Kafka and consuming from Kafka supports pluggable serialization.
2. In the task implementation: your [process method](../api/overview.html) can use raw byte arrays as inputs and outputs, and do any parsing and serialization itself.
3. Between the two: Samza provides a layer of serializers and deserializers, or *serdes* for short.

You can use whatever makes sense for your job; Samza doesn't impose any particular data model or serialization scheme on you. However, the cleanest solution is usually to use Samza's serde layer. The following configuration example shows how to use it.

{% highlight jproperties %}
# Define a system called "kafka"
systems.kafka.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory

# The job is going to consume a topic called "PageViewEvent" from the "kafka" system
task.inputs=kafka.PageViewEvent

# Define a serde called "json" which parses/serializes JSON objects
serializers.registry.json.class=org.apache.samza.serializers.JsonSerdeFactory

# Define a serde called "integer" which encodes an integer as 4 binary bytes (big-endian)
serializers.registry.integer.class=org.apache.samza.serializers.IntegerSerdeFactory

# For messages in the "PageViewEvent" topic, the key (the ID of the user viewing the page)
# is encoded as a binary integer, and the message is encoded as JSON.
systems.kafka.streams.PageViewEvent.samza.key.serde=integer
systems.kafka.streams.PageViewEvent.samza.msg.serde=json

# Define a key-value store which stores the most recent page view for each user ID.
# Again, the key is an integer user ID, and the value is JSON.
stores.LastPageViewPerUser.factory=org.apache.samza.storage.kv.KeyValueStorageEngineFactory
stores.LastPageViewPerUser.changelog=kafka.last-page-view-per-user
stores.LastPageViewPerUser.key.serde=integer
stores.LastPageViewPerUser.msg.serde=json
{% endhighlight %}

Each serde is defined with a factory class. Samza comes with several builtin serdes for UTF-8 strings, binary-encoded integers, JSON and more. The following is a comprehensive list of supported serdes in Samza.
<style>
            table th, table td {
                text-align: left;
                vertical-align: top;
                padding: 12px;
                border-bottom: 1px solid #ccc;
                border-top: 1px solid #ccc;
                border-left: 0;
                border-right: 0;
            }

            table td.property, table td.default {
                white-space: nowrap;
            }

            table th {
                background-color: #eee;
            }
</style>
<table>
    <tr>
        <th> Serde Name</th>
        <th> Data Handled </th>
    </tr>
    <tr>
        <td> string </td>
        <td> UTF-8 strings </td>
    </tr>
    <tr>
        <td> integer </td>
        <td> binary-encoded integers </td>
    </tr>
    <tr>
        <td> serializable </td>
        <td> Serializable Object Type </td>
    </tr>
    <tr>
        <td> long </td>
        <td> long data type </td>
    </tr>
    <tr>
        <td> json </td>
        <td> JSON formatted data </td>
    </tr>
    <tr>
        <td> byte </td>
        <td> Plain Bytes (effectively no-op) - Useful for Binary Messages </td>
    </tr>
    <tr>
        <td> bytebuffer </td>
        <td> Byte Buffer </td>
    </tr>
</table>

You can also create your own serializer by implementing the [SerdeFactory](../api/javadocs/org/apache/samza/serializers/SerdeFactory.html) interface.

The name you give to a serde (such as "json" and "integer" in the example above) is only for convenience in your job configuration; you can choose whatever name you like. For each stream and each state store, you can use the serde name to declare how messages should be serialized and deserialized.

If you don't declare a serde, Samza simply passes objects through between your task instance and the system stream. In that case your task needs to send and receive whatever type of object the underlying client library uses.

All the Samza APIs for sending and receiving messages are typed as *Object*. This means that you have to cast messages to the correct type before you can use them. It's a little bit more code, but it has the advantage that Samza is not restricted to any particular data model.

## [Checkpointing &raquo;](checkpointing.html)
