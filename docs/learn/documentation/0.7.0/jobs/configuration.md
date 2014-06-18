---
layout: page
title: Configuration
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

All Samza jobs have a configuration file that defines the job. A very basic configuration file looks like this:

{% highlight jproperties %}
# Job
job.factory.class=samza.job.local.LocalJobFactory
job.name=hello-world

# Task
task.class=samza.task.example.MyJavaStreamerTask
task.inputs=example-system.example-stream

# Serializers
serializers.registry.json.class=org.apache.samza.serializers.JsonSerdeFactory
serializers.registry.string.class=org.apache.samza.serializers.StringSerdeFactory

# Systems
systems.example-system.samza.factory=samza.stream.example.ExampleConsumerFactory
systems.example-system.samza.key.serde=string
systems.example-system.samza.msg.serde=json
{% endhighlight %}

There are four major sections to a configuration file:

1. The job section defines things like the name of the job, and whether to use the YarnJobFactory or LocalJobFactory.
2. The task section is where you specify the class name for your [StreamTask](../api/overview.html). It's also where you define what the [input streams](../container/streams.html) are for your task.
3. The serializers section defines the classes of the [serdes](../container/serialization.html) used for serialization and deserialization of specific objects that are received and sent along different streams.
4. The system section defines systems that your StreamTask can read from along with the types of serdes used for sending keys and messages from that system. Usually, you'll define a Kafka system, if you're reading from Kafka, although you can also specify your own self-implemented Samza-compatible systems. See the [hello-samza example project](/startup/hello-samza/0.7.0)'s Wikipedia system for a good example of a self-implemented system.

### Required Configuration

Configuration keys that absolutely must be defined for a Samza job are:

* `job.factory.class`
* `job.name`
* `task.class`
* `task.inputs`

### Configuration Keys

A complete list of configuration keys can be found on the [Configuration Table](configuration-table.html) page.

## [Packaging &raquo;](packaging.html)
