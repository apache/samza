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

All Samza applications have a [properties format](https://en.wikipedia.org/wiki/.properties) file that defines its configurations.
A complete list of configuration keys can be found on the [__Samza Configurations Table__](samza-configurations.html) page. 
 
A very basic configuration file looks like this:

{% highlight jproperties %}
# Application Configurations
job.factory.class=org.apache.samza.job.local.YarnJobFactory
app.name=hello-world
job.default.system=example-system
serializers.registry.json.class=org.apache.samza.serializers.JsonSerdeFactory
serializers.registry.string.class=org.apache.samza.serializers.StringSerdeFactory

# Systems & Streams Configurations
systems.example-system.samza.factory=samza.stream.example.ExampleConsumerFactory
systems.example-system.samza.key.serde=string
systems.example-system.samza.msg.serde=json

# Checkpointing
task.checkpoint.factory=org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory

# State Storage
stores.example-store.factory=org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory
stores.example-store.key.serde=string
stores.example-store.value.serde=json

# Metrics
metrics.reporter.example-reporter.class=org.apache.samza.metrics.reporter.JmxReporterFactory
metrics.reporters=example-reporter
{% endhighlight %}

There are 6 sections sections to a configuration file:

1. The [__Application__](samza-configurations.html#application-configurations) section defines things like the name of the job, job factory (See the job.factory.class property in [Configuration Table](samza-configurations.html)), the class name for your [StreamTask](../api/overview.html) and serialization and deserialization of specific objects that are received and sent along different streams.
2. The [__Systems & Streams__](samza-configurations.html#systems-streams) section defines systems that your StreamTask can read from along with the types of serdes used for sending keys and messages from that system. You may use any of the [predefined systems](../connectors/overview.html) that Samza ships with, although you can also specify your own self-implemented Samza-compatible systems. See the [hello-samza example project](/startup/hello-samza/{{site.version}})'s Wikipedia system for a good example of a self-implemented system.
3. The [__Checkpointing__](samza-configurations.html#checkpointing) section defines how the messages processing state is saved, which provides fault-tolerant processing of streams (See [Checkpointing](../container/checkpointing.html) for more details).
4. The [__State Storage__](samza-configurations.html#state-storage) section defines the [stateful stream processing](../container/state-management.html) settings for Samza.
5. The [__Deployment__](samza-configurations.html#deployment) section defines how the Samza application will be deployed (To a cluster manager (YARN), or as a standalone library) as well as settings for each option. See [Deployment Models](/deployment/deployment-model.html) for more details.
6. The [__Metrics__](samza-configurations.html#metrics) section defines how the Samza application metrics will be monitored and collected. (See [Monitoring](../operations/monitoring.html))

Note that configuration keys prefixed with `sensitive.` are treated specially, in that the values associated with such keys
will be masked in logs and Samza's YARN ApplicationMaster UI.  This is to prevent accidental disclosure only; no
encryption is done.
