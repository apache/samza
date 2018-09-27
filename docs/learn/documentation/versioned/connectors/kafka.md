---
layout: page
title: Kafka Connector
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
Samza offers built-in integration with Apache Kafka for stream processing. A common pattern in Samza applications is to read messages from one or more Kafka topics, process it and emit results to other Kafka topics or external systems.

## Consuming from Kafka

### <a name="kafka-basic-configuration"></a>Basic Configuration

The example below provides a basic example for configuring a system called `kafka-cluster-1` that uses the provided KafkaSystemFactory.

{% highlight jproperties %}
# Set the SystemFactory implementation to instantiate KafkaSystemConsumer, KafkaSystemProducer and KafkaSystemAdmin
systems.kafka-cluster-1.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory

# Define the default key and message SerDe.
systems.kafka-cluster-1.default.stream.samza.key.serde=string
systems.kafka-cluster-1.default.stream.samza.msg.serde=json

# Zookeeper nodes of the Kafka cluster
systems.kafka-cluster-1.consumer.zookeeper.connect=localhost:2181

# List of network endpoints where Kafka brokers are running. Also needed by consumers for querying metadata.
systems.kafka-cluster-1.producer.bootstrap.servers=localhost:9092,localhost:9093
{% endhighlight %}

Samza provides a built-in KafkaSystemDescriptor to consume from and produce to Kafka from the StreamApplication (High-level API) or the TaskApplication (Low-level API).

Below is an example of how to use the descriptors in the describe method of a StreamApplication.

{% highlight java %}
public class PageViewFilter implements StreamApplication {
  @Override
  public void describe(StreamApplicationDescriptor appDesc) {
    // add input and output streams
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("kafka-cluster-1");
    KafkaInputDescriptor<PageView> isd = ksd.getInputDescriptor("myinput", new JsonSerdeV2<>(PageView.class));
    KafkaOutputDescriptor<DecoratedPageView> osd = ksd.getOutputDescriptor("myout", new JsonSerdeV2<>(DecordatedPageView.class));

    MessageStream<PageView> ms = appDesc.getInputStream(isd);
    OutputStream<DecoratedPageView> os = appDesc.getOutputStream(osd);

    ms.filter(this::isValidPageView)
      .map(this::addProfileInformation)
      .sendTo(os);
  }
}
{% endhighlight %}

Below is an example of how to use the descriptors in the describe method of a TaskApplication

{% highlight java %}
public class PageViewFilterTask implements TaskApplication {
  @Override
  public void describe(TaskApplicationDescriptor appDesc) {
    // add input and output streams
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("kafka-cluster-1");
    KafkaInputDescriptor<String> isd = ksd.getInputDescriptor("myinput", new StringSerde());
    KafkaOutputDescriptor<String> osd = ksd.getOutputDescriptor("myout", new StringSerde());

    appDesc.addInputStream(isd);
    appDesc.addOutputStream(osd);
    appDesc.addTable(td);

    appDesc.setTaskFactory((StreamTaskFactory) () -> new MyStreamTask());
  }
}
{% endhighlight %}

### Advanced Configuration

Prefix the configuration with `systems.system-name.consumer.` followed by any of the Kafka consumer configurations. See [Kafka Consumer Configuration Documentation](http://kafka.apache.org/documentation.html#consumerconfigs)

{% highlight jproperties %}
systems.kafka-cluster-1.consumer.security.protocol=SSL
systems.kafka-cluster-1.consumer.max.partition.fetch.bytes=524288
{% endhighlight %}

## Producing to Kafka

### Basic Configuration

The basic configuration is the same as [Consuming from Kafka](#kafka-basic-configuration).

### Advanced Configuration

#### Changelog to Kafka for State Stores

For Samza processors that have local state and is configured with a changelog for durability, if the changelog is configured to use Kafka, there are Kafka specific configuration parameters.
See section on `TODO: link to state management section` State Management `\TODO` for more details.

{% highlight jproperties %}
stores.store-name.changelog=kafka-cluster-2.changelog-topic-name
stores.store-name.changelog.replication.factor=3
stores.store-name.changelog.kafka.cleanup.policy=compact
{% endhighlight %}

#### Performance Tuning

Increasing the consumer fetch buffer thresholds may improve throughput at the expense of memory by buffering more messages. Run some performance analysis to find the optimal values.

{% highlight jproperties %}
# Max number of messages to buffer across all Kafka input topic partitions per container. Default is 50000 messages.
systems.kafka-cluster-1.samza.fetch.threshold=10000
# Max buffer size by bytes. This configuration takes precedence over the above configuration if value is not -1. Default is -1.
systems.kafka-cluster-1.samza.fetch.threshold.bytes=-1
{% endhighlight %}
