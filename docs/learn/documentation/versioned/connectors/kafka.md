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

### Kafka I/O : QuickStart
Samza offers built-in integration with Apache Kafka for stream processing. A common pattern in Samza applications is to read messages from one or more Kafka topics, process them and emit results to other Kafka topics or databases.

The `hello-samza` project includes multiple examples on interacting with Kafka from your Samza jobs. Each example also includes instructions on how to run them and view results. 

- [High Level Streams API Example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/FilterExample.java) with a corresponding [tutorial](/learn/documentation/{{site.version}}/deployment/yarn.html#starting-your-application-on-yarn)

- [Low Level Task API Example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/wikipedia/task/application/WikipediaParserTaskApplication.java) with a corresponding [tutorial](https://github.com/apache/samza-hello-samza#hello-samza)


### Concepts

####KafkaSystemDescriptor

Samza refers to any IO source (eg: Kafka) it interacts with as a _system_, whose properties are set using a corresponding `SystemDescriptor`. The `KafkaSystemDescriptor` allows you to describe the Kafka cluster you are interacting with and specify its properties. 

{% highlight java %}
    KafkaSystemDescriptor kafkaSystemDescriptor =
        new KafkaSystemDescriptor("kafka").withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);
{% endhighlight %}


####KafkaInputDescriptor

A Kafka cluster usually has multiple topics (a.k.a _streams_). The `KafkaInputDescriptor` allows you to specify the properties of each Kafka topic your application should read from. For each of your input topics, you should create a corresponding instance of `KafkaInputDescriptor`
by providing a topic-name and a serializer.
{% highlight java %}
    KafkaInputDescriptor<PageView> pageViewStreamDescriptor = kafkaSystemDescriptor.getInputDescriptor("page-view-topic", new JsonSerdeV2<>(PageView.class));
{% endhighlight %}

The above example describes an input Kafka stream from the "page-view-topic" which Samza de-serializes into a JSON payload. Samza provides default serializers for common data-types like string, avro, bytes, integer etc.
 
####KafkaOutputDescriptor

Similarly, the `KafkaOutputDescriptor` allows you to specify the output streams for your application. For each output topic you write to, you should create an instance of `KafkaOutputDescriptor`.

{% highlight java %}
    KafkaOutputDescriptor<DecoratedPageView> decoratedPageView = kafkaSystemDescriptor.getOutputDescriptor("my-output-topic", new JsonSerdeV2<>(DecoratedPageView.class));
{% endhighlight %}


### Configuration

#####Configuring Kafka producer and consumer
 
The `KafkaSystemDescriptor` allows you to specify any [Kafka producer](https://kafka.apache.org/documentation/#producerconfigs) or [Kafka consumer](https://kafka.apache.org/documentation/#consumerconfigs)) property which are directly passed over to the underlying Kafka client. This allows for 
precise control over the KafkaProducer and KafkaConsumer used by Samza. 

{% highlight java %}
    KafkaSystemDescriptor kafkaSystemDescriptor =
        new KafkaSystemDescriptor("kafka").withConsumerZkConnect(..)
            .withProducerBootstrapServers(..)
            .withConsumerConfigs(..)
            .withProducerConfigs(..)
{% endhighlight %}


####Accessing an offset which is out-of-range
This setting determines the behavior if a consumer attempts to read an offset that is outside of the current valid range maintained by the broker. This could happen if the topic does not exist, or if a checkpoint is older than the maximum message history retained by the brokers. 

{% highlight java %}
    KafkaSystemDescriptor kafkaSystemDescriptor =
        new KafkaSystemDescriptor("kafka").withConsumerZkConnect(..)
            .withProducerBootstrapServers(..)
            .withConsumerAutoOffsetReset("largest")
{% endhighlight %}


#####Ignoring checkpointed offsets
Samza periodically persists the last processed Kafka offsets as a part of its checkpoint. During startup, Samza resumes consumption from the previously checkpointed offsets by default. You can over-ride this behavior and configure Samza to ignore checkpoints with `KafkaInputDescriptor#shouldResetOffset()`.
Once there are no checkpoints for a stream, the `#withOffsetDefault(..)` determines whether we start consumption from the oldest or newest offset. 

{% highlight java %}
KafkaInputDescriptor<PageView> pageViewStreamDescriptor = 
    kafkaSystemDescriptor.getInputDescriptor("page-view-topic", new JsonSerdeV2<>(PageView.class)) 
        .shouldResetOffset()
        .withOffsetDefault(OffsetType.OLDEST);

{% endhighlight %}

The above example configures Samza to ignore checkpointed offsets for `page-view-topic` and consume from the oldest available offset during startup. You can configure this behavior to apply to all topics in the Kafka cluster by using `KafkaSystemDescriptor#withDefaultStreamOffsetDefault`.

 

### Code walkthrough: High Level Streams API

In this section, we walk through a complete example that reads from a Kafka topic, filters a few messages and writes them to another topic.

{% highlight java %}
// Define coordinates of the Kafka cluster using the KafkaSystemDescriptor
1    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor("kafka")
2        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
3        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)

// Create an KafkaInputDescriptor for your input topic and a KafkaOutputDescriptor for the output topic 
4    KVSerde<String, PageView> serde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class));
5    KafkaInputDescriptor<KV<String, PageView>> inputDescriptor =
6        kafkaSystemDescriptor.getInputDescriptor("page-views", serde);
7    KafkaOutputDescriptor<KV<String, PageView>> outputDescriptor =
8        kafkaSystemDescriptor.getOutputDescriptor("filtered-page-views", serde);


// Obtain a message stream the input topic
9    MessageStream<KV<String, PageView>> pageViews = appDescriptor.getInputStream(inputDescriptor);

// Obtain an output stream for the topic    
10    OutputStream<KV<String, PageView>> filteredPageViews = appDescriptor.getOutputStream(outputDescriptor);

// write results to the output topic
11    pageViews
12       .filter(kv -> !INVALID_USER_ID.equals(kv.value.userId))
13       .sendTo(filteredPageViews);

{% endhighlight %}

- Lines 1-3 create a KafkaSystemDescriptor defining the coordinates of our Kafka cluster

- Lines 4-6 defines a KafkaInputDescriptor for our input topic - `page-views`

- Lines 7-9 defines a KafkaOutputDescriptor for our output topic - `filtered-page-views`

- Line 9 creates a MessageStream for the input topic so that you can chain operations on it later

- Line 10 creates an OuputStream for the output topic

- Lines 11-13 define a simple pipeline that reads from the input stream and writes filtered results to the output stream
