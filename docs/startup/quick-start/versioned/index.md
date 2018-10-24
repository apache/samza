---
layout: page
title: Quick Start
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

This tutorial will go through the steps of creating your first Samza application - `WordCount`. It demonstrates how to start writing a Samza application, consume from a kafka stream, tokenize the lines into words, and count the frequency of each word.  For this tutorial we are going to use gradle 4.9 to build the projects. The full tutorial project tar file can be downloaded [here](https://github.com/apache/samza-hello-samza/blob/latest/quickstart/wordcount.tar.gz).

### Setting up a Java Project

First let’s create the project structure as follows:

{% highlight bash %}
wordcount
|-- build.gradle
|-- gradle.properties
|-- scripts
|-- src
    |-- main
        |-- config
        |-- java
            |-- samzaapp
                 |-- WordCount.java
{% endhighlight %}

You can copy build.gradle and gradle.properties files from the downloaded tutorial tgz file. The WordCount class is just an empty class for now. Once finishing this setup, you can build the project by:

{% highlight bash %}
> cd wordcount
> gradle wrapper --gradle-version 4.9
> ./gradlew build
{% endhighlight %}

### Create a Samza StreamApplication

Now let’s write some code! The first step is to create your own Samza application by implementing the [StreamApplication](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html) class:

{% highlight java %}
package samzaapp;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;

public class WordCount implements StreamApplication {
 @Override
 public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
 }
}
{% endhighlight %}

The StreamApplication interface provides an API method named describe() for you to specify your streaming pipeline. Using [StreamApplicationDescriptor](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplicationDescriptor.html), you can describe your entire data processing task from data inputs, operations and outputs.

### Describe your inputs and outputs

In this example, we are going to use Kafka as the input data source and consume the text for word count line by line. We start by defining a KafkaSystemDescriptor, which specifies the properties to establishing the connection to the local Kafka cluster. Then we create a  `KafkaInputDescriptor`/`KafkaOutputDescriptor` to set up the topic, Serializer and Deserializer. Finally we use this input in the [StreamApplicationDescriptor](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplicationDescriptor.html) so we can consume from this topic. The code is in the following:

{% highlight java %}
public class WordCount implements StreamApplication {
 private static final String KAFKA_SYSTEM_NAME = "kafka";
 private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
 private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
 private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

 private static final String INPUT_STREAM_ID = "sample-text";
 private static final String OUTPUT_STREAM_ID = "word-count-output";

 @Override
 public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
   KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
       .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
       .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
       .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

   KafkaInputDescriptor<KV<String, String>> inputDescriptor =
       kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
           KVSerde.of(new StringSerde(), new StringSerde()));
   KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
       kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
           KVSerde.of(new StringSerde(), new StringSerde()));

   MessageStream<KV<String, String>> lines = streamApplicationDescriptor.getInputStream(inputDescriptor);
   OutputStream<KV<String, String>> counts = streamApplicationDescriptor.getOutputStream(outputDescriptor);
 }
}
{% endhighlight %}

The resulting [MessageStream](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html) lines contains the data set that reads from Kafka and deserialized into string of each line. We also defined the output stream counts so we can write the word count results to it. Next let’s add processing logic. 

### Add word count processing logic

First we are going to extract the value from lines. This is a one-to-one transform and we can use the Samza map operator as following:

{% highlight java %}
lines.map(kv -> kv.value)
{% endhighlight %}

Then we will split the line into words by using the flatmap operator:

{% highlight java %}
.flatMap(s -> Arrays.asList(s.split("\\W+")))
{% endhighlight %}

Now let’s think about how to count the words. We need to aggregate the count based on the word as the key, and emit the aggregation results once there are no more data coming. Here we can use a session window which will trigger the output if there is no data coming within a certain interval.

{% highlight java %}
.window(Windows.keyedSessionWindow(
   w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
   new StringSerde(), new IntegerSerde()), "count")
{% endhighlight %}

The output will be captured in a [WindowPane](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/windows/WindowPane.html) type, which contains the key and the aggregation value. We add a further map to transform that into a KV. To write the output to the output Kafka stream, we used the sentTo operator in Samza:

{% highlight java %}
.map(windowPane ->
   KV.of(windowPane.getKey().getKey(),
       windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
.sendTo(counts);
{% endhighlight %}

The full processing logic looks like the following:

{% highlight java %}
lines
   .map(kv -> kv.value)
   .flatMap(s -> Arrays.asList(s.split("\\W+")))
   .window(Windows.keyedSessionWindow(
       w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
       new StringSerde(), new IntegerSerde()), "count")
   .map(windowPane ->
       KV.of(windowPane.getKey().getKey(),
           windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
   .sendTo(counts);
{% endhighlight %}


### Config your application

In this section we will configure the word count example to run locally in a single JVM. Please add a file named “word-count.properties” under the config folder. We will add the job configs in this file.

In this section, we will configure our word count example to run locally in a single JVM. Let us add a file named “word-count.properties” under the config folder. 

{% highlight jproperties %}
job.name=word-count
# Use a PassthroughJobCoordinator since there is no coordination needed
job.coordinator.factory=org.apache.samza.standalone.PassthroughJobCoordinatorFactory
job.coordination.utils.factory=org.apache.samza.standalone.PassthroughCoordinationUtilsFactory

job.changelog.system=kafka

# Use a single container to process all of the data
task.name.grouper.factory=org.apache.samza.container.grouper.task.SingleContainerGrouperFactory
processor.id=0

# Read from the beginning of the topic
systems.kafka.default.stream.samza.offset.default=oldest
{% endhighlight %}

For more details on Samza's configs, feel free to check out the latest [configuration reference](/learn/documentation/{{site.version}}/jobs/configuration-table.html).

### Run your application

Let’s now add a `main()` function to the `WordCount` class. The function reads the config file and factory from the args, and creates a `LocalApplicationRunner` that run the application locally.

{% highlight java %}
public static void main(String[] args) {
 CommandLine cmdLine = new CommandLine();
 OptionSet options = cmdLine.parser().parse(args);
 Config config = cmdLine.loadConfig(options);
 LocalApplicationRunner runner = new LocalApplicationRunner(new WordCount(), config);
 runner.run();
 runner.waitForFinish();
}
{% endhighlight %}

In your "build.gradle" file, please add the following so we can use gradle to run it:

{% highlight jproperties %}
apply plugin:'application'

mainClassName = "samzaapp.WordCount"
{% endhighlight %}

Before running `main()`, we will create our input Kafka topic and populate it with sample data. You can download the scripts to interact with Kafka along with the sample data from [here](https://github.com/apache/samza-hello-samza/blob/latest/quickstart/wordcount.tar.gz).

{% highlight bash %}
> ./scripts/grid install zookeeper && ./scripts/grid start zookeeper
> ./scripts/grid install kafka && ./scripts/grid start kafka
{% endhighlight %}

Next we will create a Kafka topic named sample-text, and publish some sample data into it. A "sample-text.txt" file is included in the downloaded tutorial tgz file. In command line:

{% highlight bash %}
> ./deploy/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic sample-text --partition 1 --replication-factor 1
> ./deploy/kafka/bin/kafka-console-producer.sh --topic sample-text --broker localhost:9092 < ./sample-text.txt
{% endhighlight %}

Now let’s fire up our application. Here we use gradle to run it. You can also run it directly within your IDE, with the same program arguments.

{% highlight bash %}
> export BASE_DIR=`pwd`
> ./gradlew run --args="--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$BASE_DIR/src/main/config/word-count.properties"
{% endhighlight %}

This application will output to a Kafka topic named "word-count-output". Let’s consume this topic to check out the results:

{% highlight bash %}
>  ./deploy/kafka/bin/kafka-console-consumer.sh --topic word-count-output --zookeeper localhost:2181 --from-beginning
{% endhighlight %}

It will show the counts for each word like the following:

{% highlight bash %}
well: 4
complaining: 1
die: 3
but: 22
not: 50
truly: 5
murmuring: 1
satisfied: 3
the: 120
thy: 8
gods: 8
thankful: 1
and: 243
from: 16
{% endhighlight %}

### More Examples

The [hello-samza](https://github.com/apache/samza-hello-samza) project contains a lot of more examples to help you create your Samza job. To checkout the hello-samza project:

{% highlight bash %}
> git clone https://git.apache.org/samza-hello-samza.git hello-samza
{% endhighlight %}

There are four main categories of examples in this project, including:

1. [Wikipedia](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/wikipedia): this is a more complex example demonstrating the entire pipeline of consuming from the live feed from wikipedia edits, parsing the message and generating statistics from them.

2. [Cookbook](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/cookbook): you will find various examples in this folder to demonstrate usage of Samza high-level API, such as windowing, join and aggregations.

3. [Azure](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/azure): This example shows how to build an application that consumes input streams from Azure EventHubs.

4. [Kinesis](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/kinesis): This example shows how to consume from Kinesis streams.