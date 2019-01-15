---
layout: page
title: Samza Quick Start
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

In this tutorial, we will create our first Samza application - `WordCount`. This application will consume messages from a Kafka stream, tokenize them into individual words and count the frequency of each word.  Let us download the entire project from [here](https://github.com/apache/samza-hello-samza/blob/latest/quickstart/wordcount.tar.gz).

### Setting up a Java Project

Observe the project structure as follows:

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

You can build the project anytime by running:

{% highlight bash %}
> cd wordcount
> gradle wrapper --gradle-version 4.9
> ./gradlew build
{% endhighlight %}

### Create a Samza StreamApplication

Now let’s write some code! An application written using Samza's [high-level API](/learn/documentation/{{site.version}}/api/api/high-level-api.html) implements the [StreamApplication](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html) interface:

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

The interface provides a single method named `describe()`, which allows us to define our inputs, the processing logic and outputs for our application. 

### Describe your inputs and outputs

To interact with Kafka, we will first create a `KafkaSystemDescriptor` by providing the coordinates of the Kafka cluster. For each Kafka topic our application reads from, we create a `KafkaInputDescriptor` with the name of the topic and a serializer. Likewise, for each output topic, we instantiate a corresponding `KafkaOutputDescriptor`. 

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
   // Create a KafkaSystemDescriptor providing properties of the cluster
   KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
       .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
       .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
       .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

   // For each input or output stream, create a KafkaInput/Output descriptor
   KafkaInputDescriptor<KV<String, String>> inputDescriptor =
       kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
           KVSerde.of(new StringSerde(), new StringSerde()));
   KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
       kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
           KVSerde.of(new StringSerde(), new StringSerde()));

   // Obtain a handle to a MessageStream that you can chain operations on
   MessageStream<KV<String, String>> lines = streamApplicationDescriptor.getInputStream(inputDescriptor);
   OutputStream<KV<String, String>> counts = streamApplicationDescriptor.getOutputStream(outputDescriptor);
 }
}
{% endhighlight %}

The above example creates a [MessageStream](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html) which reads from an input topic named `sample-text`. It also defines an output stream that emits results to a topic named `word-count-output`. Next let’s add our processing logic. 

### Add word count processing logic

Kafka messages typically have a key and a value. Since we only care about the value here, we will apply the `map` operator on the input stream to extract the value. 

{% highlight java %}
lines.map(kv -> kv.value)
{% endhighlight %}

Next, we will tokenize the message into individual words using the `flatmap` operator.

{% highlight java %}
.flatMap(s -> Arrays.asList(s.split("\\W+")))
{% endhighlight %}


We now need to group the words, aggregate their respective counts and periodically emit our results. For this, we will use Samza's session-windowing feature.

{% highlight java %}
.window(Windows.keyedSessionWindow(
   w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
   new StringSerde(), new IntegerSerde()), "count")
{% endhighlight %}

Let's walk through each of the parameters to the above `window` function:
The first parameter is a "key function", which defines the key to group messages by. In our case, we can simply use the word as the key. The second parameter is the windowing interval, which is set to 5 seconds. The third parameter is a function which provides the initial value for our aggregations. We can start with an initial count of zero for each word. The fourth parameter is an aggregation function for computing counts. The next two parameters specify the key and value serializers for our window. 

The output from the window operator is captured in a [WindowPane](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/windows/WindowPane.html) type, which contains the word as the key and its count as the value. We add a further `map` to format this into a `KV`, that we can send to our Kafka topic. To write our results to the output topic, we use the `sendTo` operator in Samza.


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


### Configure your application

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

We are ready to add a `main()` function to the `WordCount` class. It parses the command-line arguments and instantiates a `LocalApplicationRunner` to execute the application locally.

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


Before running `main()`, we will create our input Kafka topic and populate it with sample data. You can download the scripts to interact with Kafka along with the sample data from [here](https://github.com/apache/samza-hello-samza/blob/latest/quickstart/wordcount.tar.gz).

{% highlight bash %}
> ./scripts/grid install zookeeper && ./scripts/grid start zookeeper
> ./scripts/grid install kafka && ./scripts/grid start kafka
{% endhighlight %}


{% highlight bash %}
> ./deploy/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic sample-text --partition 1 --replication-factor 1
> ./deploy/kafka/bin/kafka-console-producer.sh --topic sample-text --broker localhost:9092 < ./sample-text.txt
{% endhighlight %}

Let’s kick off our application and use gradle to run it. Alternately, you can also run it directly from your IDE, with the same program arguments.

{% highlight bash %}
> export BASE_DIR=`pwd`
> ./gradlew run --args="--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$BASE_DIR/src/main/config/word-count.properties"
{% endhighlight %}


The application will output to a Kafka topic named "word-count-output". We will now fire up a Kafka consumer to read from this topic:

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

Congratulations! You've successfully run your first Samza application.

### [More Examples >>](/startup/code-examples/{{site.version}})