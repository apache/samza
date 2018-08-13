---
layout: page
title: Hello Samza High Level API - Code Walkthrough
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

This tutorial introduces the high level API by showing you how to build wikipedia application from the [hello-samza high level API Yarn tutorial] (hello-samza-high-level-yarn.html). Upon completion of this tutorial, you'll know how to implement and configure a [StreamApplication](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html). Along the way, you'll see how to use some of the basic operators as well as how to leverage key-value stores and metrics in an app.

The same [hello-samza](https://github.com/apache/samza-hello-samza) project is used for this tutorial as for many of the others. You will clone that project and by the end of the tutorial, you will have implemented a duplicate of the `WikipediaApplication`.

Let's get started.

### Get the Code

Check out the hello-samza project:

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
{% endhighlight %}

This project already contains implementations of the wikipedia application using both the low-level task API and the high-level API. The low-level task implementations are in the `samza.examples.wikipedia.task` package. The high-level application implementation is in the `samza.examples.wikipedia.application` package.

This tutorial will provide step by step instructions to recreate the existing wikipedia application.

### Introduction to Wikipedia Consumer
In order to consume events from Wikipedia, the hello-samza project includes a `WikipediaSystemFactory` implementation of the Samza [SystemFactory](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/system/SystemFactory.html) that provides a `WikipediaConsumer`.

The WikipediaConsumer is an implementation of [SystemConsumer](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/system/SystemConsumer.html) that can consume events from Wikipedia. It is also a listener for events from the `WikipediaFeed`. It's important to note that the events received in `onEvent` are of the type `WikipediaFeedEvent`, so we will expect that type for messages on our input streams. For other systems, the messages may come in the form of `byte[]`. In that case you may want to configure a samza [serde](/learn/documentation/{{site.version}}/container/serialization.html) and the application should expect the output type of that serde.

Now that we understand the Wikipedia system and the types of inputs we'll be processing, we can proceed with creating our application.

### Create the Initial Config
In the hello-samza project, configs are kept in the _src/main/config/_ path. This is where we will add the config for our application.
Create a new file named _my-wikipedia-application.properties_ in this location.

#### Core Configuration
Let's start by adding some of the core properties to the file:

{% highlight bash %}
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

app.class=samza.examples.wikipedia.application.MyWikipediaApplication
app.runner.class=org.apache.samza.runtime.RemoteApplicationRunner

job.factory.class=org.apache.samza.job.yarn.YarnJobFactory
job.name=my-wikipedia-application
job.default.system=kafka

yarn.package.path=file://${basedir}/target/${project.artifactId}-${pom.version}-dist.tar.gz
{% endhighlight %}

Be sure to include the Apache header. The project will not compile without it. 

Here's a brief summary of what we configured so far.

* **app.class**: the class that defines the application logic. We will create this class later.
* **app.runner.class**: the runner implementation which will launch our application. Since we are using YARN, we use `RemoteApplicationRunner` which is required for any cluster-based deployment.
* **job.factory.class**: the [factory](/learn/documentation/{{site.version}}/jobs/job-runner.html) that will create the runtime instances of our jobs. Since we are using YARN, we want each job to be created as a [YARN job](/learn/documentation/{{site.version}}/jobs/yarn-jobs.html), so we use `YarnJobFactory`
* **job.name**: the primary identifier for the job.
* **job.default.system**: the default system to use for input, output, and internal metadata streams. This can be overridden on a per-stream basis. The _kafka_ system will be defined in the next section.
* **yarn.package.path**: tells YARN where to find the [job package](/learn/documentation/{{site.version}}/jobs/packaging.html) so the Node Managers can download it.

These basic configurations are enough to launch the application on YARN but we haven’t defined any streaming systems for Samza to use, so the application would not process anything.

Next, let's define the streaming systems with which the application will interact. 

#### Define Systems
This Wikipedia application will consume events from Wikipedia and produce stats to a Kafka topic. We need to define those systems in config before Samza can use them. Add the following lines to the config:

{% highlight bash %}
systems.wikipedia.samza.factory=samza.examples.wikipedia.system.WikipediaSystemFactory
systems.wikipedia.host=irc.wikimedia.org
systems.wikipedia.port=6667

systems.kafka.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory
systems.kafka.consumer.zookeeper.connect=localhost:2181/
systems.kafka.producer.bootstrap.servers=localhost:9092
systems.kafka.default.stream.replication.factor=1
{% endhighlight %}

The above configuration defines 2 systems; one called _wikipedia_ and one called _kafka_.

A factory is required for each system, so the _systems.system-name.samza.system.factory_ property is required for both systems. The other properties are system and use-case specific.

For the _kafka_ system, we set the default replication factor to 1 for all streams because this application is intended for a demo deployment which utilizes a Kafka cluster with only 1 broker, so a replication factor larger than 1 is invalid.

#### Configure Streams
Samza identifies streams using a unique stream ID. In most cases, the stream ID is the same as the actual stream name. However, if a stream has a name that doesn't match the pattern `[A-Za-z0-9_-]+`, we need to configure a separate _physical.name_ to associate the actual stream name with a legal stream ID. The Wikipedia channels we will consume have a '#' character in the names. So for each of them we must pick a legal stream ID and then configure the physical name to match the channel.

Samza uses the _job.default.system_ for any streams that do not explicitly specify a system. In the previous sections, we defined 2 systems, _wikipedia_ and _kafka_, and we configured _kafka_ as the default. To understand why, let's look at the streams and how Samza will use them.

For this app, Samza will:

1. Consume from input streams
2. Produce to an output stream and a metrics stream
3. Both produce and consume from job-coordination, checkpoint, and changelog streams

While the _wikipedia_ system is necessary for case 1, it does not support producers (we can't write Samza output to Wikipedia), which are needed for cases 2-3. So it is more convenient to use _kafka_ as the default system. We can then explicitly configure the input streams to use the _wikipedia_ system.

{% highlight bash %}
streams.en-wikipedia.samza.system=wikipedia
streams.en-wikipedia.samza.physical.name=#en.wikipedia

streams.en-wiktionary.samza.system=wikipedia
streams.en-wiktionary.samza.physical.name=#en.wiktionary

streams.en-wikinews.samza.system=wikipedia
streams.en-wikinews.samza.physical.name=#en.wikinews
{% endhighlight %}

The above configurations declare 3 streams with IDs, _en-wikipedia_, _en-wiktionary_, and _en-wikinews_. It associates each stream with the _wikipedia_ system we defined earlier and set the physical name to the corresponding Wikipedia channel. 

Since all the Kafka streams for cases 2-3 are on the default system and do not include special characters in their names, we do not need to configure them explicitly.

### Create a StreamApplication

With the core configuration settled, we turn our attention to code.

### Define Application Logic
Let's create the application class we configured above. The next 8 sections walk you through writing the code for the Wikipedia application.

Create a new class named `MyWikipediaApplication` in the `samza.examples.wikipedia.application` package. The class must implement [StreamApplication](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html) and should look like this:

{% highlight java %}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package samza.examples.wikipedia.application;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.application.StreamGraph;

public class MyWikipediaApplication implements StreamApplication{
  @Override
  public void init(StreamGraph streamGraph, Config config) {
    
  }
}
{% endhighlight %}

Be sure to include the Apache header. The project will not compile without it.

The [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html#init-org.apache.samza.application.StreamGraph-org.apache.samza.config.Config-) method is where the application logic is defined. The [Config](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/config/Config.html) argument is the runtime configuration loaded from the properties file we defined earlier. The [StreamGraph](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/StreamGraph.html) argument provides methods to declare input streams. You can then invoke a number of flexible operations on those streams. The result of each operation is another stream, so you can keep chaining more operations or direct the result to an output stream.

Next, we will declare the input streams for the Wikipedia application.

#### Inputs
The Wikipedia application consumes events from three channels. Let's declare each of those channels as an input streams via the [StreamGraph](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/StreamGraph.html) in the [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html#init-org.apache.samza.application.StreamGraph-org.apache.samza.config.Config-) method.
{% highlight java %}
MessageStream<WikipediaFeedEvent> wikipediaEvents = streamGraph.getInputStream("en-wikipedia", new NoOpSerde<>());
MessageStream<WikipediaFeedEvent> wiktionaryEvents = streamGraph.getInputStream("en-wiktionary", new NoOpSerde<>());
MessageStream<WikipediaFeedEvent> wikiNewsEvents = streamGraph.getInputStream("en-wikinews", new NoOpSerde<>());
{% endhighlight %}

The first argument to the [getInputStream](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/StreamGraph.html#getInputStream-java.lang.String-org.apache.samza.serializers.Serde-) method is the stream ID. Each ID must match the corresponding stream IDs we configured earlier.
The second argument is the `Serde` used to deserialize the message. We've set this to a `NoOpSerde` since our `wikipedia` system already returns `WikipediaFeedEvent`s and there is no need for further deserialization.

Note the streams are all MessageStreams of type WikipediaFeedEvent. [MessageStream](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html) is the in-memory representation of a stream in Samza. It uses generics to ensure type safety across the streams and operations.

#### Merge
We'd like to use the same processing logic for all three input streams, so we will use the [mergeAll](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#mergeAll-java.util.Collection-) operator to merge them together. Note: this is not the same as a [join](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#join-org.apache.samza.operators.MessageStream-org.apache.samza.operators.functions.JoinFunction-java.time.Duration-) because we are not associating events by key. We are simply combining three streams into one, like a union.

Add the following snippet to the [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html#init-org.apache.samza.application.StreamGraph-org.apache.samza.config.Config-) method. It merges all the input streams into a new one called _allWikipediaEvents_
{% highlight java %}
MessageStream<WikipediaFeed.WikipediaFeedEvent> allWikipediaEvents = MessageStream.mergeAll(ImmutableList.of(wikipediaEvents, wiktionaryEvents, wikiNewsEvents));
{% endhighlight %}

Note there is a [merge](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#merge-java.util.Collection-) operator instance method on MessageStream, but the static [mergeAll](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#mergeAll-java.util.Collection-) method is a more convenient alternative if you need to merge many streams.

#### Parse
The next step is to parse the events and extract some information. We will use the pre-existing `WikipediaParser.parseEvent()' method to do this. The parser extracts some flags we want to monitor as well as some metadata about the event. Inspect the method signature. The input is a WikipediaFeedEvents and the output is a Map<String, Object>. These types will be reflected in the types of the streams before and after the operation.

In the [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html#init-org.apache.samza.application.StreamGraph-org.apache.samza.config.Config-) method, invoke the [map](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#map-org.apache.samza.operators.functions.MapFunction-) operation on `allWikipediaEvents`, passing the `WikipediaParser::parseEvent` method reference as follows:

{% highlight java %}
allWikipediaEvents.map(WikipediaParser::parseEvent);
{% endhighlight %}

#### Window
Now that we have the relevant information extracted, let's perform some aggregations over a 10-second [window](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/windows/Window.html).

First, we need a container class for statistics we want to track. Add the following static class after the [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html#init-org.apache.samza.application.StreamGraph-org.apache.samza.config.Config-) method.
{% highlight java %}
private static class WikipediaStats {
  int edits = 0;
  int byteDiff = 0;
  Set<String> titles = new HashSet<String>();
  Map<String, Integer> counts = new HashMap<String, Integer>();
}
{% endhighlight %}

Now we need to define the logic to aggregate the stats over the duration of the window. To do this, we implement [FoldLeftFunction](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/functions/FoldLeftFunction.html) by adding the following class after the `WikipediaStats` class:
{% highlight java %}
private class WikipediaStatsAggregator implements FoldLeftFunction<Map<String, Object>, WikipediaStats> {

  @Override
  public WikipediaStats apply(Map<String, Object> edit, WikipediaStats stats) {
    // Update window stats
    stats.edits++;
    stats.byteDiff += (Integer) edit.get("diff-bytes");
    stats.titles.add((String) edit.get("title"));

    Map<String, Boolean> flags = (Map<String, Boolean>) edit.get("flags");
    for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
      if (Boolean.TRUE.equals(flag.getValue())) {
        stats.counts.compute(flag.getKey(), (k, v) -> v == null ? 0 : v + 1);
      }
    }

    return stats;
  }
}
{% endhighlight %}

Note: the type parameters for [FoldLeftFunction](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/functions/FoldLeftFunction.html) reflect the upstream data type and the window value type, respectively. In our case, the upstream type is the output of the parser and the window value is our `WikipediaStats` class.

Finally, we can define our [window](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/windows/Window.html) back in the [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html#init-org.apache.samza.application.StreamGraph-org.apache.samza.config.Config-) method by chaining the result of the parser:
{% highlight java %}
allWikipediaEvents.map(WikipediaParser::parseEvent)
        .window(Windows.tumblingWindow(Duration.ofSeconds(10), WikipediaStats::new, new WikipediaStatsAggregator(), new JsonSerdeV2<>(WikipediaStats.class)));
{% endhighlight %}

This defines an unkeyed [tumbling window](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/windows/Windows.html) that spans 10s, which instantiates a new `WikipediaStats` object at the beginning of each window and aggregates the stats using `WikipediaStatsAggregator`.

The output of the window is a [WindowPane](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/windows/WindowPane.html) with a key and value. Since we used an unkeyed tumbling window, the key is `Void`. The value is our `WikipediaStats` object.

#### Output
We will do a [map](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#map-org.apache.samza.operators.functions.MapFunction-) at the end to format our window output. Let's begin by defining a simple container class for our formatted output.

{% highlight java %}
  static class WikipediaStatsOutput {
    public int edits;
    public int bytesAdded;
    public int uniqueTitles;
    public Map<String, Integer> counts;

    public WikipediaStatsOutput(int edits, int bytesAdded, int uniqueTitles,
        Map<String, Integer> counts) {
      this.edits = edits;
      this.bytesAdded = bytesAdded;
      this.uniqueTitles = uniqueTitles;
      this.counts = counts;
    }
  }
{% endhighlight %}

Paste the following after the aggregator class:
{% highlight java %}
  private WikipediaStatsOutput formatOutput(WindowPane<Void, WikipediaStats> statsWindowPane) {
    WikipediaStats stats = statsWindowPane.getMessage();
    return new WikipediaStatsOutput(
        stats.edits, stats.byteDiff, stats.titles.size(), stats.counts);
  }
{% endhighlight %}

Now, we can invoke the method by adding another [map](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#map-org.apache.samza.operators.functions.MapFunction-) operation to the chain in [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/application/StreamApplication.html#init-org.apache.samza.application.StreamGraph-org.apache.samza.config.Config-). The operator chain should now look like this:
{% highlight java %}
allWikipediaEvents.map(WikipediaParser::parseEvent)
        .window(Windows.tumblingWindow(Duration.ofSeconds(10), WikipediaStats::new, new WikipediaStatsAggregator()))
        .map(this::formatOutput);
{% endhighlight %}

Next we need to get the output stream to which we will send the stats. Insert the following line below the creation of the 3 input streams:
{% highlight java %}
  OutputStream<WikipediaStatsOutput> wikipediaStats =
     graph.getOutputStream("wikipedia-stats", new JsonSerdeV2<>(WikipediaStatsOutput.class));
{% endhighlight %}

The [OutputStream](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/OutputStream.html) is parameterized by the type of the output.

The first parameter of [getOutputStream](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/StreamGraph.html#getOutputStream-java.lang.String-org.apache.samza.serializers.Serde-) is the output stream ID. We will use _wikipedia-stats_ and since it contains no special characters, we won't bother configuring a physical name so Samza will use the stream ID as the topic name.
The second parameter is the [Serde](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/serializers/Serde.html) to serialize the outgoing message. We will set it to [JsonSerdeV2](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/serializers/JsonSerdeV2.html) to serialize our `WikipediaStatsOutput` as a JSON string.

Finally, we can send our output to the output stream using the [sendTo](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#sendTo-org.apache.samza.operators.OutputStream-) operator:
{% highlight java %}
allWikipediaEvents.map(WikipediaParser::parseEvent)
        .window(Windows.tumblingWindow(Duration.ofSeconds(10), WikipediaStats::new, new WikipediaStatsAggregator()))
        .map(this::formatOutput)
        .sendTo(wikipediaStats);
{% endhighlight %}

Tip: Because the MessageStream type information is preserved in the operator chain, it is often easier to define the OutputStream inline with the [sendTo](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/MessageStream.html#sendTo-org.apache.samza.operators.OutputStream-) operator and then refactor it for readability. That way you don't have to hunt down the types.

#### KVStore
We now have an operational Wikipedia application which provides stats aggregated over a 10 second interval. One of those stats is a count of the number of edits within the 10s window. But what if we want to keep an additional durable counter of the total edits?

We will do this by keeping a separate count outside the window and persisting it in a [KeyValueStore](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/storage/kv/KeyValueStore.html).

We start by defining the store in the config file:
{% highlight bash %}
serializers.registry.string.class=org.apache.samza.serializers.StringSerdeFactory
serializers.registry.integer.class=org.apache.samza.serializers.IntegerSerdeFactory

stores.wikipedia-stats.factory=org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory
stores.wikipedia-stats.changelog=kafka.wikipedia-stats-changelog
stores.wikipedia-stats.key.serde=string
stores.wikipedia-stats.msg.serde=integer
{% endhighlight %}

These properties declare a [RocksDB](http://rocksdb.org/) key-value store named "wikipedia-stats". The store is replicated to a changelog stream called "wikipedia-stats-changelog" on the _kafka_ system for durability. It uses the _string_ and _integer_ serdes for keys and values respectively.

Next, we add a total count member variable to the `WikipediaStats` class, and to the `WikipediaStatsOutput` class:
{% highlight java %}
int totalEdits = 0;
{% endhighlight %}

To use the store in the application, we need to get it from the [TaskContext](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/task/TaskContext.html). Also, since we want to emit the total edit count along with the window edit count, it's easiest to update both of them in our aggregator. Declare the store as a member variable of the `WikipediaStatsAggregator` class:
{% highlight java %}
private KeyValueStore<String, Integer> store;
{% endhighlight %}

Then override the [init](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/operators/functions/InitableFunction.html#init-org.apache.samza.config.Config-org.apache.samza.task.TaskContext-) method in `WikipediaStatsAggregator` to initialize the store.
{% highlight java %}
@Override
public void init(Config config, TaskContext context) {
  store = (KeyValueStore<String, Integer>) context.getStore("wikipedia-stats");
}
{% endhighlight %}

Update and persist the counter in the `apply` method.
{% highlight java %}
Integer editsAllTime = store.get("count-edits-all-time");
if (editsAllTime == null) editsAllTime = 0;
editsAllTime++;
store.put("count-edits-all-time", editsAllTime);
stats.totalEdits = editsAllTime;
{% endhighlight %}

Finally, update the `MyWikipediaApplication#formatOutput` method to include the total counter in its `WikipediaStatsOutput`.

#### Metrics
Lastly, let's add a metric to the application which counts the number of repeat edits each topic within the window interval.

As with the key-value store, we must first define the metrics reporters in the config file.
{% highlight bash %}
metrics.reporters=snapshot,jmx
metrics.reporter.snapshot.class=org.apache.samza.metrics.reporter.MetricsSnapshotReporterFactory
metrics.reporter.snapshot.stream=kafka.metrics
metrics.reporter.jmx.class=org.apache.samza.metrics.reporter.JmxReporterFactory
{% endhighlight %}

The above properties define 2 metrics reporters. The first emits metrics to a _metrics_ topic on the _kafka_ system. The second reporter emits metrics to JMX.

In the WikipediaStatsAggregator, declare a counter member variable.
{% highlight java %}
private Counter repeatEdits;
{% endhighlight %}

Then add the following to the `WikipediaStatsAggregator#init` method to initialize the counter.
{% highlight java %}
repeatEdits = context.getMetricsRegistry().newCounter("edit-counters", "repeat-edits");
{% endhighlight %}

Update and persist the counter from the `apply` method.
{% highlight java %}
boolean newTitle = stats.titles.add((String) edit.get("title"));

if (!newTitle) {
  repeatEdits.inc();
  log.info("Frequent edits for title: {}", edit.get("title"));
}
{% endhighlight %}

#### Run and View Plan
You can set up the grid and run the application using the same instructions from the [hello samza high level API Yarn tutorial] (hello-samza-high-level-yarn.html). The only difference is to replace the `wikipedia-application.properties` config file in the _config-path_ command line parameter with `my-wikipedia-application.properties`

### Summary
Congratulations! You have built and executed a Wikipedia stream application on Samza using the high level API. The final application should be directly comparable to the pre-existing `WikipediaApplication` in the project.

You can provide feedback on this tutorial in the [dev mailing list](mailto:dev@samza.apache.org).
