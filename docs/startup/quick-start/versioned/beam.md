---
layout: page
title: Beam on Samza Quick Start
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

[Apache Beam](https://beam.apache.org/) is an open-source SDK which provides state-of-the-art data processing API and model for both batch and streaming processing pipelines across multiple languages, i.e. Java, Python and Go. By collaborating with Beam, Samza offers the capability of executing Beam API on Samza's large-scale and stateful streaming engine. Current Samza supports the full Beam [Java API](https://beam.apache.org/documentation/runners/capability-matrix/), and the support of Python and Go is work-in-progress.

### Setting up the Word-Count Project

To get started, you need to install [Java 8 SDK]() as well as [Apache Maven](http://maven.apache.org/download.cgi). After that, the easiest way to get a copy of the WordCount examples in Beam API is to use the following command to generate a simple Maven project:

{% highlight bash %}
> mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.11.0 \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false
{% endhighlight %}

This command creates a maven project `word-count-beam` which contains a series of example pipelines that count words in text files:

{% highlight bash %}
> cd word-count-beam/

> ls src/main/java/org/apache/beam/examples/
DebuggingWordCount.java	WindowedWordCount.java	common
MinimalWordCount.java	WordCount.java
{% endhighlight %}

Let's use the MinimalWordCount example to demonstrate how to create a simple Beam pipeline:

{% highlight java %}
public class MinimalWordCount {

  public static void main(String[] args) {
    // Create the Pipeline object with the options we defined above
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    // This example reads a public data set consisting of the complete works of Shakespeare.
    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Count.perElement())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(TextIO.write().to("wordcounts"));

    p.run().waitUntilFinish();
  }
}
{% endhighlight %}

In this example, we first create a Beam `Pipeline` object to build the graph of transformations to be executed. Then we first use the Read transform to consume a public data set, and split into words. Then we use Beam build-in `Count` transform and returns the key/value pairs where each key represents a unique element from the input collection, and each value represents the number of times that key appeared in the input collection. Finally we format the results and write them to a file. A detailed walkthrough of the example code can be found [here](https://beam.apache.org/get-started/wordcount-example/).

Let's run the WordCount example with Samza using the following command:

{% highlight bash %}
>mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=/tmp/counts --runner=SamzaRunner" -Psamza-runner
{% endhighlight %}

After the pipeline finishes, you can check out the output counts files in /tmp folder. Note Beam generates multiple output files for parallel processing. If you prefer a single output, please update the code to use TextIO.write().withoutSharding().

{% highlight bash %}
>more /tmp/counts*
This: 1
When: 1
YARN: 1
apex: 2
apis: 2
beam: 43
beta: 1
code: 2
copy: 1
...
{% endhighlight %}

For more examples and how to deploy your job in local, standalone and Yarn cluster, you can look at the [code examples](/startup/code-examples/{{site.version}}/beam.html). Please don't hesitate to [reach out](https://samza.apache.org/community/contact-us.html) if you encounter any issues.