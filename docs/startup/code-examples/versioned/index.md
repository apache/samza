---
layout: page
title:
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


### Checking out our examples

The [hello-samza](https://github.com/apache/samza-hello-samza) project contains several examples to help you create your Samza applications. To checkout the hello-samza project:

{% highlight bash %}
> git clone https://git.apache.org/samza-hello-samza.git hello-samza
{% endhighlight %}

#### High-level API examples
[The Samza Cookbook](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/cookbook) contains various recipes using the Samza high-level API.
These include:

- The [Filter example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/FilterExample.java) demonstrates how to perform stateless operations on a stream. 

- The [Join example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/JoinExample.java) demonstrates how you can join a Kafka stream of page-views with a stream of ad-clicks

- The [Stream-Table Join example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/RemoteTableJoinExample.java) demonstrates how to use the Samza Table API. It joins a Kafka stream with a remote dataset accessed through a REST service.

- The [SessionWindow](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/SessionWindowExample.java) and [TumblingWindow](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/TumblingWindowExample.java) examples illustrate Samza's rich windowing and triggering capabilities.


In addition to the cookbook, you can also consult these:

- [Wikipedia Parser](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/wikipedia): An advanced example that builds a streaming pipeline consuming a live-feed of wikipedia edits, parsing each message and generating statistics from them.


- [Amazon Kinesis](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/kinesis) and [Azure Eventhubs](https://github.com/apache/samza-hello-samza/tree/latest/src/main/java/samza/examples/azure) examples that cover how to consume input data from the respective systems.

#### Low-level API examples
The [Wikipedia Parser (low-level API)](https://github.com/apache/samza-hello-samza/tree/latest/src/main/java/samza/examples/wikipedia/task/application): 
Same example that builds a streaming pipeline consuming a live-feed of 
wikipedia edits, parsing each message and generating statistics from them, but
using low-level APIs. 

#### Samza SQL API examples
You can easily create a Samza job declaratively using 
[Samza SQL](https://samza.apache.org/learn/tutorials/0.14/samza-sql.html).

#### Apache Beam API examples

The easiest way to get a copy of the WordCount examples in Beam API is to use [Apache Maven](http://maven.apache.org/download.cgi). After installing Maven, please run the following command:

{% highlight bash %}
> mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.9.0 \
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

To use SamzaRunner, please add the following `samza-runner` profile to `pom.xml` under the "profiles" section, same as in [here](https://github.com/apache/beam/blob/master/sdks/java/maven-archetypes/examples/src/main/resources/archetype-resources/pom.xml).

{% highlight xml %}
    ...
    <profile>
      <id>samza-runner</id>
      <dependencies>
        <dependency>
          <groupId>org.apache.beam</groupId>
          <artifactId>beam-runners-samza</artifactId>
          <version>${beam.version}</version>
          <scope>runtime</scope>
        </dependency>
      </dependencies>
    </profile>
    ....
{% endhighlight %}

Now we can run the wordcount example with Samza using the following command:

{% highlight bash %}
>mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=/tmp/counts --runner=SamzaRunner" -Psamza-runner
{% endhighlight %}

After the pipeline finishes, you can check out the output counts files in /tmp folder. Note Beam generates multiple output files for parallel processing. If you prefer a single output, please update the code to use TextIO.write().withoutSharding().

{% highlight bash %}
>more /tmp/counts*
AS: 1
IO: 2
IS: 1
OF: 1
...
{% endhighlight %}

A walkthrough of the example code can be found [here](https://beam.apache.org/get-started/wordcount-example/). Feel free to play with other examples in the project or write your own. Please don't hesitate to [reach out](https://samza.apache.org/community/contact-us.html) if you encounter any issues.
