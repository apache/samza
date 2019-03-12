---
layout: page
title: Apache Beam API
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

### Table Of Contents
- [Introduction](#introduction)
- [Basic Concepts](#basic-concepts)
- [Apache Beam - A Samza’s Perspective](#apache-beam---a-samza’s-perspective)

### Introduction

Apache Beam brings an easy-to-usen but powerful API and model for state-of-art stream and batch data processing with portability across a variety of languages. The Beam API and model has the following characteristics:

- *Simple constructs, powerful semantics*: the whole beam API can be simply described by a `Pipeline` object, which captures all your data processing steps from input to output. Beam SDK supports over [20 data IOs](https://beam.apache.org/documentation/io/built-in/), and data transformations from simple [Map](https://beam.apache.org/releases/javadoc/2.11.0/org/apache/beam/sdk/transforms/MapElements.html) to complex [Combines and Joins](https://beam.apache.org/releases/javadoc/2.11.0/index.html?org/apache/beam/sdk/transforms/Combine.html).

- *Strong consistency via event-time*: Beam provides advanced [event-time support](https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data) so you can perform windowing and aggregations based on when the events happen, instead of when they arrive. The event-time mechanism improves the accuracy of processing results, and guarantees repeatability in results when reprocessing the same data set.

- *Comprehensive stream processing semantics*: Beam supports an up-to-date stream processing model, including [tumbling/sliding/session windows](https://beam.apache.org/documentation/programming-guide/#windowing), joins and aggregations. It provides [triggers](https://beam.apache.org/documentation/programming-guide/#triggers) based on conditions of early and late firings, and late arrival handling with accumulation mode and allowed lateness.

- *Portability with multiple programming languages*: Beam supports a consistent API in multiple languages, including [Java, Python and Go](https://beam.apache.org/roadmap/portability/). This allows you to leverage the rich ecosystem built for different languages, e.g. ML libs for Python.

### Basic Concepts

Let's walk through the WordCount example to illustrate the Beam basic concepts. A Beam program often starts by creating a [Pipeline](https://beam.apache.org/releases/javadoc/2.11.0/org/apache/beam/sdk/Pipeline.html) object in your `main()` function.

{% highlight java %}

// Start by defining the options for the pipeline.
PipelineOptions options = PipelineOptionsFactory.create();

// Then create the pipeline.
Pipeline p = Pipeline.create(options);

{% endhighlight %}

The `PipelineOptions` supported by SamzaRunner is documented in detail [here](https://beam.apache.org/documentation/runners/samza/).

Let's apply the first data transform to read from a text file using [TextIO.read()](https://beam.apache.org/releases/javadoc/2.11.0/org/apache/beam/sdk/io/TextIO.html):

{% highlight java %}

PCollection<String> lines = p.apply(
  "ReadLines", TextIO.read().from("/path/to/inputData"));

{% endhighlight %}

To break down each line into words, you can use a [FlatMap](https://beam.apache.org/releases/javadoc/2.11.0/org/apache/beam/sdk/transforms/FlatMapElements.html):

{% highlight java %}

PCollection<String> words = lines.apply(
    FlatMapElements.into(TypeDescriptors.strings())
        .via((String word) -> Arrays.asList(word.split("\\W+"))));

{% endhighlight %}

Beam provides a build-in transform [Count.perElement](https://beam.apache.org/releases/javadoc/2.11.0/org/apache/beam/sdk/transforms/Count.html) to count the number of elements based on each value. Let's use it here to count the words:

{% highlight java %}

PCollection<KV<String, Long>> counts = pipeline.apply(Count.perElement());

{% endhighlight %}

Finally we format the counts into strings and write to a file using `TextIO.write()`:

{% highlight java %}

counts.apply(ToString.kvs())
      .apply(TextIO.write().to("/path/to/output").withoutSharding());

{% endhighlight %}

To run your pipeline and wait for the results, just do:

{% highlight java %}

pipeline.run().waitUntilFinish();

{% endhighlight %}

Or you can run your pipeline asynchronously, e.g. when you submit it to a remote cluster:

{% highlight java %}

pipeline.run();

{% endhighlight %}

To run this Beam program with Samza, you can simply provide "--runner=SamzaRunner" as a program argument. You can follow our [quick start](/startup/quick-start/{{site.version}}/beam.html) to set up your project and run different examples. For more details on writing the Beam program, please refer the [Beam programming guide](https://beam.apache.org/documentation/programming-guide/).

### Apache Beam - A Samza’s Perspective

The goal of Samza is to provide large-scale streaming processing capabilities with first-class state support. This does not contradict with Beam. In fact, while Samza lays out a solid foundation for large-scale stateful stream processing, Beam adds the cutting-edge stream processing API and model on top of it. The Beam API and model allows further optimization in the Samza platform, including multi-stage distributed computation and parallel processing on the per-key basis. The performance enhancements from these optimizations will benefit both Samza and its users. Samza can also further improve Beam model by providing various use cases. We firmly believe Samza will benefit from collaborating with Beam.