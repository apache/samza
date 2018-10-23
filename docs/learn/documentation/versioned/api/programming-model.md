---
layout: page
title: Programming Model
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
# Introduction
Samza provides different sets of programming APIs to meet requirements from different sets of users. The APIs are listed below:

1. Java programming APIs: Samza provides Java programming APIs for users who are familiar with imperative programming languages. The overall programming model to create a Samza application in Java will be described here. Samza also provides two sets of APIs to describe user processing logic:
    1. [High-level API](high-level-api.md): this API allows users to describe the end-to-end stream processing pipeline in a connected DAG (Directional Acyclic Graph). It also provides a rich set of build-in operators to help users implementing common transformation logic, such as filter, map, join, and window.
    2. [Task API](low-level-api.md): this is low-level Java API which provides “bare-metal” programming interfaces to the users. Task API allows users to explicitly access physical implementation details in the system, such as accessing the physical system stream partition of an incoming message and explicitly controlling the thread pool to execute asynchronous processing method.
2. [Samza SQL](samza-sql.md): Samza provides SQL for users who are familiar with declarative query languages, which allows the users to focus on data manipulation via SQL predicates and UDFs, not the physical implementation details.
3. Beam API: Samza also provides a [Beam runner](https://beam.apache.org/documentation/runners/capability-matrix/) to run applications written in Beam API. This is considered as an extension to existing operators supported by the high-level API in Samza.

The following sections will be focused on Java programming APIs.

# Key Concepts for a Samza Java Application
To write a Samza Java application, you will typically follow the steps below:
1. Define your input and output streams and tables
2. Define your main processing logic

The following sections will talk about key concepts in writing your Samza applications in Java.

## Samza Applications
When writing your stream processing application using Java API in Samza, you implement either a [StreamApplication](javadocs/org/apache/samza/application/StreamApplication.html) or [TaskApplication](javadocs/org/apache/samza/application/TaskApplication.html) and define your processing logic in the describe method.
- For StreamApplication:

{% highlight java %}
    
    public void describe(StreamApplicationDescriptor appDesc) { … }

{% endhighlight %}
- For TaskApplication:

{% highlight java %}
    
    public void describe(TaskApplicationDescriptor appDesc) { … }

{% endhighlight %}

## Descriptors for Data Streams and Tables
There are three different types of descriptors in Samza: [InputDescriptor](javadocs/org/apache/samza/system/descriptors/InputDescriptor.html), [OutputDescriptor](javadocs/org/apache/samza/system/descriptors/OutputDescriptor.html), and [TableDescriptor](javadocs/org/apache/samza/table/descriptors/TableDescriptor.html). The InputDescriptor and OutputDescriptor are used to describe the physical sources and destinations of a stream, while a TableDescriptor is used to describe the physical dataset and IO functions for a table.
Usually, you will obtain InputDescriptor and OutputDescriptor from a [SystemDescriptor](javadocs/org/apache/samza/system/descriptors/SystemDescriptor.html), which include all information about producer and consumers to a physical system. The following code snippet illustrate how you will obtain InputDescriptor and OutputDescriptor from a SystemDescriptor.

{% highlight java %}
    
    public class BadPageViewFilter implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafka = new KafkaSystemDescriptor();
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> pageViewOutput = kafka.getOutputDescriptor(“decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));

        // Now, implement your main processing logic
      }
    }
    
{% endhighlight %}

You can also add a TableDescriptor to your application.

{% highlight java %}
     
    public class BadPageViewFilter implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafka = new KafkaSystemDescriptor();
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> pageViewOutput = kafka.getOutputDescriptor(“decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));
        TableDescriptor<String, Integer> viewCountTable = new RocksDBTableDescriptor(
            “pageViewCountTable”, KVSerde.of(new StringSerde(), new IntegerSerde()));

        // Now, implement your main processing logic
      }
    }
    
{% endhighlight %}

The same code in the above describe method applies to TaskApplication as well.

## Stream Processing Logic

Samza provides two sets of APIs to define the main stream processing logic, high-level API and Task API, via StreamApplication and TaskApplication, respectively. 

High-level API allows you to describe the processing logic in a connected DAG of transformation operators, like the example below:

{% highlight java %}

    public class BadPageViewFilter implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafka = new KafkaSystemDescriptor();
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> pageViewOutput = kafka.getOutputDescriptor(“decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));
        TableDescriptor<String, Integer> viewCountTable = new RocksDBTableDescriptor(
            “pageViewCountTable”, KVSerde.of(new StringSerde(), new IntegerSerde()));

        // Now, implement your main processing logic
        MessageStream<PageView> pageViews = appDesc.getInputStream(pageViewInput);
        pageViews.filter(this::isValidPageView)
             .map(this::addProfileInformation)
             .sendTo(pageViewOutput);
      }
    }
    
{% endhighlight %}

Task API allows you to describe the processing logic in a customized StreamTaskFactory or AsyncStreamTaskFactory, like the example below:

{% highlight java %}

    public class BadPageViewFilter implements TaskApplication {
      @Override
      public void describe(TaskApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafka = new KafkaSystemDescriptor();
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> pageViewOutput = kafka.getOutputDescriptor(“decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));
        TableDescriptor<String, Integer> viewCountTable = new RocksDBTableDescriptor(
            “pageViewCountTable”, KVSerde.of(new StringSerde(), new IntegerSerde()));

        // Now, implement your main processing logic
        appDesc.withInputStream(pageViewInput)
           .withOutputStream(pageViewOutput)
           .withTaskFactory(new PageViewFilterTaskFactory());
      }
    }
    
{% endhighlight %}

Details for [high-level API](high-level-api.md) and [Task API](low-level-api.md) are explained later.

## Configuration for a Samza Application

To deploy a Samza application, you will need to specify the implementation class for your application and the ApplicationRunner to launch your application. The following is an incomplete example of minimum required configuration to set up the Samza application and the runner:
{% highlight jproperties %}
    
    # This is the class implementing StreamApplication
    app.class=com.example.samza.PageViewFilter

    # This is defining the ApplicationRunner class to launch the application
    app.runner.class=org.apache.samza.runtime.RemoteApplicationRunner
    
{% endhighlight %}