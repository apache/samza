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
### Introduction
Samza provides multiple programming APIs to fit your use case:

1. Java APIs: Samza's provides two Java programming APIs that are ideal for building advanced Stream Processing applications. 
    1. [High Level Streams API](high-level-api.md): Samza's flexible High Level Streams API lets you describe your complex stream processing pipeline in the form of a Directional Acyclic Graph (DAG) of operations on message streams. It provides a rich set of built-in operators that simplify common stream processing operations such as filtering, projection, repartitioning, joins, and windows.
    2. [Low Level Task API](low-level-api.md): Samza's powerful Low Level Task API lets you write your application in terms of processing logic for each incoming message. 
2. [Samza SQL](samza-sql.md): Samza SQL provides a declarative query language for describing your stream processing logic. It lets you manipulate streams using SQL predicates and UDFs instead of working with the physical implementation details.
3. Apache Beam API: Samza also provides a [Apache Beam runner](https://beam.apache.org/documentation/runners/capability-matrix/) to run applications written using the Apache Beam API. This is considered as an extension to the operators supported by the High Level Streams API in Samza.


### Key Concepts
The following sections will talk about key concepts in writing your Samza applications in Java.

#### Samza Applications
A [SamzaApplication](javadocs/org/apache/samza/application/SamzaApplication.html) describes the inputs, outputs, state, configuration and the logic for processing data from one or more streaming sources. 

You can implement a 
[StreamApplication](javadocs/org/apache/samza/application/StreamApplication.html) and use the provided [StreamApplicationDescriptor](javadocs/org/apache/samza/application/descriptors/StreamApplicationDescriptor) to describe the processing logic using Samza's High Level Streams API in terms of [MessageStream](javadocs/org/apache/samza/operators/MessageStream.html) operators. 

{% highlight java %}

    public class MyStreamApplication implements StreamApplication {
        @Override
        public void describe(StreamApplicationDescriptor appDesc) {
            // Describe your application here 
        }
    }

{% endhighlight %}

Alternatively, you can implement a [TaskApplication](javadocs/org/apache/samza/application/TaskApplication.html) and use the provided [TaskApplicationDescriptor](javadocs/org/apache/samza/application/descriptors/TaskApplicationDescriptor) to describe it using Samza's Low Level API in terms of per-message processing logic.


- For TaskApplication:

{% highlight java %}
    
    public class MyTaskApplication implements TaskApplication {
        @Override
        public void describe(TaskApplicationDescriptor appDesc) {
            // Describe your application here
        }
    }

{% endhighlight %}


#### Streams and Table Descriptors
Descriptors let you specify the properties of various aspects of your application from within it. 

[InputDescriptor](javadocs/org/apache/samza/system/descriptors/InputDescriptor.html)s and [OutputDescriptor](javadocs/org/apache/samza/system/descriptors/OutputDescriptor.html)s can be used for specifying Samza and implementation-specific properties of the streaming inputs and outputs for your application. You can obtain InputDescriptors and OutputDescriptors using a [SystemDescriptor](javadocs/org/apache/samza/system/descriptors/SystemDescriptor.html) for your system. This SystemDescriptor can be used for specify Samza and implementation-specific properties of the producer and consumers for your I/O system. Most Samza system implementations come with their own SystemDescriptors, but if one isn't available, you 
can use the [GenericSystemDescriptor](javadocs/org/apache/samza/system/descriptors/GenericSystemDescriptor.html).

A [TableDescriptor](javadocs/org/apache/samza/table/descriptors/TableDescriptor.html) can be used for specifying Samza and implementation-specific properties of a [Table](javadocs/org/apache/samza/table/Table.html). You can use a Local TableDescriptor (e.g. [RocksDbTableDescriptor](javadocs/org/apache/samza/storage/kv/descriptors/RocksDbTableDescriptor.html) or a [RemoteTableDescriptor](javadocs/org/apache/samza/table/descriptors/RemoteTableDescriptor).


The following example illustrates how you can use input and output descriptors for a Kafka system, and a table descriptor for a local RocksDB table within your application:

{% highlight java %}
    
    public class MyStreamApplication implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDescriptor) {
        KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("kafka")
            .withConsumerZkConnect(ImmutableList.of("..."))
            .withProducerBootstrapServers(ImmutableList.of("...", "..."));
        KafkaInputDescriptor<PageView> kid = 
            ksd.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        KafkaOutputDescriptor<DecoratedPageView> kod = 
            ksd.getOutputDescriptor(“decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));

        RocksDbTableDescriptor<String, Integer> td = 
            new RocksDbTableDescriptor(“viewCounts”, KVSerde.of(new StringSerde(), new IntegerSerde()));
            
        // Implement your processing logic here
      }
    }
    
{% endhighlight %}

The same code in the above describe method applies to TaskApplication as well.

#### Stream Processing Logic

Samza provides two sets of APIs to define the main stream processing logic, High Level Streams API and Low Level Task API, via StreamApplication and TaskApplication, respectively. 

High Level Streams API allows you to describe the processing logic in a connected DAG of transformation operators, like the example below:

{% highlight java %}

    public class BadPageViewFilter implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        KafkaSystemDescriptor ksd = new KafkaSystemDescriptor();
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> pageViewOutput = kafka.getOutputDescriptor(“decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));
        RocksDbTableDescriptor<String, Integer> viewCountTable = new RocksDbTableDescriptor(
            “pageViewCountTable”, KVSerde.of(new StringSerde(), new IntegerSerde()));

        // Now, implement your main processing logic
        MessageStream<PageView> pageViews = appDesc.getInputStream(pageViewInput);
        pageViews.filter(this::isValidPageView)
             .map(this::addProfileInformation)
             .sendTo(pageViewOutput);
      }
    }
    
{% endhighlight %}

Low Level Task API allows you to describe the processing logic in a customized StreamTaskFactory or AsyncStreamTaskFactory, like the example below:

{% highlight java %}

    public class BadPageViewFilter implements TaskApplication {
      @Override
      public void describe(TaskApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafka = new KafkaSystemDescriptor();
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> pageViewOutput = kafka.getOutputDescriptor(“decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));
        RocksDbTableDescriptor<String, Integer> viewCountTable = new RocksDbTableDescriptor(
            “pageViewCountTable”, KVSerde.of(new StringSerde(), new IntegerSerde()));

        // Now, implement your main processing logic
        appDesc.withInputStream(pageViewInput)
           .withOutputStream(pageViewOutput)
           .withTaskFactory(new PageViewFilterTaskFactory());
      }
    }
    
{% endhighlight %}

#### Configuration for a Samza Application

To deploy a Samza application, you need to specify the implementation class for your application and the ApplicationRunner to launch your application. The following is an incomplete example of minimum required configuration to set up the Samza application and the runner. For additional configuration, see the Configuration Reference.

{% highlight jproperties %}
    
    # This is the class implementing StreamApplication
    app.class=com.example.samza.PageViewFilter

    # This is defining the ApplicationRunner class to launch the application
    app.runner.class=org.apache.samza.runtime.RemoteApplicationRunner
    
{% endhighlight %}