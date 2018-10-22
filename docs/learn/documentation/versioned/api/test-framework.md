---
layout: page
title: Testing Samza jobs: Integration Framework
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

# What is Samza's Integration Test Framework ?

Samza provides an Integration framework which allows you to test applications by quickly running them against a few messages and asserting on expected results. This alleviates the need to set up dependencies like Kafka, Yarn, Zookeeper to test your Samza applications

# Some Prerequisite Information
1.  Your Samza job will be executed in single container mode and framework will set all the required configs for you to run your job (more on configs later)
2.  Your Samza job will read from a special kind of bounded streams introduced in the next section, containing finite number of messages to make testing feasible.


# Key Concepts

## Introduction to In Memory System and Streams

1.  With Samza 1.0 we now get the feature of using streams that are maintained in memory using an in memory system.
2.  These in memory streams are described by InMemoryInputDescriptor, InMemoryOutputDescriptor and the corresponding system is described by InMemorySystemDescriptors
3.  These streams are like Kafka streams but there lifecycle is maintained in memory which means they get initialized with your job, are available throughout its run and are destroyed after the test ends . 



## Introduction to TestRunner api
1.  Samza 1.0 introduces a new TestRunner api to set up a test for Samza job, add configs, configure input/output streams, run the job in testing mode
2.  TestRunner also provides utilities to consume contents of a stream once the test has ran successfully
3.  TestRunner does basic config setup for you by default, you have flexibility to change these default configs if required


## How To Write Test

For example, here is a StreamApplication that validates and decorates page views with viewer’s profile information.

{% highlight java %}
    
    class BadPageViewFilterApplication implements StreamApplication {
        @Override
        public void describe(StreamApplicationDescriptor appDesc) { … }
    }
    
{% endhighlight %}

{% highlight java %}

    public class BadPageViewFilter implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        KafkaSystemDescriptor kafka = new KafkaSystemDescriptor("test");
        InputDescriptor<PageView> pageViewInput = kafka.getInputDescriptor(“page-views”, new JsonSerdeV2<>(PageView.class));
        OutputDescriptor<DecoratedPageView> outputPageViews = kafka.getOutputDescriptor( “decorated-page-views”, new JsonSerdeV2<>(DecoratedPageView.class));    
        MessageStream<PageView> pageViews = appDesc.getInputStream(pageViewInput);
        pageViews.filter(this::isValidPageView)
            .map(this::addProfileInformation)
            .sendTo(appDesc.getOutputStream(outputPageViews));
      }
    }
    
{% endhighlight %}


There are 4 simple steps to write a test for your stream processing logic and assert on the output

## Step 1: Construct an InMemorySystem
In the example we are writing we use a Kafka system called "test", so we will configure an equivalent in memory system (name should be the same as used in job) as shown below:   

{% highlight java %}
    
    InMemorySystemDescriptor inmemory = new InMemorySystemDescriptor("test");

{% endhighlight %}

## Step 2:  Initialize your input and output streams
1.  TestRunner API uses a special kind of input and output streams called in memory streams which are easy to define and write assertions on.
2.  Data in these streams are maintained in memory hence they always use a NoOpSerde<>
3.  You need to configure all the stream that your job reads/writes to. 
4.  You can obtain handle of these streams from the system we initialized in previous step
5.  We have two choices when we configure a stream type 

Input Stream described by InMemoryInputDescriptor, these streams need to be initialized with messages (data), since your job reads this.

{% highlight java %}
     
     InMemoryInputDescriptor<PageView> pageViewInput = inmemory.getInputDescriptor(“page-views”, new NoOpSerde<>());

{% endhighlight %}

{% highlight jproperties %}

    INFO: Use the org.apache.samza.operators.KV as the message type ex: InMemoryInputDescriptor<KV<String,PageView>> as the message type
    to use key of the KV (String here) as key and value as message (PageView here) for the IncomingMessageEnvelope in samza job, using all the other data types will result in key of the the IncomingMessageEnvelope set to null 

{% endhighlight %}

Output Stream described by InMemoryOutputDescriptor with a partition count, these streams are empty and your job writes to these streams

{% highlight java %}

    InMemoryOutputDescriptor<DecoratedPageView> outputPageViews = inmemory.getOutputDescriptor("decorated-page-views", new NoOpSerde<>())

{% endhighlight %}

{% highlight jproperties %}

    Note: Input streams are immutable - ie., once they have been created you can't modify their contents eg: by adding new messages"All input streams are supposed to be bounded

{% endhighlight %}


## Step 3: Create a TestRunner 

1.  Initialize a TestRunner of your Samza job
2.  Configure TestRunner with input streams and mock data to it 
3.  Configure TestRunner with output streams with a partition count
4.  Add any configs if necessary
5.  Run the test runner

{% highlight java %}

    List<PageView> pageViews = generateData(...);
    TestRunner
       .of(pageViewRepartitionApp)
       .addInputStream(pageViewInput, pageViews)
       .addOutputStream(outputPageViews, 10)
       .run(Duration.ofMillis(1500));


{% endhighlight %}

{% highlight jproperties  %}

    Info: Use addConfig(Map<String, String> configs) TestRunner to add/modify any config in the TestRunner

{% endhighlight %}


## Step-4: Assert on the output stream

You have the following choices for asserting the results of your tests

1. You can use StreamAssert utils on your In Memory Streams to do consumption of all partitions

{% highlight java %}
    
    // Consume multi-paritioned stream
    Map<Integer, PageView> expOutput;
    StreamAssert.containsInOrder(imod, expectedOutput, Duration.ofMillis(1000));
    // Consume single paritioned stream
    StreamAssert.containsInOrder(outputPageViews, Arrays.asList(...), Duration.ofMillis(1000));

{% endhighlight %}
   
2. You have the flexibility to define your custom assertions using api TestRunner.consumeStream() to assert on any partitions of the stream

{% highlight java %}

    Assert.assertEquals(
        TestRunner.consumeStream(outputPageViews,Duration.ofMillis(1000)).get(0).size(),1
       );

{% endhighlight %}

Complete Glance at the code

{% highlight java %}

    @Test
    public void testStreamDSLApi() throws Exception {
     // Generate Mock Data
     List<PageView> pageViews = genrateMockInput(...);
     List<DecoratedPageView> expectedOutput = genrateMockOutput(...);
    
     // Configure System and Stream Descriptors
     InMemorySystemDescriptor inmemory = new InMemorySystemDescriptor("test");
     InMemoryInputDescriptor<PageView> pageViewInput = inmemory
        .getInputDescriptor(“page-views”, new NoOpSerde<>());
     InMemoryOutputDescriptor<DecoratedPageView> outputPageView = inmemory
        .getOutputDescriptor(“decorated-page-views”, new NoOpSerde<>())
     
     // Configure the TestRunner 
     TestRunner
         .of(pageViewRepartition)
         .addInputStream(pageViewInput, pageViews)
         .addOutputStream(outputPageView, 10)
         .run(Duration.ofMillis(1500));
    
     // Assert the results
     StreamAssert.containsInOrder(expectedOutput, outputPageView, Duration.ofMillis(1000));
    }

{% endhighlight %}


### Example for Low Level Api:

For a Low Level Task API

{% highlight java %}

    public class BadPageViewFilter implements TaskApplication {
      @Override
      public void describe(TaskApplicationDescriptor appDesc) {
        // Add input, output streams and tables
        KafkaSystemDescriptor<String, PageViewEvent> kafkaSystem = 
            new KafkaSystemDescriptor(“kafka”)
              .withConsumerZkConnect(myZkServers)
              .withProducerBootstrapServers(myBrokers);
        KVSerde<String, PageViewEvent> serde = 
            KVSerde.of(new StringSerde(), new JsonSerdeV2<PageViewEvent>());
        // Add input, output streams and tables
        appDesc.withInputStream(kafkaSystem.getInputDescriptor(“pageViewEvent”, serde))
            .withOutputStream(kafkaSystem.getOutputDescriptor(“goodPageViewEvent”, serde))
            .withTable(new RocksDBTableDescriptor(
                “badPageUrlTable”, KVSerde.of(new StringSerde(), new IntegerSerde())
            .withTaskFactory(new BadPageViewTaskFactory());
      }
    }
    
    public class BadPageViewTaskFactory implements StreamTaskFactory {
      @Override
      public StreamTask createInstance() {
        // Add input, output streams and tables
        return new BadPageViewFilterTask();
      }
    }
    
     public class BadPageViewFilterTask implements StreamTask {
       @Override
       public void process(IncomingMessageEnvelope envelope,
                           MessageCollector collector,
                           TaskCoordinator coordinator) {
            // process message synchronously
        }
     }   
     
     
       @Test
       public void testStatefulTaskWithLocalTable() {
         List<PageView> badPageViews = Arrays.asList(generatePageViews(..));
         List<Profile> expectedGoodPageViews = Arrays.asList(generatePageViews(..));
     
         InMemorySystemDescriptor inmemory = new InMemorySystemDescriptor("kafka");
     
         InMemoryInputDescriptor<PageView> pageViewInput = inmemory
             .getInputDescriptor("PageView", new NoOpSerde<TestTableData.PageView>());
     
         InMemoryOutputDescriptor<TestTableData.EnrichedPageView> pageViewOutput = inmemory
             .getOutputDescriptor("EnrichedPageView", new NoOpSerde<>());
     
         TestRunner
             .of(new JoinTaskApplication())
             .addInputStream(pageViewStreamDesc, pageViews)
             .addInputStream(profileStreamDesc, profiles)
             .addOutputStream(outputStreamDesc, 1)
             .run(Duration.ofSeconds(2));
     
         Assert.assertEquals(10, TestRunner.consumeStream(outputStreamDesc, Duration.ofSeconds(1)).get(0).size());
     }

{% endhighlight %}


Follow a similar approach for Legacy Low Level API as described above.

{% highlight java %}

      public class MultiplyByTenStreamTask implements StreamTask {
       @Override
       public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
           throws Exception {
         Integer obj = (Integer) envelope.getMessage();
         collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "output"),
             envelope.getKey(), envelope.getKey(), obj * 10));
       }
      }
       
      @Test
      public void testLowLevelApi() throws Exception {
       InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
       InMemoryInputDescriptor<PageView> imid = isd
          .getInputDescriptor("input", new NoOpSerde<>());
       InMemoryOutputDescriptor<PageView> imod = isd
         .getOutputDescriptor("output", new NoOpSerde<>())
        
       TestRunner
           .of(MultiplyByTenStreamTask.class)
           .addInputStream(inputDescriptor, Arrays.asList(1, 2, 3, 4, 5))
           .addOutputStream(outputDescriptor, 1)
           .run(Duration.ofSeconds(1));
      
      Map<String, List<Integer>> output = new Map<>();
      output.add(0, Arrays.asList(10, 20, 30, 40, 50));
      
      StreamAssert.containsInOrder(imod, output, Duration.ofMillis(1000));
      }


{% endhighlight %}