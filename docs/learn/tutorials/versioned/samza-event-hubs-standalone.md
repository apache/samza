---
layout: page
title: Samza Event Hubs Connectors Example
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
The [hello-samza](https://github.com/apache/samza-hello-samza) project has an example that uses the Samza High Level Streams API to consume and produce from [Event Hubs](../../documentation/versioned/connectors/eventhubs.html) using the Zookeeper deployment model.

#### Get the Code

Let's get started by cloning the hello-samza project

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
{% endhighlight %}

The project comes up with numerous examples and for this tutorial, we will pick the Azure Event Hubs demo application.

#### Setting up the Deployment Environment

For our Azure application, we require [ZooKeeper](http://zookeeper.apache.org/). The hello-samza project comes with a script called "grid" to help with the environment setup

{% highlight bash %}
./bin/grid standalone
{% endhighlight %}

This command will download, install, and start ZooKeeper and Kafka. It will also check out the latest version of Samza and build it. All package files will be put in a sub-directory called "deploy" inside hello-samza's root folder.

If you get a complaint that JAVA_HOME is not set, then you'll need to set it to the path where Java is installed on your system.


#### Configuring the Samza Application

Here are the [Event Hubs descriptors](../../documentation/versioned/connectors/eventhubs.html) you must set before building the project.
Configure these in the `src/main/java/samza/examples/AzureApplication.java` file.

{% highlight java %}
 1  public void describe(StreamApplicationDescriptor appDescriptor) {
 2  // Define your system here
 3  EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");
 4  
 5  // Choose your serializer/deserializer for the EventData payload
 6  StringSerde serde = new StringSerde();
 7  
 8  // Define the input and output descriptors with respective descriptors
 9  EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
10    systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
11        .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
12        .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);
13  
14  EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
15    systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, serde)
16        .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
17        .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);
18  
19  // Define the input and output streams with descriptors
20  MessageStream<KV<String, String>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
21  OutputStream<KV<String, String>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);
22  
23  //...
24  }
{% endhighlight %}

In the code snippet above, we create the input and output streams that can consume and produce from the configured Event Hubs entities.

1. Line 3: A `EventHubsSystemDescriptor` is created with the name "eventhubs". You may set different system descriptors here. 
2. Line 6: Event Hubs messages are consumed as key value pairs. The [serde](../../documentation/versioned/container/serialization.html) is defined for the value of the payload of the Event Hubs' EventData. You may use any of the serdes that samza ships with out of the box or define your own.
The serde for the key is not set since it will always the String from the EventData [partitionKey](https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.eventhubs._event_data._system_properties.getpartitionkey?view=azure-java-stable#com_microsoft_azure_eventhubs__event_data__system_properties_getPartitionKey__).
3. Line 9-17: An `EventHubsInputDescriptor` and an `EventHubsOutputDescriptor` are created with the required descriptors to gain access of the Event Hubs entity (`STREAM_ID`, `EVENTHUBS_NAMESPACE`, `EVENTHUBS_ENTITY`, `EVENTHUBS_SAS_KEY_NAME`, `EVENTHUBS_SAS_KEY_TOKEN`).
These must be set to the credentials of the entities you wish to connect to.
4. Line 10-21: creates an `InputStream` and `OutputStream` with the previously defined `EventHubsInputDescriptor` and `EventHubsOutputDescriptor`, respectively.

Alternatively, you can set these properties in the `src/main/config/azure-application-local-runner.properties` file.
Note: the keys set in the `.properties` file will override the ones set in code with descriptors.
Refer to the [Event Hubs configuration reference](../../documentation/versioned/jobs/samza-configurations.html#eventhubs) for the complete list of configurations.

#### Building the Hello Samza Project

With the environment setup complete, let's move on to building the hello-samza project. Execute the following command:

{% highlight bash %}
./bin/deploy.sh
{% endhighlight %}

We are now all set to run the application locally.

#### Running the Azure application

In order to run the application, we will use the *run-azure-application* script.

{% highlight bash %}
./deploy/samza/bin/run-event-hubs-zk-application.sh
{% endhighlight %}

The above command executes the helper script which invokes the *AzureZKLocalApplication* main class, which starts the *AzureApplication*. This application prints out the messages from the input stream to `stdout` and send them the output stream.

The messages consumed should be printed in the following format:
{% highlight bash %}
Sending: 
Received Key: <KEY>
Received Message: <VALUE>
{% endhighlight %}

#### Shutdown

This application can be shutdown by terminating the *run-azure-application* script.
We can use the *grid* script to tear down the local environment ([Kafka](http://kafka.apache.org/) and [Zookeeper](http://zookeeper.apache.org/)).

{% highlight bash %}
./bin/grid stop all
{% endhighlight %}