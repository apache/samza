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
The [hello-samza](https://github.com/apache/samza-hello-samza) project contains an example of a high level job that consumes and produces to [Event Hubs](../../documentation/versioned/connectors/eventhubs.html) using the Zookeeper deployment model.

#### Get the Code

Let's get started by cloning the hello-samza project

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
{% endhighlight %}

The project comes up with numerous examples and for this tutorial, we will pick the Azure application.

#### Setting up the Deployment Environment

For our Azure application, we require [ZooKeeper](http://zookeeper.apache.org/). The hello-samza project comes with a script called "grid" to help with the environment setup

{% highlight bash %}
./bin/grid standalone
{% endhighlight %}

This command will download, install, and start ZooKeeper and Kafka. It will also check out the latest version of Samza and build it. All package files will be put in a sub-directory called "deploy" inside hello-samza's root folder.

If you get a complaint that JAVA_HOME is not set, then you'll need to set it to the path where Java is installed on your system.


#### Configuring the Azure application

Here are the [Event Hubs configs](../../documentation/versioned/connectors/eventhubs.html) you must set before building the project. 
Configure these in the `src/main/java/samza/examples/AzureApplication.java` file.

{% highlight java %}
@Override
public void describe(StreamApplicationDescriptor appDescriptor) {
  // Define your system here
  EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");

  // Choose your serializer/deserializer for the EventData payload
  StringSerde serde = new StringSerde();

  // Define the input and output descriptors with respective configs
  EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
      systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
          .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
          .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);

  EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
      systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, serde)
          .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
          .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);

  // Define the input and output streams with descriptors
  MessageStream<KV<String, String>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
  OutputStream<KV<String, String>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);

  //...
}
{% endhighlight %}

Alternatively, you man set these properties in in the `src/main/config/azure-application-local-runner.properties` file.
Note: the keys set in the `.properties` file will override the ones set in code with Descriptors.
{% highlight jproperties %}
# define an event hub system factory with your identifier. eg: eh-system
systems.eh-system.samza.factory=org.apache.samza.system.eventhub.EventHubSystemFactory

# define your streams
systems.eh-system.stream.list=input0, output0

# define required properties for your streams
systems.eh-system.streams.input0.eventhubs.namespace=YOUR-STREAM-NAMESPACE
systems.eh-system.streams.input0.eventhubs.entitypath=YOUR-ENTITY-NAME
systems.eh-system.streams.input0.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
systems.eh-system.streams.input0.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN

systems.eh-system.streams.output0.eventhubs.namespace=YOUR-STREAM-NAMESPACE
systems.eh-system.streams.output0.eventhubs.entitypath=YOUR-ENTITY-NAME
systems.eh-system.streams.output0.eventhubs.sas.keyname=YOUR-SAS-KEY-NAME
systems.eh-system.streams.output0.eventhubs.sas.token=YOUR-SAS-KEY-TOKEN
{% endhighlight %}

Refer to the [Event Hubs configuration reference](../../documentation/versioned/jobs/samza-configurations.html#eventhubs) for the complete list of configurations.

{% highlight java %}

{% endhighlight %}

#### Building the Hello Samza Project

With the environment setup complete, let us move on to building the hello-samza project. Execute the following command:

{% highlight bash %}
./bin/deploy.sh
{% endhighlight %}

We are now all set to run the application locally.

#### Running the Azure application

In order to run the application, we will use the *run-azure-application* script.

{% highlight bash %}
./deploy/samza/bin/run-azure-application.sh
{% endhighlight %}

The above command executes the helper script which invokes the *AzureZKLocalApplication* main class, which starts the *AzureApplication*. This application filters out the messages consumed without keys, prints them out and send them the configured output stream.

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