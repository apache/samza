---
layout: page
title: Hello Samza High Level API - Zookeeper Deployment
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

The [hello-samza](https://github.com/apache/samza-hello-samza) project is designed to get started with your first Samza job.
In this tutorial, we will learn how to run a Samza application using zookeeper deployment model.

### Get the Code

Let's get started by cloning the hello-samza project

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
{% endhighlight %}

The project comes up with numerous examples and for this tutorial, we will pick the Wikipedia application.

### Setting up the Deployment Environment

For our Wikipedia application, we require two systems: [Kafka](http://kafka.apache.org/) and [ZooKeeper](http://zookeeper.apache.org/). The hello-samza project comes with a script called "grid" to help with the environment setup

{% highlight bash %}
./bin/grid standalone
{% endhighlight %}

This command will download, install, and start ZooKeeper and Kafka. It will also check out the latest version of Samza and build it. All package files will be put in a sub-directory called "deploy" inside hello-samza's root folder.

If you get a complaint that JAVA_HOME is not set, then you'll need to set it to the path where Java is installed on your system.

### Building the Hello Samza Project

NOTE: if you are building from the latest branch of hello-samza project and want to use your local copy of samza, make sure that you run the following step from your local Samza project first

{% highlight bash %}
./gradlew publishToMavenLocal
{% endhighlight %}

With the environment setup complete, let us move on to building the hello-samza project. Execute the following commands:

{% highlight bash %}
mvn clean package
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-0.13.0-SNAPSHOT-dist.tar.gz -C deploy/samza
{% endhighlight %}

We are now all set to deploy the application locally.

### Running the Wikipedia application

In order to run the application, we will use the *run-wikipedia-zk-application* script.

{% highlight bash %}
./deploy/samza/bin/run-wikipedia-zk-application.sh
{% endhighlight %}

The above command executes the helper script which invokes the *WikipediaZkLocalApplication* main class with the appropriate job configurations as command line arguments. The main class is an application wrapper
that initializes the application and passes it to the local runner for execution. It is blocking and waits for the *LocalApplicationRunner* to finish.

To run your own application using zookeeper deployment model, you would need something similar to *WikipediaZkLocalApplication* class that initializes your application
and uses the *LocalApplicationRunner* to run it. To learn more about the internals checkout [deployment-models](/startup/preview/) documentation and the [configurations](/learn/documentation/{{site.version}}/jobs/configuration-table.html) table.

Getting back to our example, the application consumes a feed of real-time edits from Wikipedia, and produces them to a Kafka topic called "wikipedia-stats". Give the job a minute to startup, and then tail the Kafka topic. To do so, run the following command:

{% highlight bash %}
./deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-stats
{% endhighlight %}

The messages in the stats topic should look like the sample below:

{% highlight json %}
{"is-talk":2,"bytes-added":5276,"edits":13,"unique-titles":13}
{"is-bot-edit":1,"is-talk":3,"bytes-added":4211,"edits":30,"unique-titles":30,"is-unpatrolled":1,"is-new":2,"is-minor":7}
{"bytes-added":3180,"edits":19,"unique-titles":19,"is-unpatrolled":1,"is-new":1,"is-minor":3}
{"bytes-added":2218,"edits":18,"unique-titles":18,"is-unpatrolled":2,"is-new":2,"is-minor":3}
{% endhighlight %}

Excellent! Now that the job is running, open the *plan.html* file under *deploy/samza/bin* directory to take a look at the execution plan for the Wikipedia application.
The execution plan is a colorful graphic representing various stages of your application and how they are connected. Here is a sample plan visualization:

<img src="/img/{{site.version}}/learn/tutorials/hello-samza-high-level/wikipedia-execution-plan.png" alt="Execution plan" style="max-width: 100%; height: auto;" onclick="window.open(this.src)"/>


### Shutdown

The Wikipedia application can be shutdown by terminating the *run-wikipedia-zk-application* script.
We can use the *grid* script to tear down the local environment ([Kafka](http://kafka.apache.org/) and [Zookeeper](http://zookeeper.apache.org/)).

{% highlight bash %}
bin/grid stop all
{% endhighlight %}

Congratulations! You've now successfully run a Samza application using zookeeper deployment model. Next up, check out the [deployment-models](/startup/preview/) and [high level API](/startup/preview.html) pages.
