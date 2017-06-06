---
layout: page
title: Hello Samza High Level API - YARN Deployment
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
The [hello-samza](https://github.com/apache/samza-hello-samza) project is an example project designed to help you run your first Samza application. It has examples of applications using the low level task API as well as the high level API.

This tutorial demonstrates a simple wikipedia application created with the high level API. The [Hello Samza tutorial] (/startup/hello-samza/{{site.version}}/index.html) is the low-level analog to this tutorial. It demonstrates the same logic but is created with the task API. The tutorials are designed to be as similar as possible. The primary differences are that with the high level API we accomplish the equivalent of 3 separate low-level jobs with a single application, we skip the intermediate topics for simplicity, and we can visualize the execution plan after we start the application.

### Get the Code

Check out the hello-samza project:

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
{% endhighlight %}

This project contains everything you'll need to run your first Samza application.

### Start a Grid

A Samza grid usually comprises three different systems: [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), [Kafka](http://kafka.apache.org/), and [ZooKeeper](http://zookeeper.apache.org/). The hello-samza project comes with a script called "grid" to help you setup these systems. Start by running:

{% highlight bash %}
./bin/grid bootstrap
{% endhighlight %}

This command will download, install, and start ZooKeeper, Kafka, and YARN. It will also check out the latest version of Samza and build it. All package files will be put in a sub-directory called "deploy" inside hello-samza's root folder.

If you get a complaint that JAVA_HOME is not set, then you'll need to set it to the path where Java is installed on your system.

Once the grid command completes, you can verify that YARN is up and running by going to [http://localhost:8088](http://localhost:8088). This is the YARN UI.

### Build a Samza Application Package

Before you can run a Samza application, you need to build a package for it. This package is what YARN uses to deploy your apps on the grid.

NOTE: if you are building from the latest branch of hello-samza project, make sure that you run the following step from your local Samza project first:

{% highlight bash %}
./gradlew publishToMavenLocal
{% endhighlight %}

Then, you can continue w/ the following command in hello-samza project:

{% highlight bash %}
mvn clean package
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-0.13.1-SNAPSHOT-dist.tar.gz -C deploy/samza
{% endhighlight %}

### Run a Samza Application

After you've built your Samza package, you can start the app on the grid using the run-app.sh script.

{% highlight bash %}
./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-application.properties
{% endhighlight %}

The app will do all of the following:

1. Consume 3 feeds of real-time edits from Wikipedia
3. Parse the events to extract information about the size of the edit, who made the change, etc.
4. Calculate counts, every ten seconds, for all edits that were made during that window 
5. Output the counts to the wikipedia-stats topic

For details about how the app works, take a look at the [code walkthrough](hello-samza-high-level-code.html).

Give the job a minute to startup, and then tail the Kafka topic:

{% highlight bash %}
./deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-stats
{% endhighlight %}

The messages in the stats topic look like this:

{% highlight json %}
{"is-talk":2,"bytes-added":5276,"edits":13,"unique-titles":13}
{"is-bot-edit":1,"is-talk":3,"bytes-added":4211,"edits":30,"unique-titles":30,"is-unpatrolled":1,"is-new":2,"is-minor":7}
{"bytes-added":3180,"edits":19,"unique-titles":19,"is-unpatrolled":1,"is-new":1,"is-minor":3}
{"bytes-added":2218,"edits":18,"unique-titles":18,"is-unpatrolled":2,"is-new":2,"is-minor":3}
{% endhighlight %}

Pretty neat, right? Now, check out the YARN UI again ([http://localhost:8088](http://localhost:8088)). This time around, you'll see your Samza job is running!

### View the Execution Plan
Each application goes through an execution planner and you can visualize the execution plan after starting the job by opening the following file in a browser
{% highlight bash %}
deploy/samza/bin/plan.html
{% endhighlight %}

This plan will make more sense after the [code walkthrough](hello-samza-high-level-code.html). For now, just take note that this visualization is available and it is useful for visibility into the structure of the application. For this tutorial, the plan should look something like this:

<img src="/img/{{site.version}}/learn/tutorials/hello-samza-high-level/wikipedia-execution-plan.png" alt="Execution plan" style="max-width: 100%; height: auto;" onclick="window.open(this.src)"/>


### Shutdown

To shutdown the app, use the same _run-app.sh_ script with an extra _--operation=kill_ argument
{% highlight bash %}
./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-application.properties --operation=kill
{% endhighlight %}

After you're done, you can clean everything up using the same grid script.

{% highlight bash %}
./bin/grid stop all
{% endhighlight %}

Congratulations! You've now setup a local grid that includes YARN, Kafka, and ZooKeeper, and run a Samza application on it. Curious how this application was built? See the [code walk-through](hello-samza-high-level-code.html).
