---
layout: page
title: Hello Samza
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
The [hello-samza](https://github.com/apache/samza-hello-samza) project is a stand-alone project designed to help you run your first Samza job.

### Get the Code

Check out the hello-samza project:

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
{% endhighlight %}

This project contains everything you'll need to run your first Samza jobs.

### Start a Grid

A Samza grid usually comprises three different systems: [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), [Kafka](http://kafka.apache.org/), and [ZooKeeper](http://zookeeper.apache.org/). The hello-samza project comes with a script called "grid" to help you setup these systems. Start by running:

{% highlight bash %}
bin/grid bootstrap
{% endhighlight %}

This command will download, install, and start ZooKeeper, Kafka, and YARN. It will also check out the latest version of Samza and build it. All package files will be put in a sub-directory called "deploy" inside hello-samza's root folder.

If you get a complaint that JAVA_HOME is not set, then you'll need to set it to the path where Java is installed on your system.

Once the grid command completes, you can verify that YARN is up and running by going to [http://localhost:8088](http://localhost:8088). This is the YARN UI.

### Build a Samza Job Package

Before you can run a Samza job, you need to build a package for it. This package is what YARN uses to deploy your jobs on the grid.

NOTE: if you are building from the latest branch of hello-samza project, make sure that you run the following step from your local Samza project first:

{% highlight bash %}
./gradlew publishToMavenLocal
{% endhighlight %}

Then, you can continue w/ the following command in hello-samza project:

{% highlight bash %}
mvn clean package
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-0.10.0-dist.tar.gz -C deploy/samza
{% endhighlight %}

### Run a Samza Job

After you've built your Samza package, you can start a job on the grid using the run-job.sh script.

{% highlight bash %}
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
{% endhighlight %}

The job will consume a feed of real-time edits from Wikipedia, and produce them to a Kafka topic called "wikipedia-raw". Give the job a minute to startup, and then tail the Kafka topic:

{% highlight bash %}
deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-raw
{% endhighlight %}

Pretty neat, right? Now, check out the YARN UI again ([http://localhost:8088](http://localhost:8088)). This time around, you'll see your Samza job is running!

If you can not see any output from Kafka consumer, you may have connection problem. Check [here](../../../learn/tutorials/{{site.version}}/run-hello-samza-without-internet.html).

### Generate Wikipedia Statistics

Let's calculate some statistics based on the messages in the wikipedia-raw topic. Start two more jobs:

{% highlight bash %}
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-parser.properties
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-stats.properties
{% endhighlight %}

The first job (wikipedia-parser) parses the messages in wikipedia-raw, and extracts information about the size of the edit, who made the change, etc. You can take a look at its output with:

{% highlight bash %}
deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-edits
{% endhighlight %}

The last job (wikipedia-stats) reads messages from the wikipedia-edits topic, and calculates counts, every ten seconds, for all edits that were made during that window. It outputs these counts to the wikipedia-stats topic.

{% highlight bash %}
deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-stats
{% endhighlight %}

The messages in the stats topic look like this:

{% highlight json %}
{"is-talk":2,"bytes-added":5276,"edits":13,"unique-titles":13}
{"is-bot-edit":1,"is-talk":3,"bytes-added":4211,"edits":30,"unique-titles":30,"is-unpatrolled":1,"is-new":2,"is-minor":7}
{"bytes-added":3180,"edits":19,"unique-titles":19,"is-unpatrolled":1,"is-new":1,"is-minor":3}
{"bytes-added":2218,"edits":18,"unique-titles":18,"is-unpatrolled":2,"is-new":2,"is-minor":3}
{% endhighlight %}

If you check the YARN UI, again, you'll see that all three jobs are now listed.

### Shutdown

After you're done, you can clean everything up using the same grid script.

{% highlight bash %}
bin/grid stop all
{% endhighlight %}

Congratulations! You've now setup a local grid that includes YARN, Kafka, and ZooKeeper, and run a Samza job on it. Next up, check out the [Background](/learn/documentation/{{site.version}}/introduction/background.html) and [API Overview](/learn/documentation/{{site.version}}/api/overview.html) pages.
