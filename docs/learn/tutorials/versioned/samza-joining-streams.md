---
layout: page
title: Joining streams with Samza
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

The tutorial assumes you have successfully run [hello-samza](../../../startup/hello-samza/{{site.version}}/).
This tutorial represents stream-stream join use case implemented with key-value stores.
If you are not familiar with Samza's state management or a "stream join" term you should take a look into Samza's [State Management](../../documentation/{{site.version}}/container/state-management.html).
This tutorial follows the example described [here](../../documentation/{{site.version}}/container/state-management.html#stream-stream-join-example).

### Produce some ad events

Before producing ad impression and click events, we assume that you already got Hello Samza code, started Samza grid and built a Samza job package. If not, check [hello-samza's](../../../startup/hello-samza/{{site.version}}/) first three steps.
In this example we will rely on Kafka system. Before running the job you need to create kafka topics that you will be using.
We made a script that will create all required kafka topics and start producing raw ad impression and click events. It produces to localhost:9092 as the Kafka broker and uses localhost:2181 as zookeeper.
Raw ad impression and click events look like this:

{% highlight bash %}
impression-id=1 type=impression advertiser-id=1 ip=111.111.111.* agent=Chrome timestamp=2017-01-01T00:00:00.000
impression-id=1 type=click advertiser-id=1 ip=111.111.111.* agent=Chrome timestamp=2017-01-01T00:00:14.234
{% endhighlight %}

Make sure you navigate to the root hello-samza directory and run the script

{% highlight bash %}
bin/produce-ad-event-data.sh
{% endhighlight %}

### Run Samza jobs

Now that you are producing raw ad events, you need to partition them by their impression ID. To do so, run the ad-event-feed job.

{% highlight bash %}
bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/deploy/samza/config/ad-event-feed.properties
{% endhighlight %}

Now you have partitioned ad impressions and ad clicks into 4 partitions each. Second Samza job will consume them and build joined events that join raw events and calculate passed time between impression and click event.

{% highlight bash %}
bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/deploy/samza/config/ad-event-join.properties
{% endhighlight %}

### The result

Check out messages produced by jobs with

{% highlight bash %}
deploy/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ad-imp-metadata
deploy/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ad-clk-metadata
deploy/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ad-join
{% endhighlight %}

You can also produce events manually like this. For instance, following lines will produce one join event.

{% highlight bash %}
deploy/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ad-impression --property key.separator=, --property parse.key=true
11,impression-id=11 type=impression advertiser-id=1 ip=111.111.111.* agent=Chrome timestamp=2017-01-01T12:00:00.000
{% endhighlight %}
{% highlight bash %}
deploy/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ad-click --property key.separator=, --property parse.key=true
11,impression-id=11 type=click advertiser-id=1 ip=111.111.111.* agent=Chrome timestamp=2017-01-01T13:13:35.404
{% endhighlight %}

Congratulations! You have successfully run steam-stream join example implemented with Samza's key-value stores!