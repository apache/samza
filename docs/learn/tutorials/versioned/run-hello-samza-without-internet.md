---
layout: page
title: Run Hello Samza without Internet
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

This tutorial is to help you run [Hello Samza](../../../startup/hello-samza/{{site.version}}/) if you can not connect to the internet. 

### Test Your Connection

Ping irc.wikimedia.org. Sometimes the firewall in your company blocks this service.

{% highlight bash %}
telnet irc.wikimedia.org 6667
{% endhighlight %}

You should see something like this:

```
Trying 208.80.152.178...
Connected to ekrem.wikimedia.org.
Escape character is '^]'.
NOTICE AUTH :*** Processing connection to irc.pmtpa.wikimedia.org
NOTICE AUTH :*** Looking up your hostname...
NOTICE AUTH :*** Checking Ident
NOTICE AUTH :*** Found your hostname
```

Otherwise, you may have the connection problem.

### Use Local Data to Run Hello Samza

We provide an alternative to get wikipedia feed data. Instead of running

{% highlight bash %}
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
{% endhighlight %}

You will run

{% highlight bash %}
bin/produce-wikipedia-raw-data.sh
{% endhighlight %}

This script will read wikipedia feed data from local file and produce them to the Kafka broker. By default, it produces to localhost:9092 as the Kafka broker and uses localhost:2181 as zookeeper. You can overwrite them:

{% highlight bash %}
bin/produce-wikipedia-raw-data.sh -b yourKafkaBrokerAddress -z yourZookeeperAddress
{% endhighlight %}

Now you can go back to Generate Wikipedia Statistics section in [Hello Samza](../../../startup/hello-samza/{{site.version}}/) and follow the remaining steps.

### A Little Explanation

The goal of

{% highlight bash %}
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
{% endhighlight %}

is to deploy a Samza job which listens to wikipedia API, receives the feed in realtime and produces the feed to the Kafka topic wikipedia-raw. The alternative in this tutorial is reading local wikipedia feed in an infinite loop and producing the data to Kafka wikipedia-raw. The follow-up job, wikipedia-parser is getting data from Kafka topic wikipedia-raw, so as long as we have correct data in Kafka topic wikipedia-raw, we are fine. All Samza jobs are connected by the Kafka and do not depend on each other.


