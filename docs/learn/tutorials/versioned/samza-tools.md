---
layout: page
title: How to use Samza tools
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


# Get Samza tools

Please visit the [Download page] (/startup/download) to download the Samza tools package

{% highlight bash %}
tar -xvzf samza-tools-*.tgz
cd samza-tools-<version>
{% endhighlight %}


# Using Samza tools


## Generate kafka events


Generate kafka events tool is used to insert avro serialized events into kafka topics. Right now it can insert two types of events [PageViewEvent](https://github.com/apache/samza/blob/master/samza-tools/src/main/java/org/com/linkedin/samza/tools/schemas/PageViewEvent.avsc) and [ProfileChangeEvent](https://github.com/apache/samza/blob/master/samza-tools/src/main/java/org/com/linkedin/samza/tools/schemas/ProfileChangeEvent.avsc)

Before you can generate kafka events, Please follow instructions [here](http://kafka.apache.org/quickstart) to start the zookeeper and kafka server on your local machine.

You can follow below instructions on how to use Generate kafka events tool.

{% highlight bash %}

# Usage of the tool

./scripts/generate-kafka-events.sh
usage: Error: Missing required options: t, e
              generate-kafka-events.sh
 -b,--broker <BROKER>               Kafka broker endpoint Default (localhost:9092).
 -n,--numEvents <NUM_EVENTS>        Number of events to be produced, 
                                    Default - Produces events continuously every second.
 -p,--partitions <NUM_PARTITIONS>   Number of partitions in the topic,
                                    Default (4).
 -t,--topic <TOPIC_NAME>            Name of the topic to write events to.
 -e,--eventtype <EVENT_TYPE>        Type of the event values can be (PageView|ProfileChange). 


# Example command to generate 100 events of type PageViewEvent into topic named PageViewStream

 ./scripts/generate-kafka-events.sh -t PageViewStream -e PageView -n 100


# Example command to generate ProfileChange events continuously into topic named ProfileChangeStream

 ./scripts/generate-kafka-events.sh -t ProfileChangeStream -e ProfileChange 

{% endhighlight %}

## Samza SQL console tool

Once you generated the events into the kafka topic. Now you can use samza-sql-console tool to perform processing on the events published into the kafka topic.

There are two ways to use the tool -

1. You can either pass the sql statement directly as an argument to the tool. 
2. You can write the sql statement(s) into a file and pass the sql file as an argument to the tool.

Second option allows you to execute multiple sql statements, whereas the first one lets you execute one at a time.

Samza SQL needs all the events in the topic to be uniform schema. And it also needs access to the schema corresponding to the events in a topic. Typically in an organization, there is a deployment of schema registry which maps topics to schemas. 

In the absence of schema registry, Samza SQL console tool uses the convention to identify the schemas associated with the topic. If the topic name has string "page" it assumes the topic has PageViewEvents else ProfileChangeEvents. 

{% highlight bash %}

# Usage of the tool

 ./scripts/samza-sql-console.sh
usage: Error: One of the (f or s) options needs to be set
              samza-sql-console.sh
 -f,--file <SQL_FILE>   Path to the SQL file to execute.
 -s,--sql <SQL_STMT>    SQL statement to execute.

# Example command to filter out all the users who have moved to LinkedIn

./scripts/samza-sql-console.sh --sql "Insert into log.consoleOutput select Name, OldCompany from kafka.ProfileChangeStream where NewCompany = 'LinkedIn'"

{% endhighlight %}

You can run below sql commands using Samza sql console. Please make sure you are running generate-kafka-events tool to generate events into ProfileChangeStream before running the below command.

{% highlight bash %}
./scripts/samza-sql-console.sh --sql "Insert into log.consoleOutput select Name, OldCompany from kafka.ProfileChangeStream where NewCompany = 'LinkedIn'"

{% endhighlight %}
