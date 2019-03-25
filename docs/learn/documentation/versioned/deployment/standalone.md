---
layout: page
title: Run as embedded library
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

- [Introduction](#introduction)
- [Quick start](#quick-start-guide)
  - [Installing Zookeeper and Kafka](#setup-zookeeper)
  - [Building binaries](#build-binaries)
  - [Running the application](#deploy-binaries)
  - [Inspecting results](#inspect-results)
- [Coordinator internals](#coordinator-internals)


### Introduction

Often you want to embed and integrate Samza as a component within a larger application. To enable this, Samza supports a standalone mode of deployment allowing greater control over your application’s life-cycle. In this model, Samza can be used just like any library you import within your Java application. This is identical to using a high-level Kafka consumer to process your streams.

You can increase your application’s capacity by spinning up multiple instances. These instances will then dynamically coordinate with each other and distribute work among themselves. If an instance fails for some reason, the tasks running on it will be re-assigned to the remaining ones. By default, Samza uses Zookeeper for coordination across individual instances. The coordination logic by itself is pluggable and hence, can integrate with other frameworks.

This mode allows you to bring any cluster-manager or hosting-environment of your choice(eg: Kubernetes, Marathon) to run your application. You are also free to control memory-limits, multi-tenancy on your own - since Samza is used as a light-weight library.


### Quick start

The [Hello-samza](https://github.com/apache/samza-hello-samza/) project includes multiple examples of Samza standalone applications. Let us first check out the repository.

```bash
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza 
```


#### Installing Zookeeper and Kafka

We will use the `./bin/grid` script from the `hello-samza` project to setup up Zookeeper and Kafka locally.

```bash
./bin/grid start zookeeper
./bin/grid start kafka
```


#### Building the binaries

Let us now build the `hello-samza` project from its sources.

```bash
mvn clean package
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-1.1.0-dist.tar.gz -C deploy/samza
```

#### Running the application

We are ready to run the example application [WikipediaZkLocalApplication](https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/wikipedia/application/WikipediaZkLocalApplication.java). This application reads messages from the wikipedia-edits topic, and calculates counts, every ten seconds, for all edits that were made during that window. It emits these results to another topic named `wikipedia-stats`.

```bash
./deploy/samza/bin/run-class.sh samza.examples.wikipedia.application.WikipediaZkLocalApplication  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-application-local-runner.properties
```

You can run the above command again to spin up a new instance of your application.

#### Inspecting results

To inspect events in output topic, run the following command.

```bash
./deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-stats
```

You should see the output messages emitted to the Kafka topic.

```
{"is-talk":2,"bytes-added":5276,"edits":13,"unique-titles":13}
{"is-bot-edit":1,"is-talk":3,"bytes-added":4211,"edits":30,"unique-titles":30,"is-unpatrolled":1,"is-new":2,"is-minor":7}
```

### Standalone Coordinator Internals

Samza runs your application by logically breaking its execution down into multiple tasks. A task is the unit of parallelism for your application, with each task consumeing data from one or more partitions of your input streams.
Obviously, there is a limit to the throughput you can achieve with a single instance. You can increase parallelism by simply running multiple instances of your application. The individual instances can be executed on the same machine, or distributed across
machines. Likewise, to scale down your parallelism, you can shut down a few instances of your application. Samza will coordinate among available instances and dynamically assign the tasks them. The coordination logic itself is 
pluggable - with a Zookeeper-based implementation provided out of the box.

Here is a typical sequence of actions when using Zookeeper for coordination.

1. Everytime you spawn a new instance of your application, it registers itself with Zookeeper as a participant.

2. There is always a single leader - which acts as the coordinator. The coordinator which manages the assignment of tasks across the individual containers. The coordinator also monitors the liveness of individual containers and redistributes the tasks among the remaining ones during a failure.  

3. Whenever a new instance joins or leaves the group, it triggers a notification to the leader. The leader can then recompute assignments of tasks to the live instances.

4. Once the leader publishes new partition assignments, all running instances pick up the new assignment and resume processing.
