---
layout: page
title: Feature Preview
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

# Overview
Samza 0.13.0 includes a preview of a new programming model and a new deployment model. Both are functional and usable but not quite ready for production applications yet. They're being released as a preview because they represent major enhancements to how developers work with Samza, so it's beneficial for both early adopters and the Samza development community to experiment with it and gather feedback. The following sections introduce the new features and link to tutorials which demonstrate each of them.

# Fluent API

### Introduction
TODO

### Try it Out
Let's jump straight in. There are two tutorials prepared to help you get acquainted with running Samza applications and programming with the fluent API:

* [Hello Samza Fluent](/learn/tutorials/{{site.version}}/hello-samza-fluent.html) - run a pre-built wikipedia application and observe the output
* [Hello Samza Fluent Code](/learn/tutorials/{{site.version}}/hello-samza-fluent-code.html) - walk through building the wikipedia application, step by step.

### Fluent API Guide
This page shows you how to run a Samza stream application with fluent API under different environments.

<img src="/img/{{site.version}}/learn/documentation/introduction/fluent-arch.png" alt="Fluent architecture diagram" style="max-width: 100%; height: auto;" onclick="window.open(this.src)">

Above diagram shows an overview of Apache Samza architecture with Fluent API. There are four layers in the architecture:

#### I. Fluent API

Samza fluent API provides a unified way to handle both streaming and batch data. It provides operators like map, filter, window and join to allow the user to describe the whole end-to-end data processing in a single program. It can consume data from various sources and publish to different sinks.

#### II. ApplicationRunner

During run time, Samza uses [ApplicationRunner]((javadocs/org/apache/samza/runtime/ApplicationRunner.html)) to execute a stream application. The ApplicationRunner generates the configs such as input/output streams, creates intermediate streams, and starts the execution. There are two types of ApplicationRunner:

* RemoteApplicationRunner - submit the application to a remote cluster which runs it in remote JVMs. This runner is invoked via _run-app.sh_ script. 
* LocalApplicationRunner - runs the application in the local JVM processes. This runner is directly invoked by the users in their application’s main() method.

To use RemoteApplicationRunner, config the following property with your [StreamApplication](javadocs/org/apache/samza/application/StreamApplication) class:

{% highlight jproperties %}
# The StreamApplication class to run
app.class=Your.StreamApplication.Class
{% endhighlight %}

Then you can use _run-app.sh_ to run the application in remote cluster as described in tutorial [here](/learn/tutorials/{{site.version}}/hello-samza-fluent.html).

To use LocalApplicationRunner, you can run it with the StreamApplication in your program. The following shows an example of how to run it in main():

{% highlight java %}
public static void main(String[] args) throws Exception {
 CommandLine cmdLine = new CommandLine();
 Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
 // Create the new LocalApplicationRunner instance
 LocalApplicationRunner localRunner = new LocalApplicationRunner(config);
 // Run your StreamApplication in local JVM
 StreamApplication app = new YourStreamApplication();
 localRunner.run(app);

 // Wait for the application to finish
 localRunner.waitForFinish();
 System.out.println("Application completed with status " + localRunner.status(app));
}
{% endhighlight %}

When the ApplicationRunner runs the StreamApplication, it first generates an execution plan. To visualize this plan, Samza provides _plan.html_ with javascripts to render the plan json. All the files needed for visualization are in the samza-shell tgz. Once the job is deployed, the plan can be viewed as follows:

* For the applications using _run-app.sh_, Samza will automatically create a _plan_ folder under your application deployment directory and write the _plan.json_ file there.
* For the applications using your own script (this is mostly for LocalApplicationRunner), please create a _plan_ folder under the same directory as _bin_, and export an env var named **EXECUTION_PLAN_DIR** which is the _plan_ folder path.

To view the plan, you can simply open the _bin/plan.html file in a browser. The visualization of a sample plan looks like the following:

<img src="/img/{{site.version}}/learn/documentation/introduction/execution-plan.png" alt="Execution plan" style="max-width: 100%; height: auto;" onclick="window.open(this.src)"/>

#### III. Deployment

Samza supports two types of deployment models: remote deployment and local deployment. In the remote deployment, a cluster will run Samza application in distributed containers and manage their life cycle. Right now Samza only support Yarn cluster deployment.

In the local deployment, the users can use Samza as a library and run stream processing in their program with any kind of the clusters, like Amazaon EC2 or Mesos. For this deployment, Samza can be configured to use two kinds of coordinator among the cluster:

* Zookeeper - Samza uses Zookeeper to manage group membership and partition assignment. This allows the users to scale in run time by adding more processors.
* Standalone - Samza can run the application in a single JVM locally without coordination, or multiple JVMs using the user-configured task-to-container and partition-to-task groupers. This supports static user-defined partition assignment.

To use Zookeeper-based coordination, the following configs are required:

{% highlight jproperties %}
job.coordinator.factory=org.apache.samza.zk.ZkJobCoordinatorFactory
job.coordinator.zk.connect=yourzkconnection
{% endhighlight %}

To use standalone coordination, the following configs are needed:

{% highlight jproperties %}
job.coordinator.factory=org.apache.samza.standalone.StandaloneJobCoordinatorFactory
{% endhighlight %}

For more details of local deployment using Zookeeper, please take a look at this [tutorial](/learn/tutorials/{{site.version}}/hello-samza-standalone.html).

#### IV. Processor

The finest execution unit of a Samza application is the StreamProcessor, which runs stream processing in a single thread. It reads the configs generated from the ApplicationRunner, and consumes the input stream partitions assigned by the JobCoordinator. It can access local state data in either RocksDb or memory, and access remote data efficiently using multithreading.
