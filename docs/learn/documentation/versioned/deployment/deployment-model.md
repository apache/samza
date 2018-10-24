---
layout: page
title: Deployment options
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

### Overview
A unique thing about Samza is that it provides multiple ways to run your applications. Each deployment model comes with its own benefits, so you have flexibility in choosing whichever fits your needs. Since Samza supports “Write Once, Run Anywhere”, your application logic does not change depending on where you deploy it.

### Running Samza on YARN
Samza integrates with [Apache YARN](learn/documentation/{{site.version}}/deployment/yarn.html) for running stream-processing as a managed service. We leverage YARN for isolation, multi-tenancy, resource-management and deployment for your applications. In this mode, you write your Samza application and submit it to be scheduled on a YARN cluster. You also specify its resource requirement - the number of containers needed, number of cores and memory required per-container. Samza then works with YARN to provision resources for your application and run it across a cluster of machines. It also handles failures of individual instances and automatically restarts them.

When multiple applications share the same YARN cluster, they need to be isolated from each other. For this purpose, Samza works with YARN to enforce cpu and memory limits. Any application that uses more than its requested share of memory or cpu is automatically terminated - thereby, allowing multi-tenancy. Just like you would for any YARN-based application, you can use YARN's [web UI](/learn/documentation/{{site.version}}/deployment/yarn.html#application-master-ui) to manage your Samza jobs, view their logs etc.

### Running Samza in standalone mode

Often you want to embed and integrate Samza as a component within a larger application. To enable this, Samza supports a [standalone mode](learn/documentation/{{site.version}}/deployment/standalone.html) of deployment allowing greater control over your application's life-cycle. In this model, Samza can be used just like any library you import within your Java application. This is identical to using a [high-level Kafka consumer](https://kafka.apache.org/) to process your streams.

You can increase your application's capacity by spinning up multiple instances. These instances will then dynamically coordinate with each other and distribute work among themselves. If an instance fails for some reason, the tasks running on it will be re-assigned to the remaining ones. By default, Samza uses [Zookeeper](https://zookeeper.apache.org/) for coordination across individual instances. The coordination logic by itself is pluggable and hence, can integrate with other frameworks.

This mode allows you to bring any cluster-manager or hosting-environment of your choice(eg: [Kubernetes](https://kubernetes.io/), [Marathon](https://mesosphere.github.io/marathon/)) to run your application. You are also free to control memory-limits, multi-tenancy on your own - since Samza is used as a light-weight library.

### Choosing a deployment model

A common question that we get asked is - "Should I use YARN or standalone?". Here are some guidelines when choosing your deployment model. Since your application logic does not change, it is quite easy to port from one to the other.

* Would you like Samza to be embedded as a component of a larger application?
    * If so, then you should use standalone.
* Would you like to have out-of-the-box resource management (e.g. CPU/memory limits, restarts on failures)?
    * If so, then you should use YARN.
* Would you like to run your application on any other cluster manager - eg: Kubernetes?
    * If so, then you should use standalone.
* Would you like to run centrally-managed tools and dashboards?
    * If so, then you should use YARN.
    * Note: You can still have tools and dashboards when using standalone, but you will need to run them yourself wherever your application is deployed.