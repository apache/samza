---
layout: page
title: Deployment model
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
One unique thing about Samza is that it provides multiple ways to deploy an application. Each deployment model comes with its own benefits, so you have flexibility in choosing which model best fits your needs. Samza supports “write once, run anywhere”, so application logic is the same regardless of where you choose to deploy your application.

### Running Samza on YARN
Samza integrates with Apache YARN for running stream-processing as a managed service. Samza leverages YARN for multi-tenancy, resource-management, isolation and deployment for your applications. In this mode, you write your Samza application and submit it to be scheduled on a YARN cluster. You also specify its resource requirements - the number of containers needed, number of cores and memory per-container. Samza then works with YARN to provision resources for your application and run it across a cluster of machines. It also handles failures of individual instances and restarts them.

When multiple applications share the same YARN cluster, they need to be isolated from each other. For this purpose, Samza works with YARN to enforce cpu and memory limits. Any application that uses more than its requested share of memory or cpu is terminated - thereby, enabling multi-tenancy. Just like you would for any YARN-based application, you can use YARN's web UI to manage your Samza jobs, view their logs etc.

### Running Samza in standalone mode

Often you want to embed Samza as a component in a larger application. To enable this, Samza supports a standalone mode of operation. In this mode, Samza can be used like a library within your application. This is very similar to Kafka Streams and offers greater control over the application life-cycle. You can increase capacity by spinning up multiple instances. The instances will dynamically coordinate among themselves to distribute work. If any instance fails, the tasks running on it will be re-assigned to the remaining ones. By default, Samza uses Zookeeper for coordination across instances. The coordination logic by itself is pluggable.

This mode allows you to run Samza with any cluster-manager of your choice - including Kubernetes, Marathon or on any hosting environment. You are free to control memory-limits, multi-tenancy for your application on your own - since Samza now acts as a light-weight library used by your application. 

### Choosing a deployment model

A common question that we get asked is - "Where should I run my Samza application?". Here are some guidelines when choosing your deployment model. Since your application logic does not change, it is easy to port from one deployment model to the other.

* Would you like Samza to be embedded as a component of a larger application?
    * If so, then you should use standalone.
* Would you like to have out-of-the-box resource management (e.g. CPU/memory limits, restarts on failures)?
    * If so, then you should use YARN.
* Would you like to run your application on any other cluster manager - eg: Kubernetes?
    * If so, then you should use standalone.
* Would you like to run centrally-managed tools and dashboards?
    * If so, then you should use YARN.
    * Note: You can still have tools and dashboards when using standalone, but you will need to run them yourself wherever your application is deployed.
