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

# Overview
One unique thing about Samza is that it provides multiple ways to deploy an application. Each deployment model comes with its own benefits, so you have flexibility in being able to choose which model best fits your needs. Samza supports “write once, run anywhere”, so application logic is the same regardless of the deployment model that you choose.

## YARN
Apache YARN is a technology that manages resources, deploys applications, and monitors applications for a cluster of machines. Samza submits an application to YARN, and YARN assigns resources from across the cluster to that application. Multiple applications can run on a single YARN cluster.

* Provides central cluster management
* Each application has an associated application master in YARN to coordinate processing containers
* Enforces CPU and memory limits
* Supports multi-tenancy for applications
* A Samza application is run directly as its own set of processes
* Automatically restarts containers that have failed
* Provides centrally managed tools and dashboards

## Standalone

In standalone mode, a Samza application is a library embedded into another application (similar to Kafka Streams). This means that an application owner can control the full lifecycle of the application. Samza will do the coordination between processing containers to ensure that processing is balanced and failures are handled.

* Application owner is free to control cluster management, CPU and memory limits, and multi-tenancy
* Container coordination is done by Zookeeper out of the box, and container coordination can be extended to be done by a technology other than Zookeeper
* If containers fail, then partitions will be rebalanced across remaining containers
* Samza logic can run within the same process as non-Samza logic
* Application owner can run tools and dashboards wherever the application is deployed

# Choosing a deployment model

Here are some guidelines when choosing your deployment model.

* Would you like your Samza application to be embedded as a component of a larger application?
    * If so, then you should use standalone.
* Would you like to have out-of-the-box resource management (e.g. CPU/memory limits, restarts on failures)?
    * If so, then you should use YARN.
* Would you like to have the freedom to deploy and run your application anywhere?
    * If so, then you should use standalone.
* Would you like to run centrally-managed tools and dashboards?
    * If so, then you should use YARN.
    * Note: You can still have tools and dashboards when using standalone, but you will need to run them yourself wherever you have actually deployed your application.
