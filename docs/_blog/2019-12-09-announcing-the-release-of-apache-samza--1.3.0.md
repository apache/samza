---
layout: blog
title: Announcing the release of Apache Samza 1.3.0
icon: git-pull-request
authors:
    - name: Hai Lu
      website:
      image:
excerpt_separator: <!--more-->
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

# **Announcing the release of Apache Samza 1.3.0**


<!--more-->


We’re thrilled to announce the release of Apache Samza 1.3.0.

Today Samza forms the backbone of hundreds of real-time production applications across a multitude of companies, such as LinkedIn, VMWare, Slack, Redfin among many others. Samza provides leading support for large-scale stateful stream processing with:

* First class support for local state (with RocksDB store). This allows a stateful application to scale up to 1.1 Million events/sec on a single machine with SSD.

* Support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.

* A fully asynchronous programming model that makes parallelizing remote calls efficient and effortless.

* High level API for expressing complex stream processing pipelines in a few lines of code.

* Beam Samza Runner that marries Beam’s best in class support for EventTime based windowed processing and sophisticated triggering with Samza’s stable and scalable stateful processing model.

* A fully pluggable model for input sources (e.g. Kafka, Kinesis, DynamoDB streams etc.) and output systems (HDFS, Kafka, ElastiCache etc.).

* A Table API that provides a common abstraction for accessing remote or local databases and allowing developers are able to "join" an input event stream with such a Table.

* Flexible deployment model for running the applications in any hosting environment and with cluster managers other than YARN.

* Features like canaries, upgrades and rollbacks that support extremely large deployments with minimal downtime.

###  **New Features, Upgrades and Bug Fixes:**
This release brings the following features, upgrades, and capabilities (highlights):

* Startpoint support improvement

* Samza SQL improvement

* Table API improvement

* Miscellaneous bug fixes

Full list of the jiras addressed in this release can be found [here](https://issues.apache.org/jira/browse/SAMZA-2354?jql=project%20%3D%20%22SAMZA%22%20and%20fixVersion%20in%20(1.3)).
### **Upgrading your application to Apache Samza 1.3.0**

### Startpoint support improvement
[SAMZA-2201](https://issues.apache.org/jira/browse/SAMZA-2201) Startpoints - Integrate fan out with job coordinators

[SAMZA-2215](https://issues.apache.org/jira/browse/SAMZA-2215) StartpointManager fix for previous CoordinatorStreamStore refactor

[SAMZA-2220](https://issues.apache.org/jira/browse/SAMZA-2220) Startpoints - Fully encapsulate resolution of starting offsets in OffsetManager

### Samza SQL improvement
[SAMZA-2234](https://issues.apache.org/jira/browse/SAMZA-2234) Samza SQL : Provide access to Samza context to the Sama SQL UDFs

[SAMZA-2313](https://issues.apache.org/jira/browse/SAMZA-2313) Samza-sql: Add validation for Samza sql statements

[SAMZA-2354](https://issues.apache.org/jira/browse/SAMZA-2354) Improve UDF discovery in samza-sql

#### Table API improvement
[SAMZA-2191](https://issues.apache.org/jira/browse/SAMZA-2191) support batching for remote tables

[SAMZA-2200](https://issues.apache.org/jira/browse/SAMZA-2200) update table sendTo() and join() operation to accept additional arguments

[SAMZA-2219](https://issues.apache.org/jira/browse/SAMZA-2219) Add a dummy table read function

[SAMZA-2309](https://issues.apache.org/jira/browse/SAMZA-2309) Remote table descriptor requires read function

#### Miscellaneous bug fixing
[SAMZA-2198](https://issues.apache.org/jira/browse/SAMZA-2198) containers process always takes task.shutdown.ms to shut down

[SAMZA-2293](https://issues.apache.org/jira/browse/SAMZA-2293) Propagate the watermark future to StreamOperatorTask correctly

### Sources downloads
A source download of Samza 1.3.0 is available [here](https://dist.apache.org/repos/dist/release/samza/1.3.0/), and is also available in Apache’s Maven repository. See Samza’s download [page](https://samza.apache.org/startup/download/) for details and Samza’s feature preview for new features.

