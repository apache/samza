---
layout: blog
title: Announcing the release of Apache Samza 1.4.0
icon: git-pull-request
authors:
    - name: Cameron Lee
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

# **Announcing the release of Apache Samza 1.4.0**


<!--more-->

**IMPORTANT NOTE**: We may introduce **backward incompatible changes regarding samza job submission** in the future 1.5 release. Details can be found on [SEP-23: Simplify Job Runner](https://cwiki.apache.org/confluence/display/SAMZA/SEP-23%3A+Simplify+Job+Runner)

We are thrilled to announce the release of Apache Samza 1.4.0.

Today, Samza forms the backbone of hundreds of real-time production applications across a multitude of companies, such as LinkedIn, Slack, and Redfin, among many others. Samza provides leading support for large-scale stateful stream processing with:

* First class support for local state (with RocksDB store). This allows a stateful application to scale up to 1.1 Million events/sec on a single machine with SSD.

* Support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.

* A fully asynchronous programming model that makes parallelizing remote calls efficient and effortless.

* High level API for expressing complex stream processing pipelines in a few lines of code.

* Beam Samza Runner that marries Beam’s best in class support for EventTime based windowed processing and sophisticated triggering with Samza’s stable and scalable stateful processing model.

* A fully pluggable model for input sources (e.g. Kafka, Kinesis, DynamoDB streams etc.) and output systems (HDFS, Kafka, ElastiCache etc.).

* A Table API that provides a common abstraction for accessing remote or local databases and allowing developers are able to “join” an input event stream with such a Table.

* Flexible deployment model for running the applications in any hosting environment and with cluster managers other than YARN.

### New Features, Upgrades and Bug Fixes:
This release brings the following features, upgrades, and capabilities (highlights):

* Improvements regarding management and monitoring of local state

* Improvements to the Samza SQL API

* New system producer for Azure blob storage

* Bug fixes

Full list of the jiras addressed in this release can be found [here](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20and%20fixVersion%20in%20(1.4)).

### Upgrading your application to Apache Samza 1.4.0
If an application is being upgraded to Samza 1.4, please note the following usage changes.

* The samza-autoscaling module is no longer supported, and the module has been removed.

### State
[SAMZA-2386](https://issues.apache.org/jira/browse/SAMZA-2386) Get store names should return correct store names in the presence of side inputs

[SAMZA-2324](https://issues.apache.org/jira/browse/SAMZA-2324) Adding KV store metrics for rocksdb

[SAMZA-2416](https://issues.apache.org/jira/browse/SAMZA-2416) Adding null-check before incrementing metrics for bytesSerialized

[SAMZA-2397](https://issues.apache.org/jira/browse/SAMZA-2397) Samza rocksdb metrics do not emit values after Samza version >= 1.1

[SAMZA-2447](https://issues.apache.org/jira/browse/SAMZA-2447) Checkpoint dir removal should only search in valid store dirs

### SQL
[SAMZA-2362](https://issues.apache.org/jira/browse/SAMZA-2362) Include the ScalarUDF implementations with the configured package prefix in ReflectionBasedUdfResolver.

[SAMZA-2375](https://issues.apache.org/jira/browse/SAMZA-2375) Samza-sql: Store udf original name for display purposes

[SAMZA-2376](https://issues.apache.org/jira/browse/SAMZA-2376) Samza-sql: Samza sql should handle sql statements with trailing semi-colon (;)

[SAMZA-2396](https://issues.apache.org/jira/browse/SAMZA-2396) Support dynamic addition of jars in ReflectionUdfResolver.

[SAMZA-2415](https://issues.apache.org/jira/browse/SAMZA-2415) Samza-Sql: Fix AvroRelConverter to only consider cached schema while populating SamzaSqlRelRecord for all the nested records.

[SAMZA-2425](https://issues.apache.org/jira/browse/SAMZA-2425) Samza-sql: support subquery in joins

[SAMZA-2455](https://issues.apache.org/jira/browse/SAMZA-2455) Validate the argument types in SamzaSQL UDF on execution planning phase

### Azure system producer
[SAMZA-2421](https://issues.apache.org/jira/browse/SAMZA-2421) Add SystemProducer for Azure Blob Storage

### Job coordinator dependency isolation (experimental)
[SAMZA-2332](https://issues.apache.org/jira/browse/SAMZA-2332) [AM isolation] YarnJob should pass new command and additional environment variables for AM deployment

[SAMZA-2333](https://issues.apache.org/jira/browse/SAMZA-2333) [AM isolation] Use cytodynamics classloader to launch job coordinator

### Bug fixes

[SAMZA-2334](https://issues.apache.org/jira/browse/SAMZA-2334) ProxyGrouper selection based on Host Affinity not whether job is stateful

[SAMZA-2372](https://issues.apache.org/jira/browse/SAMZA-2372) Null pointer exception in LocalApplicationRunner

[SAMZA-2443](https://issues.apache.org/jira/browse/SAMZA-2443) Upgrade Jetty version to prevent AM file descriptor leak

[SAMZA-2446](https://issues.apache.org/jira/browse/SAMZA-2446) Invoke onCheckpoint only for registered SSPs

[SAMZA-2463](https://issues.apache.org/jira/browse/SAMZA-2463) Duplicate firings of processing timers

[SAMZA-2461](https://issues.apache.org/jira/browse/SAMZA-2461) Fix Concurrent Modification Exception in InMemorySystem

### Other improvements
[SAMZA-2364](https://issues.apache.org/jira/browse/SAMZA-2364) Include the localized resource lib directory in the classpath of SamzaContainer

Clean up unused org.apache.samza.autoscaling module

[SAMZA-2444](https://issues.apache.org/jira/browse/SAMZA-2444) JobModel save in CoordinatorStreamStore resulting flush for each message

[SAMZA-2452](https://issues.apache.org/jira/browse/SAMZA-2452) Adding internal autosizing related configs

### Sources downloads
A source download of Samza 1.4.0 is available [here](https://dist.apache.org/repos/dist/release/samza/1.4.0/), and is also available in Apache’s Maven repository. See Samza’s download [page](https://samza.apache.org/startup/download/) for details and Samza’s feature preview for new features.
