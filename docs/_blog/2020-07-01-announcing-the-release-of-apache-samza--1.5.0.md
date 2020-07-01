---
layout: blog
title: Announcing the release of Apache Samza 1.5.0
icon: git-pull-request
authors:
    - name: Bharath Kumarasubramanian
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

# **Announcing the release of Apache Samza 1.5.0**


<!--more-->

We are thrilled to announce the release of Apache Samza 1.5.0.

Today, Samza forms the backbone of hundreds of real-time production applications across a multitude of companies, such as LinkedIn, Slack, and Redfin, among many others. Samza provides leading support for large-scale stateful stream processing with:

* First class support for local states (with RocksDB store). This allows a stateful application to scale up to 1.1 Million events/sec on a single machine with SSD.

* Support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large states.

* A fully asynchronous programming model that makes parallelizing remote calls efficient and effortless.

* High level API for expressing complex stream processing pipelines in a few lines of code.

* Beam Samza Runner that marries Beam’s best in class support for EventTime based windowed processing and sophisticated triggering with Samza’s stable and scalable stateful processing model.

* A fully pluggable model for input sources (e.g. Kafka, Kinesis, DynamoDB streams etc.) and output systems (HDFS, Kafka, ElastiCache etc.).

* A Table API that provides a common abstraction for accessing remote or local databases and allows developers to “join” an input event stream with such a Table.

* Flexible deployment model for running the applications in any hosting environment and with cluster managers other than YARN.

### New Features, Upgrades and Bug Fixes:
This release brings the following features, upgrades, and capabilities (highlights):

#### Samza Container Placement
Container Placements API gives you the ability to move / restart one or more containers (either active or standby) of your cluster based applications from one host to another without restarting your application. You can use these api to build maintenance, balancing & remediation tools. 

#### Simplify Job Runner & Configs
Job Runner will now simply submit Samza job to Yarn RM without executing any user code and job planning will happen on ClusterBasedJobCoordinator instead. This simplified workflow addresses security requirements where job submissions need to be isolated in order to execute user code as well as operational pain points where deployment failure could happen at multiple places.

Full list of the jiras addressed in this release can be found [here](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20and%20fixVersion%20in%20(1.5)).

### Upgrading your application to Apache Samza 1.5.0
ConfigFactory is deprecated as Job Runner does not load full job config anymore. Instead, ConfigLoaderFactory is introduced to be executed on ClusterBasedJobCoordinator to fetch full job config.
If you are using the default PropertiesConfigFactory, simply switching to use the default PropertiesConfigLoaderFactory will work, otherwise if you are using a custom ConfigFactory, kindly creates its new counterpart following ConfigLoaderFactory. 

Configs related to job submission must be explicitly provided to Job Runner as it is no longer loading full job config anymore.

### Simplify Job Runner & Configs
[SAMZA-2488](https://issues.apache.org/jira/browse/SAMZA-2488) Add JobCoordinatorLaunchUtil to handle common logic when launching job coordinator

[SAMZA-2471](https://issues.apache.org/jira/browse/SAMZA-2471) Simplify CommandLine

[SAMZA-2458](https://issues.apache.org/jira/browse/SAMZA-2458) Update ProcessJobFactory and ThreadJobFactory to load full job config

[SAMZA-2453](https://issues.apache.org/jira/browse/SAMZA-2453) Update ClusterBasedJobCoordinator to support Beam jobs

[SAMZA-2441](https://issues.apache.org/jira/browse/SAMZA-2441) Update ApplicationRunnerMain#ApplicationRunnerCommandLine not to load local file

[SAMZA-2420](https://issues.apache.org/jira/browse/SAMZA-2420) Update CommandLine to use config loader for local config file

### Container Placement API
[SAMZA-2402](https://issues.apache.org/jira/browse/SAMZA-2402) Tie Container placement service and Container placement handler and validate placement requests

[SAMZA-2379](https://issues.apache.org/jira/browse/SAMZA-2379) Support Container Placements for job running in degraded state

[SAMZA-2378](https://issues.apache.org/jira/browse/SAMZA-2378) Container Placements support for Standby containers enabled jobs


### Bug Fixes
[SAMZA-2515](https://issues.apache.org/jira/browse/SAMZA-2515) Thread safety for Kafka consumer in KafkaConsumerProxy

[SAMZA-2511](https://issues.apache.org/jira/browse/SAMZA-2511) Handle container-stop-fail in case of standby container failover

[SAMZA-2510](https://issues.apache.org/jira/browse/SAMZA-2510) Incorrect shutdown status due to race between runloop thread and process callback thread

[SAMZA-2506](https://issues.apache.org/jira/browse/SAMZA-2506) Inconsistent end of stream semantics in SystemStreamPartitionMetadata

[SAMZA-2464](https://issues.apache.org/jira/browse/SAMZA-2464) Container shuts down when task fails to remove old state checkpoint dirs

[SAMZA-2468](https://issues.apache.org/jira/browse/SAMZA-2468) Standby container needs to respond to shutdown request

### Other Improvements
[SAMZA-2519](https://issues.apache.org/jira/browse/SAMZA-2519) Support duplicate timer registration

[SAMZA-2508](https://issues.apache.org/jira/browse/SAMZA-2508) Use cytodynamics classloader to launch job container

[SAMZA-2478](https://issues.apache.org/jira/browse/SAMZA-2478) Add new metrics to track key and value size of records written to RocksDb

[SAMZA-2462](https://issues.apache.org/jira/browse/SAMZA-2462) Adding metric for container thread pool size

### Sources downloads
A source download of Samza 1.5.0 is available [here](https://dist.apache.org/repos/dist/release/samza/1.5.0/), and is also available in Apache’s Maven repository. See Samza’s download [page](https://samza.apache.org/startup/download/) for details and Samza’s feature preview for new features.
