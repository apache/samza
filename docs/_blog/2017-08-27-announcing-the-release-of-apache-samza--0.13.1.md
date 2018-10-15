---
layout: blog
title: Announcing the release of Apache Samza 0.13.1
icon: git-pull-request
authors:
    - name: Navina
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


Testing the excerpt

<!--more-->


Announcing the release of Apache Samza 0.13.1

We are very excited to announce the release of **Apache Samza 0.13.1**  
Samza has been powering real-time applications in production across several large companies (including LinkedIn, Netflix, Uber) for years now. Samza provides leading support for large-scale stateful stream processing with

-   First class support for local state (with RocksDB store). This allows a stateful application to scale up to 1.1 Million events/sec on a single machine with SSD.
-   Support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.
-   A fully pluggable model for input sources (e.g. Kafka, Kinesis, DynamoDB streams etc.) and output systems (HDFS, Kafka, ElastiCache etc.).
-   A fully asynchronous programming model that makes parallelizing remote calls efficient and effortless.
-   High level API for expressing complex stream processing pipelines in a few lines of code.
-   Flexible deployment model for running the the applications in any hosting environment and with cluster managers other than YARN.
-   Features like canaries, upgrades and rollbacks that support extremely large deployments with minimal downtime.


### Enhancements, Upgrades and Bug Fixes 

This is a stability release to make Samza as an embedded library production ready. Samza as a library is part of Samza’s Flexible Deployment model; release fixes a number of outstanding bugs includes the following enhancements to existing features:

-   **Standalone**
-   [SAMZA-1165](https://issues.apache.org/jira/browse/SAMZA-1165) Cleanup data created by ZkStandalone in ZK
-   [SAMZA-1324](https://issues.apache.org/jira/browse/SAMZA-1324) Add a metrics reporter lifecycle for JobCoordinator component of StreamProcessor
-   [SAMZA-1336](https://issues.apache.org/jira/browse/SAMZA-1336) Standalone session expiration propagation
-   [SAMZA-1337](https://issues.apache.org/jira/browse/SAMZA-1337) LocalApplicationRunner supports StreamTask
-   [SAMZA-1339](https://issues.apache.org/jira/browse/SAMZA-1339) Add standalone integration tests
-   **General**
-   [SAMZA-1282](https://issues.apache.org/jira/browse/SAMZA-1282) Fix killed leader process issue when spinning up more containers than the number of tasks kills leader
-   [SAMZA-1340](https://issues.apache.org/jira/browse/SAMZA-1340) StreamProcessor does not propagate container failures from StreamTask
-   [SAMZA-1346](https://issues.apache.org/jira/browse/SAMZA-1346) GroupByContainerCount.balance() should guard against null LocalityManager
-   [SAMZA-1347](https://issues.apache.org/jira/browse/SAMZA-1347) GroupByContainerIds NPE if containerIds list is null
-   [SAMZA-1358](https://issues.apache.org/jira/browse/SAMZA-1358) task.class empty string should be ignored when app.class is configured
-   [SAMZA-1361](https://issues.apache.org/jira/browse/SAMZA-1361) OperatorImplGraph used wrong keys to store/retrieve OperatorImpl in the map
-   [SAMZA-1366](https://issues.apache.org/jira/browse/SAMZA-1366) ScriptRunner should allow callers to control the child process environment
-   [SAMZA-1384](https://issues.apache.org/jira/browse/SAMZA-1384) Race condition with async commit affects checkpoint correctness
-   [SAMZA-1385](https://issues.apache.org/jira/browse/SAMZA-1385) Fix coordination issues during stream creation in LocalApplicationRunner

Overall, [29 JIRAs](https://issues.apache.org/jira/issues/?jql=project%20%3D%2012314526%20AND%20fixVersion%20%3D%2012340845%20ORDER%20BY%20priority%20DESC%2C%20key%20ASC) were resolved in this release. 
A source download of the 0.13.1 release is available [here](http://www.apache.org/dyn/closer.cgi/samza/0.13.1). The release JARs are also available in Apache’s Maven repository. See Samza’s [download](http://samza.apache.org/startup/download/) page for details and Samza’s [feature preview](https://samza.apache.org/startup/preview/) for new features. We requires JDK version newer than 1.8.0_111 when running 0.13.1 release for users who are using Scala 2.12.

### Community Developments

We’ve made great community progress since the last release (0.13.0). We presented Samza high level API features at the Cloud+Data NEXT Conference 2017 held in Silicon Valley, USA, and also gave a talk regarding the key features (Secret Kung Fu) of Samza at ArchSummit 2017 in Shenzhen, China, and a detailed study of stateful stream processing in VLDB 2017. Here are the details to these conferences.

-   July 15, 2017 - [Unified Processing with the Samza High-level API (Cloud+Data NEXT Conference, Silicon Valley)](http://www.cdnextcon.com/recap.html) [slides](https://www.slideshare.net/YiPan7/nextcon-samza-preso-july-final)
-   July 7, 2017 - [Secret Kung Fu of Massive Scale Stream Processing with Apache Samza - Xinyu Liu](http://sz2017.archsummit.com/presentation/900) [ArchSummit, Shenzhen, 2017]
-   Aug 28, 2017 - [Samza: Stateful Scalable Stream Processing at LinkedIn - Kartik Paramasivam (ACM VLDB, Munich, 2017)](http://www.vldb.org/pvldb/vol10/p1634-noghabi.pdf)

In industry, Samza got new adopters, including Redfin and VMWare. 
As future development, we are continuing working on improving the new High Level API and flexible deployment features. Here is the list of the tasks for upcoming features and improvements.
### Contribute

It’s a great time to get involved. You can start by reviewing the [tutorials](http://samza.apache.org/startup/preview/#try-it-out), signing up for the [mailing list](http://samza.apache.org/community/mailing-lists.html), and grabbing some [newbie JIRAs](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open).  
I’d like to close by thanking everyone who’s been involved in the project. It’s been a great experience to be involved in this community, and I look forward to its continued growth.
