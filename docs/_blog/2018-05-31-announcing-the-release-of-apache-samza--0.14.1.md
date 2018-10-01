---
layout: blog
title: Announcing the release of Apache Samza 0.14.1
icon: git-pull-request
authors:
    - name: Xinyu
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


A minor release which contains improvements over multiple areas: SQL, Standalone, Eventhub, and Host-affinity.

<!--more-->


Announcing the release of Apache Samza 0.14.1

We are very excited to announce the release of **Apache Samza 0.14.1**  
Samza has been powering real-time applications in production across several large companies (including LinkedIn, Netflix, Uber, Slack, Redfin, TripAdvisor, etc) for years now. Samza provides leading support for large-scale stateful stream processing with:

-   First class support for local state (with RocksDB store). This allows a stateful application to scale up to 1.1 Million events/sec on a single machine with SSD.
-   Support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.
-   A fully pluggable model for input sources (e.g. Kafka, Kinesis, DynamoDB streams etc.) and output systems (HDFS, Kafka, ElastiCache etc.).
-   A fully asynchronous programming model that makes parallelizing remote calls efficient and effortless.
-   High level API for expressing complex stream processing pipelines in a few lines of code.
-   Flexible deployment model for running the the applications in any hosting environment and with cluster managers other than YARN.
-   Features like canaries, upgrades and rollbacks that support extremely large deployments with minimal downtime.

### Enhancements, Upgrades and Bug Fixes

This is a minor release which contains improvements over multiple areas. In particular:  

-   **SQL**
-   [SAMZA-1681](https://issues.apache.org/jira/browse/SAMZA-1681) Add support for handling older record schema versions in AvroRelConverter
-   [SAMZA-1671](https://issues.apache.org/jira/browse/SAMZA-1671) Add insert into table support
-   [SAMZA-1651](https://issues.apache.org/jira/browse/SAMZA-1651) Implement GROUP BY SQL operator
-   **Standalone**
-   [SAMZA-1689](https://issues.apache.org/jira/browse/SAMZA-1689) Add validations before state transitions in ZkBarrierForVersionUpgrade
-   [SAMZA-1686](https://issues.apache.org/jira/browse/SAMZA-1686) Set finite operation timeout when creating zkClient
-   [SAMZA-1667](https://issues.apache.org/jira/browse/SAMZA-1667) Skip storing configuration as a part of JobModel in zookeeper data nodes
-   [SAMZA-1647](https://issues.apache.org/jira/browse/SAMZA-1647) Fix NPE in JobModelExpired event handler
-   **Eventhub**
-   [SAMZA-1688](https://issues.apache.org/jira/browse/SAMZA-1688) Use per partition eventhubs client
-   [SAMZA-1676](https://issues.apache.org/jira/browse/SAMZA-1676) Miscellaneous fix and improvement for eventhubs system
-   [SAMZA-1656](https://issues.apache.org/jira/browse/SAMZA-1656) EventHubSystemAdmin does not fetch metadata for valid streams
-   **Host-affinity**
-   [SAMZA-1687](https://issues.apache.org/jira/browse/SAMZA-1687) Prioritize preferred host requests over ANY-HOST requests
-   [SAMZA-1649](https://issues.apache.org/jira/browse/SAMZA-1649) Improve host-aware allocation to account for strict locality

In addition, Samza is also upgraded to support Kafka version 0.11 in this release.  
Overall, [51 JIRAs](https://issues.apache.org/jira/projects/SAMZA/versions/12343155) were resolved in this release. A source download of the 0.14.1 release is available [here](http://www.apache.org/dyn/closer.cgi/samza/0.14.1). The release JARs are also available in Apache’s Maven repository. See Samza’s [download](http://samza.apache.org/startup/download/) page for details and Samza’s [feature preview](https://samza.apache.org/startup/preview/) for new features. We requires JDK version newer than 1.8.0_111 when running 0.14.1 release for users who are using Scala 2.12.

### Community Developments

In March 21th, we held the meetup for [Stream Processing with Apache Kafka & Apache Samza](https://www.meetup.com/Stream-Processing-Meetup-LinkedIn/events/248309045/), which has the following presentations for Samza:

-   [Conquering the Lambda architecture in LinkedIn metrics platform with Apache Calcite and Apache Samza](https://www.youtube.com/watch?v=ZPWInJ4USIU) ([Slides](https://www.slideshare.net/KhaiTran17/conquering-the-lambda-architecture-in-linkedin-metrics-platform-with-apache-calcite-and-apache-samza))
-   [Building Venice with Apache Kafka & Samza](https://www.youtube.com/watch?v=Usz8E4S-hZE)

In industry, Samza got new adopters, including [Ntent](http://www.ntent.com) and [Movico](https://movio.co/en/).

### Contribute

It’s a great time to get involved. You can start by reviewing the [tutorials](http://samza.apache.org/startup/preview/#try-it-out), signing up for the [mailing list](http://samza.apache.org/community/mailing-lists.html), and grabbing some [newbie JIRAs](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open).  
I’d like to close by thanking everyone who’s been involved in the project. It’s been a great experience to be involved in this community, and I look forward to its continued growth.
