---
layout: blog
title: Announcing the release of Apache Samza 0.14.0
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


Testing the excerpt

<!--more-->


Announcing the release of Apache Samza 0.14.0

We are very excited to announce the release of **Apache Samza 0.14.0**  
Samza has been powering real-time applications in production across several large companies (including LinkedIn, Netflix, Uber, Slack, Redfin, TripAdvisor, etc) for years now. Samza provides leading support for large-scale stateful stream processing with:

-   First class support for local state (with RocksDB store). This allows a stateful application to scale up to 1.1 Million events/sec on a single machine with SSD.
-   Support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.
-   A fully pluggable model for input sources (e.g. Kafka, Kinesis, DynamoDB streams etc.) and output systems (HDFS, Kafka, ElastiCache etc.).
-   A fully asynchronous programming model that makes parallelizing remote calls efficient and effortless.
-   High level API for expressing complex stream processing pipelines in a few lines of code.
-   Flexible deployment model for running the the applications in any hosting environment and with cluster managers other than YARN.
-   Features like canaries, upgrades and rollbacks that support extremely large deployments with minimal downtime.

### New Features, Upgrades and Bug Fixes

The 0.14.0 release contains the following highly anticipated features:

-   Samza SQL
-   Azure EventHubs producer, consumer and checkpoint provider
-   AWS Kinesis consumer

This release also includes improvements such as durable state in high-level API, Zookeeper-based deployment stability, and multi-stage batch processing, and bug fixes such as KafkaSystemProducer concurrent sends and flushes.  
Overall, [65 JIRAs](https://issues.apache.org/jira/browse/SAMZA-1109?jql=project%20%3D%20SAMZA%20AND%20status%20%3D%20Resolved%20AND%20fixVersion%20%3D%200.14.0%20ORDER%20BY%20priority%20DESC%2C%20key%20ASC) were resolved in this release. For more details about this release, please check out the [release notes](http://samza.apache.org/startup/releases/0.14/release-notes.html).

### Community Developments

We’ve made great community progress since the last release (0.13.1). We presented the unified data processing with Samza at the 2017 Big Data conference held in Spain and the Dataworks Summit in Sydney, and held a demo at @scale conference in San Jose. Here are the details to these conferences.

-   Nov 17, 2017 - [Unified Stream Processing at Scale with Apache Samza (BigDataSpain 2017)](https://www.bigdataspain.org/2017/talk/apache-samza-jake-maes) ([Slides](https://www.slideshare.net/secret/oQe3debYJoY5q3))
-   Sept 21, 2017 - [Unified Batch & Stream Processing with Apache Samza (Dataworks Summit Sydney 2017)](https://dataworkssummit.com/sydney-2017/) ([Slides](https://www.slideshare.net/Hadoop_Summit/unified-batch-stream-processing-with-apache-samza))
-   Aug 31, 2017 - Demo of Stream Processing@LinkedIn (@scale conference 2017) ([Slides](https://www.slideshare.net/XinyuLiu11/samza-demo-scale-2017))

In Dec 4th, we held the meetup for [Stream Processing with Apache Kafka & Apache Samza](https://www.meetup.com/Stream-Processing-Meetup-LinkedIn/events/244889719/), which has the following presentations for Samza:

-   [Samza SQL](https://youtu.be/YDGIDO29Dqk) ([slides](https://www.slideshare.net/SamarthShetty2/stream-processing-using-samza-sql))
-   [Streaming data pipelines at Slack](https://youtu.be/wbS1P9ehgd0) ([slides](https://speakerdeck.com/vananth22/streaming-data-pipelines-at-slack))

As future development, we are continuing working on improvements to the new High Level API, SQL, Stream-Table Join and flexible deployment features.

### Contribute

It’s a great time to get involved. You can start by reviewing the [tutorials](http://samza.apache.org/startup/preview/#try-it-out), signing up for the [mailing list](http://samza.apache.org/community/mailing-lists.html), and grabbing some [newbie JIRAs](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open).  
I’d like to close by thanking everyone who’s been involved in the project. It’s been a great experience to be involved in this community, and I look forward to its continued growth.
