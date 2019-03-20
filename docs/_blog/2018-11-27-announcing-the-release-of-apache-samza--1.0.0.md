---
layout: blog
title: Announcing the release of Apache Samza 1.0.0
icon: git-pull-request
authors:
    - name: Daniel Chen
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


A major release which contains a rich set of new features.

<!--more-->


We are thrilled to announce the release of Apache Samza 1.0.0

Today Samza forms the backbone of hundreds of real-time production applications across a multitude of companies, such as LinkedIn, VMWare, Slack, Redfin among many others. This release of Samza adds a variety of features and capabilities to Samza’s existing arsenal, coupled with new and improved documentation, code snippets, examples, and a brand-new website design! Here are a few selected highlights:

- Stable high level APIs that allow creating complex processing pipelines with ease.
- Beam Samza Runner now marries Beam’s best in class support for EventTime based windowed processing and sophisticated triggering with Samza’s stable and scalable stateful processing model.
- Table API that provides a common abstraction for accessing remote or local databases. Developers are now able to “join” an input event stream with such a Table.
- Integration Test Framework to enable effortless testing of Samza jobs without deploying a Kafka, Yarn, or Zookeeper cluster.
- Support for Apache Log4j2 allowing improved logging performance, customization, and efficiency.
- Upgraded Kafka client and consumer.
- An interactive shell for Samza SQL for seamless formulation, development, and testing of SamzaSQL queries.
- Side-input support that allows using log-compacted data sources to populate KV state for Samza applications.
- An improved website with detailed documentation and lots of code samples!
- In addition, Samza 1.0 brings numerous bug-fixes, upgrades, and improvements listed below.

### New features
Samza 1.0 brings full-feature support for the following:

##### Improved Stable High Level APIs
Samza 1.0 brings Descriptor APIs that allows applications to specify their input and output systems and streams in code. Samza’s new Context APIs provide applications unified access to job-level, container-level, task-level, and application-level context and capabilities. This also simplifies Samza’s ApplicationRunner interface.

This API evolution requires a few simple modifications to application code, which we describe in detail in our upgrade steps

##### Beam Runner Support
Samza’s Beam Runner enables executing Beam pipelines over Samza. This enables Samza applications to create complex processing pipelines that require event-time based processing, varying types of event-time based windowing, and more. This feature is supported in both the YARN and standalone deployment models.

##### Joining Streams and Tables
Samza’s Table API provides developers with unified access to local and remote data sources such as Key-Value stores or web services, while providing features such as rate-limiting, throttling, and caching capabilities. This provides first-class API primitives for building Stream-Table join jobs. Learn more about the use, semantics, and examples for Table API here.

##### Test Samza without ZK, Yarn or Kafka
Samza 1.0 brings a test framework that allows testing Samza applications using in-memory input and output. Users can now setup test and testing pipelines for their applications without needing to setup any other services, such as Kafka, YARN, or Zookeeper.

##### Log4J2 support
Samza now supports Apache Log4j 2 for system and application logging. Log4j 2 is an upgrade to Log4j that provides significant improvements over its predecessor, Log4j 1.x, such as better throughput and latency, custom log levels, and a pluggable logging architecture.

##### Kafka upgrade
This release upgrades Samza to use Kafka’s high-level consumer (Kafka v0.11.1.62). This brings latency and throughput benefits for Samza applications that consume from Kafka, in addition to bug-fixes. This also means Samza applications can now better their utilization of the underlying Kafka cluster.

##### SamzaSQL Shell
SamzaSQL now provides a shell for users to type-in their SQL queries, while Samza does the heavy-lifting of wiring the inputs and outputs, and sizing the application in the background. This is great for testing and experimenting with queries while formulating your application-logic, specially suited for data-scientists and tinkerers.

##### Side-inputs
Samza 1.0 brings the ability to leverage existing log-compacted data sources (e.g., Kafka topics) to populate KV state for Samza applications. If your data processing pipeline involves Hadoop-to-Kafka push, this feature alleviates the need for your Samza job to create separate Kafka-topics to back KV state.

##### Improved website, documentation, and samples
We’ve re-designed the Samza website making it easier to find details on key Samza concepts and patterns. All documentation has been revised and rewritten, keeping in mind the feedback we got from our customers. We’ve revised and added sample application code to showcase Samza 1.0 and the use of its new APIs.

### Enhancements and Upgrades
This release brings the following enhancements, upgrades, and capabilities:

##### API enhancements and simplifications
-   [SAMZA-1789](https://issues.apache.org/jira/browse/SAMZA-1789): unify ApplicationDescriptor and ApplicationRunner for high- and low-level APIs in YARN and standalone environment
-   [SAMZA-1804](https://issues.apache.org/jira/browse/SAMZA-1804): System and stream descriptors
-   [SAMZA-1858](https://issues.apache.org/jira/browse/SAMZA-1858): Public APIs for shared context
-   [SAMZA-1763](https://issues.apache.org/jira/browse/SAMZA-1763): Add async methods to Table API
-   [SAMZA-1786](https://issues.apache.org/jira/browse/SAMZA-1786): Introduce the metadata store abstraction
-   [SAMZA-1859](https://issues.apache.org/jira/browse/SAMZA-1859): Zookeeper implementation of MetadataStore
-   [SAMZA-1788](https://issues.apache.org/jira/browse/SAMZA-1788): Add the LocationIdProvider abstraction

##### Upgrades and Bug-fixes
-   [SAMZA-1768](https://issues.apache.org/jira/browse/SAMZA-1768): Handle corrupted OFFSET file
-   [SAMZA-1817](https://issues.apache.org/jira/browse/SAMZA-1817): Long classpath support for non-split deployments SAMZA-1719: Add caching support to table-API
-   [SAMZA-1783](https://issues.apache.org/jira/browse/SAMZA-1783): Add Log4j2 functionality in Samza
-   [SAMZA-1868](https://issues.apache.org/jira/browse/SAMZA-1868): Refactor KafkaSystemAdmin from using SimpleConsumer
-   [SAMZA-1776](https://issues.apache.org/jira/browse/SAMZA-1776): Refactor KafkaSystemConsumer to remove the usage of deprecated SimpleConsumer client
-   [SAMZA-1730](https://issues.apache.org/jira/browse/SAMZA-1730): Adding state validation in StreamProcessor before any lifecycle operation and group coordination
-   [SAMZA-1695](https://issues.apache.org/jira/browse/SAMZA-1695): Clear events in ScheduleAfterDebounceTime on session expiration
-   [SAMZA-1647](https://issues.apache.org/jira/browse/SAMZA-1647): Fix race conditions in StreamProcessor
-   [SAMZA-1371](https://issues.apache.org/jira/browse/SAMZA-1371): Some Samza Containers get stuck at \“Starting BrokerProxy\”
-   [SAMZA-1648](https://issues.apache.org/jira/browse/SAMZA-1648): Integration Test Framework & Collection Stream Impl
-   [SAMZA-1748](https://issues.apache.org/jira/browse/SAMZA-1748): Failure tests in the standalone deployment

A source download of Samza 1.0 is available [here](https://dist.apache.org/repos/dist/release/samza/1.0.0/), and in Apache’s Maven repository.

### Community Developments
A [symposium](https://www.meetup.com/Stream-Processing-Meetup-LinkedIn/events/251481797) on Stream processing with Apache Samza and Apache Kafka was held on July 19th and on October 23rd. Both were attended by more than 350 participants from across the industry. It featured in-depth talks on Samza’s Beam integration, its use at LinkedIn for real-time notifications, a talk on Kafka-replication at Uber, and Kafka cruise control, and many others.
Samza was also the focus of a talk at [Strange Loop'18](https://www.youtube.com/watch?v=2y8QImf-RpI), focussing in depth on its scalability, performance, extensibility, and programmability.



### Contribute

It’s a great time to get involved. You can start by reviewing the [tutorials](http://samza.apache.org/startup/preview/#try-it-out), signing up for the [mailing list](http://samza.apache.org/community/mailing-lists.html), and grabbing some [newbie JIRAs](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open).  
I’d like to close by thanking everyone who’s been involved in the project. It’s been a great experience to be involved in this community, and I look forward to its continued growth.
