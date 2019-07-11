---
layout: blog
title: Announcing the release of Apache Samza 1.2.0
icon: git-pull-request
authors:
    - name: Boris Shkolnik
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

# **Announcing the release of Apache Samza 1.2.0**


<!--more-->

We’re thrilled to announce the release of Apache Samza 1.2.0.

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
This release brings the following features, upgrades, and capabilities:

* Upgrade to Kafka 2

* Beam integration with tables and integration with CouchBase

* Async high level API

* Bug fixes

Full list of the jiras addressed in this release can be found [here](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20fixVersion%20%3D%201.2).
### **Upgrading your application to Apache Samza 1.2.0**


### Kafka upgrade
[SAMZA-2127](https://issues.apache.org/jira/browse/SAMZA-2127) Upgrade to Kafka 2.0

### Async API for high level
[SAMZA-2055](https://issues.apache.org/jira/browse/SAMZA-2055) Design and Implement async API for high level

[SAMZA-2172](https://issues.apache.org/jira/browse/SAMZA-2172) Async High Level API does not schedule StreamOperatorTasks on separate threads

#### Startpoint
[SAMZA-2192](https://issues.apache.org/jira/browse/SAMZA-2192) Add StartpointVisitor implementation for EventHub.

[SAMZA-2189](https://issues.apache.org/jira/browse/SAMZA-2189) Integrate startpoint resolution workflow with SamzaContainer startup sequence.

[SAMZA-2179](https://issues.apache.org/jira/browse/SAMZA-2179) Move the StartpointVisitor abstraction to SystemAdmin interface.

[SAMZA-2046](https://issues.apache.org/jira/browse/SAMZA-2046) Startpoints - Fanout of SSP-only keyed Startpoints to SSP+TaskName

[SAMZA-2132](https://issues.apache.org/jira/browse/SAMZA-2132) Startpoint - flatten serialized key

#### Table API
[SAMZA-2185](https://issues.apache.org/jira/browse/SAMZA-2185) Ability to expose remote data source specific features in remote table

[SAMZA-2156](https://issues.apache.org/jira/browse/SAMZA-2156) Couchbase Table Support for Samza Table API

[SAMZA-2153](https://issues.apache.org/jira/browse/SAMZA-2153) Config for TableRetryPolicy

[SAMZA-2134](https://issues.apache.org/jira/browse/SAMZA-2134) Enable remote table rate limiter by default

[SAMZA-2116](https://issues.apache.org/jira/browse/SAMZA-2116) Make sendTo operators non-terminal

#### Bug Fixes, Testing and Stability improvments
[SAMZA-2202](https://issues.apache.org/jira/browse/SAMZA-2202) Modify topic creation s.t. all log compacted topics are created with a 5MB message size limit.

[SAMZA-2181](https://issues.apache.org/jira/browse/SAMZA-2181) Ensure consistency of coordinator store creation and initialization

[SAMZA-2178](https://issues.apache.org/jira/browse/SAMZA-2178) Utils to directly inject custom IME to InMemorySystem streams

[SAMZA-2176](https://issues.apache.org/jira/browse/SAMZA-2176) Ignore the configurations with serialized null values from coordinator stream.

[SAMZA-2171](https://issues.apache.org/jira/browse/SAMZA-2171) Encapsulate creation and loading of metadata streams

[SAMZA-2170](https://issues.apache.org/jira/browse/SAMZA-2170) Enabling writing of both new and old format offset files for stores and side-input-stores

[SAMZA-2169](https://issues.apache.org/jira/browse/SAMZA-2169) Preventing task-shuffle after task mode addition

[SAMZA-2161](https://issues.apache.org/jira/browse/SAMZA-2161) Move ChangelogPartitionManager and CoordinatorStream ConfigReader to MetadataStore

[SAMZA-2135](https://issues.apache.org/jira/browse/SAMZA-2135) Provide a way inject ExternalContext to TestRunner

### Sources downloads
A source download of Samza 1.2.0 is available [here](https://dist.apache.org/repos/dist/release/samza/1.2.0/), and is also available in Apache’s Maven repository. See Samza’s download [page](https://samza.apache.org/startup/download/) for details and Samza’s feature preview for new features.

## **Community Developments**
Future Samza meetups can be found at [Stream Processing with Apache Kafka & Apache Samza meetup/symposium](https://www.meetup.com/Stream-Processing-Meetup-LinkedIn/)

##**Contribute**

It’s a great time to get involved. You can start by reviewing the [tutorials](http://samza.apache.org/startup/preview/#try-it-out), signing up for the [mailing list](http://samza.apache.org/community/mailing-lists.html), and grabbing some [newbie JIRAs](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open).

We'd like to close by thanking everyone who’s been involved in the project. It’s been a great experience to be involved in this community, and we are lookng forward to its continued growth.

