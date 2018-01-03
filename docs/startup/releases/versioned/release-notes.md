---
layout: page
title: Release Notes
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

{% if site.version != "latest" %}
1. [Download](#download)
2. [Release Notes for Samza-{{site.version}} version](#release-notes-for-{{site.version}})
3. [Upgrade Notes](#upgrade-notes)

{% else %}
1. [Release Notes for Samza-{{site.version}} version](#release-notes)
2. [Upgrade Notes](#upgrade-notes)
{% endif %}

{% if site.version != "latest" %}
## Download
All Samza JARs are published through [Apache's Maven repository](https://repository.apache.org/content/groups/public/org/apache/samza/). See [here](../download/index.html) for more details.

## Source Release
[samza-sources-{{site.version}}.tgz](http://www.apache.org/dyn/closer.lua/samza/{{site.version}}.*)


{% endif %}



## Release Notes
<!-- Add notes on new features, modified behavior of existing features, operational/performance improvements, new tools etc -->
* [SAMZA-1510](https://issues.apache.org/jira/browse/SAMZA-1510) - Samza SQL
* [SAMZA-1438](https://issues.apache.org/jira/browse/SAMZA-1438) - Producer and consumer for Azure EventHubs
* [SAMZA-1515](https://issues.apache.org/jira/browse/SAMZA-1515) - Kinesis consumer
* [SAMZA-1486](https://issues.apache.org/jira/browse/SAMZA-1486) - Checkpoint provider for Azure tables
* [SAMZA-1421](https://issues.apache.org/jira/browse/SAMZA-1421) - Support for durable state in high-level API
* [SAMZA-1392](https://issues.apache.org/jira/browse/SAMZA-1392) - KafkaSystemProducer performance and correctness with concurrent sends and flushes
* [SAMZA-1406](https://issues.apache.org/jira/browse/SAMZA-1406) - Enhancements to the ZooKeeper-based deployment model
* [SAMZA-1321](https://issues.apache.org/jira/browse/SAMZA-1321) - Support for multi-stage batch processing

## Upgrade Notes
<!-- Add detailed notes on how someone using an older version of samza (typically, currentVersion - 1) can upgrade to the latest -->
<!-- Notes typically include config changes, public-api changes, new user guides/tutorials etc -->

### Configuration Changes

<!-- PR 290 -->
* Introduced a new **mandatory** configuration - `job.coordination.utils.factory`. Read more about it
[here](../../learn/{{site.version}}/configuration.html). <br />This config is applicable to all Samza
applications deployed using the `LocalApplicationRunner` (that is, non-yarn deployments).

### API Changes

<!-- PR 292 -->
* The following APIs in `SystemAdmin` have been deprecated in the previous versions and hence, replaced with newer APIs.
If you have a custom **System** implementation, then you have to update to the newer APIs.
  * ~~void createChangelogStream(String streamName, int numOfPartitions);~~ -> ``` boolean createStream(StreamSpec streamSpec); ```
  * ~~void createCoordinatorStream(String streamName);~~ -> ``` boolean createStream(StreamSpec streamSpec); ```
  * ~~void validateChangelogStream(String streamName, int numOfPartitions);~~ -> ``` void validateStream(StreamSpec streamSpec) throws StreamValidationException; ```

<!-- PR 292 -->
* New API has been added to `SystemAdmin` that clear a stream. <br />
```
boolean clearStream(StreamSpec streamSpec);
```
<br />
Read more about it in the [API docs](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/system/SystemAdmin.html).

