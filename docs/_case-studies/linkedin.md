---
layout: case-study # the layout to use
hide_title: true # so we have control in case-study layout, but can still use page
title: Air Traffic Controller with Samza at LinkedIn
study_domain: linkedin.com # just the domain, not the protocol
priority: 4
menu_title: LinkedIn # what shows up in the menu
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

LinkedIn is a professional networking company that offers various services and platform for job seekers, employers and sales professionals. With a growing user base and multiple product offerings, it becomes imperative to streamline and standardize our communications to the users. In order to ensure member experience comes first before individual product metrics, LinkedIn developed a new email and notifications platform called *Air Traffic Controller*.

ATC is an intelligent platform, that is capable of tracking all the outgoing communications to the user and delivering the communication through the right channel to the right member at the right time.

<img src="/img/{{site.version}}/case-studies/linkedin-atc-samza-pipeline.png" alt="architecture" style="max-width: 80%; height: auto;" onclick="window.open(this.src)"/>

It has a three main components,

- **Partitioner**: Partition communication requests, metrics based on user
- **Pipeline**: Handle partitioned communication requests which performs aggregation and consults with the relevance model to determine delivery time
- **Relevance processor**: Provide insights on how relevant is the content to the user, the right delivery time, etc.

ATC, leverages Samza extensively and uses a lot of features including but not limited to:

- **Stateful processing**: The ML models in the relevance module are stored locally in RocksDb which are updated realtime time based on user feedback.
- **Async APIs and Multi-threading**: Samza’s multi-threading and Async APIs allows ATC to perform remote calls with high-throughput. This helps bring down the 90th percentile (P90) end-to-end latency for end to end latency for push notifications from about 12 seconds to about 1.5 seconds.
- **Host affinity**: Co-location of local state stores along with host awareness helps ATC to achieve zero downtime and instant recovery.

Key Samza Features: *Stateful processing*, *Async API*, *Host affinity*

More information

- [Sending less email is just the beginning](https://blog.linkedin.com/2015/11/10/sending-less-email-is-just-the-beginning)
