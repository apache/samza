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

How LinkedIn built Air Traffic Controller, a stateful stream processing system to optimize email communications?

<!--more-->

LinkedIn is a professional networking company that offers various services and platform for job seekers, employers and sales professionals. With a growing user base and multiple product offerings, it becomes imperative to streamline communications to members. To ensure member experience comes first before individual product metrics, LinkedIn developed a new email and notifications platform called *Air Traffic Controller*.

ATC is designed to be an intelligent platform that tracks all outgoing communications and delivers the communication through the right channe to the right member at the right time.

<img src="/img/{{site.version}}/case-studies/linkedin-atc-samza-pipeline.png" alt="architecture" style="max-width: 80%; height: auto;" onclick="window.open(this.src)"/>

Any service that wants to send out a notification to members writes its request to a Kafka topic, which ATC later reads from. The ATC platform comprises of three components:

- **Partitioner**: _Partitioners_ read incoming communication requests from Kafka and distribute them across _Pipeline_ instances based on the hash of the recipient. It also does some
filtering early-on to drop malformed messages.
- **Relevance processor**: The _Relevance processors_ read personalized machine-learning models and stores them in Samza's RocksDb store. It uses them to evaluate incoming requests and determine the right channel (eg: drop it vs sending an email vs push notification vs badge) for the notification.
- **Pipeline**:  The _pipeline_ processors aggregate the output of the _Relevance_ and the _Partitioners_, thereby making the final call. It heavily leverages Samza's local state to 
store and merge notifications. It decides the frequency of notifications (eg: duplicate notifications are merged, notifications are capped at a certain threshold). The _Pipeline_ also implements a _scheduler_ on top of Samza's local-store so that it can schedule messages for delivery later (For eg: it makes no sense to send notifications to a member at midnight)


Handle partitioned communication requests which performs aggregation and consults with the relevance model to determine delivery time
- **Relevance processor**: Provide insights on how relevant is the content to the user, the right delivery time, etc.

ATC, leverages Samza extensively and uses a lot of features including but not limited to:

- **Stateful processing**: The ML models in the relevance module are stored locally in RocksDb which are updated realtime time based on user feedback.
- **Async APIs and Multi-threading**: Samza’s multi-threading and Async APIs allows ATC to perform remote calls with high-throughput. This helps bring down the 90th percentile (P90) end-to-end latency for end to end latency for push notifications from about 12 seconds to about 1.5 seconds.
- **Host affinity**: Co-location of local state stores along with host awareness helps ATC to achieve zero downtime and instant recovery.

Key Samza Features: *Stateful processing*, *Async API*, *Host affinity*

More information

- [Sending less email is just the beginning](https://blog.linkedin.com/2015/11/10/sending-less-email-is-just-the-beginning)
