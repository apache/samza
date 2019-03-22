---
layout: case-study # the layout to use
hide_title: true # so we have control in case-study layout, but can still use page
title: Air Traffic Controller with Samza at LinkedIn
study_domain: linkedin.com # just the domain, not the protocol
priority: 4
menu_title: LinkedIn # what shows up in the menu
logo_url: https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png
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

LinkedIn is a professional networking company that offers various services and platform for job seekers, employers and sales professionals. With a growing user base and multiple product offerings, it becomes imperative to streamline communications to members. To ensure member experience comes first, LinkedIn developed a new email and notifications platform called *Air Traffic Controller (ATC)*.

ATC is designed to be an intelligent platform that tracks all outgoing communications and delivers the communication through the right channel to the right member at the right time.

<img src="/img/{{site.version}}/case-studies/linkedin-atc-samza-pipeline.png" alt="architecture" style="max-width: 80%; height: auto;" onclick="window.open(this.src)"/>

Any service that wants to send out a notification to members writes its request to a Kafka topic, which ATC later reads from. The ATC platform comprises of three components: <br/>

_Partitioners_ read incoming communication requests from Kafka and distribute them across _Pipeline_ instances based on the hash of the recipient. It also does some
filtering early-on to drop malformed messages. <br/><br/>
The _Relevance processors_ read personalized machine-learning models from Kafka and stores them in Samza's state store for evaluating them later. It uses them to score incoming requests and determine the right channel for the notification (eg: drop it vs sending an email vs push notification) . <br/><br/>
The _ATC pipeline_ processors aggregate the output from the _Relevance_ and the _Partitioners_, thereby making the final call on the notification. It heavily leverages Samza's local state to batch and aggregate notifications. It decides the frequency of notifications - duplicate notifications are merged, notifications are capped at a certain threshold. The _Pipeline_ also implements a _scheduler_ on top of Samza's local-store so that it can schedule messages for delivery later. As an example, it may not be helpful to send a push-notification at midnight. <br/><br/>


ATC uses several of Samza features:

**1.Stateful processing**: The ML models in the relevance module are stored locally in RocksDb and are updated realtime time based on user feedback. <br/><br/>
**2.Async APIs and Multi-threading**: Samza’s multi-threading and Async APIs allow ATC to perform remote calls with high throughput. This helps bring down the 90th percentile end-to-end latency for push notifications. <br/><br/>
**3.Host affinity**: Samza's incremental checkpointing and host-affinity enable ATC to achieve zero downtime during upgrades and instant recovery during failures. <br/><br/>

Key Samza Features: *Stateful processing*, *Async API*, *Host affinity*

More information

- [Sending less email is just the beginning](https://blog.linkedin.com/2015/11/10/sending-less-email-is-just-the-beginning)
