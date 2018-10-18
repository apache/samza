---
layout: case-study # the layout to use
hide_title: true # so we have control in case-study layout, but can still use page
title: Building streaming data pipelines for monitoring and analytics at Slack # title of case study page
study_domain: slack.com # just the domain, not the protocol
priority: 1
menu_title: Slack # what shows up in the menu
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

How Slack monitors their infrastructure using Samza's streaming data-pipelines?

<!--more-->

Slack is a cloud based company that offers collaboration tools and services to increase productivity. With a rapidly growing user base and a daily active users north of 8 million, they needed to react quickly to issues and proactively monitor the application health. For this, the team went on to build a new monitoiring solution using Apache Samza with the following requirements:

- Near real-time alerting to quickly surface issues
- Fault-tolerant processing of data streams
- Process billions of events from metrics, logs and derive timely insights on application health
- Ease of extensibility to other use cases like experimentation

<img src="/img/{{site.version}}/case-studies/slack-samza-pipeline.png" alt="architecture" style="max-width: 80%; height: auto;" onclick="window.open(this.src)"/>

The engineering team at Slack built their data platform using Apache Samza. It has three types of Samza jobs - _Routers_, _Processors_ and _Converters_.

All services at Slack emit their logs in a well-defined format, which end up in a Kafka cluster. The logs are processed by a fleet of Samza jobs called _Routers_. The routers deserialize
incoming log events, decorate them and add instrumentation on top of them. The output of the router is processed by another pipeline, _Processors_ which perform aggregations using Samza's state-store. Finally, the processed results are enriched by the last stage - _Coverters_, which pipe the data into Druid for analytics and querying. Performance anomalies trigger an alert to a slackbot for further action. Slack built the data-platform to be extensible, thereby enabling other teams within the company to build their own applications on top of it.

Another noteworthy use-case powered by Samza is their experimentation framework. It leverages a data-pipeline to measure the results of A/B testing in near real-time. The pipeline uses Samza to join a stream of performance-related metrics with additional data on experiments that the customer was a part of. This enables Slack to learn how each experiment affects their overall customer experience. 

Key Samza Features: *Stateful processing*, *Join*, *Windowing*

More information

- [Talk: Streaming data pipelines at Slack](https://www.youtube.com/watch?v=wbS1P9ehgd0)
- [Slides: Streaming data pipelines at Slack](https://speakerdeck.com/vananth22/streaming-data-pipelines-at-slack)