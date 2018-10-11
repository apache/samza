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

Testing the excerpt

<!--more-->

Slack is a cloud based company that offers collaboration tools and services to increase productivity. With a rapidly growing user base, and a daily active users north of 8 million, there is an imminent need to react quickly to issues and proactively monitor the health of the application. With a lack of existing monitoring solution, the team went on to build a new data pipeline with the following requirements

- Near realtime alerting
- Fault tolerant and high throughput data pipeline
- Process billions of metric data, logs and derive timely insights on the health of application
- Extend the pipeline to other use cases such as experimentation, performance etc.

<img src="/img/{{site.version}}/case-studies/slack-samza-pipeline.png" alt="architecture" style="max-width: 80%; height: auto;" onclick="window.open(this.src)"/>

The engineering team built a data platform using Apache Samza. It has three main components,

- **Router**: Deserialize Kafka events and add instrumentation
- **Processor**: Registers with the routers to process subset of message types and performs aggregation
- **Converter**: Enrich the processed data before piping the data to analytics store.  

The clients and backend servers channels the logs and exceptions through Kafka to content routers a.k.a samza partitioners. The partitioned data then flows through processors where it is stored in RocksDb before being joined with other metrics data. The enriched data is stored in druid which powers analytics queries and also acts as a trigger to alert slackbot notifications.

Other notable use case includes experimentation framework that leverage the data pipeline to track the results of A/B testing in near realtime. The metrics data is joined with the exposure table (members part of the experiment) to derive insights on the experiment. The periodic snapshots of RocksDb is also used to perform data quality check with the batch pipeline.

Key Samza Features: *Stateful processing*, *Join*, *Windowing*

More information

- [Talk: Streaming data pipelines at Slack](https://www.youtube.com/watch?v=wbS1P9ehgd0)
- [Slides: Streaming data pipelines at Slack](https://speakerdeck.com/vananth22/streaming-data-pipelines-at-slack)
