---
layout: case-study
hide_title: true # so we have control in case-study layout, but can still use page
title: Real Time Session Aggregation
study_domain: optimizely.com
priority: 2
menu_title: Optimizely
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

Real Time Session Aggregation

<!--more-->

Optimizely is a world’s leading experimentation platform, enabling businesses to 
deliver continuous experimentation and personalization across websites, mobile 
apps and connected devices. At Optimizely, billions of events are tracked on a 
daily basis. Session metrics are among the key metrics provided to their end user 
in real time. Prior to introducing Samza for their realtime computation, the 
engineering team at Optimizely built their data-pipeline using a complex 
[Lambda architecture] (http://lambda-architecture.net/) leveraging 
[Druid and Hbase] (https://medium.com/engineers-optimizely/building-a-scalable-data-pipeline-bfe3f531eb38). 
As business requirements evolve, this solution became more and more challenging.

The engineering team at Optimizely decided to move away from Druid and focus on 
HBase as the store, and introduced stream processing to pre-aggregate and 
deduplicate session events. In their solution, every session event is tagged 
with an identifier for up to 30 minutes; upon receiving a session event, the 
Samza job updates session metadata and aggregates counters for the session 
that is stored in a local RocksDB state store. At the end of each one-minute 
window, aggregated session metrics are ingested to HBase. With the new solution

-   The median query latency was reduced from 40+ ms to 5 ms
-   Session metrics are now available in realtime
-   HBase query response time is improved due to reduced write-rate
-   HBase storage requirement are drastically reduced
-   Lower development effort thanks to out-of-the-box Kafka integration
 
Here is a testimonial from Optimizely

“At Optimizely, we have built the world’s leading experimentation platform, 
which ingests billions of click-stream events a day from millions of visitors 
for analysis. Apache Samza has been a great asset to Optimizely's Event 
ingestion pipeline allowing us to perform large scale, real time stream 
computing such as aggregations (e.g. session computations) and data enrichment 
on a multiple billion events / day scale. The programming model, durability 
and the close integration with Apache Kafka fit our needs perfectly” said 
Vignesh Sukumar, Senior Engineering Manager at Optimizely”

In addition, stream processing is also applied to other use cases such as 
data enrichment, event stream partitioning and metrics processing at Optimizely.

Key Samza features: *Stateful processing*, *Windowing*, *Kafka-integration*

More information

-   [https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-1-aed2051dd7a3](https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-1-aed2051dd7a3)
c9715fbc85f973907807cccc26c9d7d3ed983df
-   [https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-2-b596350a7820](https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-2-b596350a7820)
    