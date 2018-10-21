---
layout: case-study
hide_title: true # so we have control in case-study layout, but can still use page
title: On using Samza at Optimizely to compute analytics over session windows.
study_domain: optimizely.com
priority: 2
menu_title: Optimizely
exclude_from_loop: false
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

On using Samza at Optimizely to compute analytics over session windows

<!--more-->

Optimizely is the world’s leading experimentation platform, enabling businesses to 
deliver continuous experimentation and personalization across websites, mobile 
apps and connected devices. At Optimizely, billions of events are tracked on a 
daily basis and session metrics are provided to their users in real-time. 

Prior to introducing Samza for their realtime computation, the 
engineering team at Optimizely built their data-pipeline using a complex 
[Lambda architecture](http://lambda-architecture.net/) using 
[Druid and Hbase](https://medium.com/engineers-optimizely/building-a-scalable-data-pipeline-bfe3f531eb38). 
Since some session metrics were computed using Map-Reduce jobs, they 
could be delayed up to hours after the events are received. As business requirements evolved, 
this solution became more and [more challenging](https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-1-aed2051dd7a3) to scale. 


The engineering team at Optimizely turned to stream processing to reduce latencies. 
In their solution, each up-stream client associates a _sessionId_ with the events it generates. Upon receiving each event, the Samza job extracts various
fields (e.g. ip address, location information, browser version, etc) and updates aggregated metrics
for the session. At the end of a time-window, the merged metrics for that session are ingested to HBase. 

With the new solution <br/>
-   The median query latency was reduced from 40+ ms to 5 ms <br/>
-   Session metrics are now available in real-time <br/>
-   Write-rate to Hbase is reduced, since the metrics are pre-aggregated by Samza<br/>
-   Storage requirements on Hbase are drastically reduced <br/>
-   Lower development effort thanks to out-of-the-box Kafka integration <br/>
 
Here is a testimonial from Optimizely

“At Optimizely, we have built the world’s leading experimentation platform, 
which ingests billions of click-stream events a day from millions of visitors 
for analysis. Apache Samza has been a great asset to Optimizely's Event 
ingestion pipeline allowing us to perform large scale, real time stream 
computing such as aggregations (e.g. session computations) and data enrichment 
on a multiple billion events / day scale. The programming model, durability 
and the close integration with Apache Kafka fit our needs perfectly” says 
Vignesh Sukumar, Senior Engineering Manager at Optimizely.

In addition to this case-study, Apache Samza is also leveraged for other usecases such as 
data-enrichment, re-partitioning of event streams and computing realtime metrics etc.

Key Samza features: *Stateful processing*, *Windowing*, *Kafka-integration*

More information

-   [From batching to streaming at Optimizely - Part 1](https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-1-aed2051dd7a3)
-   [From batching to streaming at Optimizely - Part 2](https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-2-b596350a7820)
    