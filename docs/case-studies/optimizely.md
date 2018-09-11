---
layout: test
title: Real Time Session Aggregation at Optimizely
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

Optimizely is a world’s leading experimentation platform, enabling businesses to deliver continuous experimentation and personalization across websites, mobile apps and connected devices. At Optimizely, billions of events are tracked on a daily basis. Session metrics are among the key metrics provided to their end user in real time. Prior to introducing Samza for realtime computation, the engineering team at Optimizely used HBase to store and serve experimentation data, and Druid for personalization data including session metrics. As business requirements evolved, the Druid-based solution became more and more challenging.

-   Long delays in session metrics caused by M/R jobs
-   Reprocessing of events due to inability to incrementally update Druid index
-   Difficulties in scaling dimensions and cardinality
-   Queries expanding long time periods are expensive

The engineering team at Optimizely decided to move away from Druid and focus on HBase as the store, and introduced stream processing to pre-aggregate and deduplicate session events. They evaluated multiple stream processing platforms and chose Samza as their stream processing platform. In their solution, every session event is tagged with an identifier for up to 30 minutes; upon receiving a session event, the Samza job updates session metadata and aggregates counters for the session that is stored in a local RocksDB state store. At the end of each one-minute window, aggregated session metrics are ingested to HBase. With the new solution

-   The median query latency was reduced from 40+ ms to 5 ms
-   Session metrics are now available in realtime
-   HBase query response time is improved due to reduced write-rate
-   HBase storage requirement are drastically reduced
-   Lower development effort thanks to out-of-the-box Kafka integration
 
Here is a testimonial from Optimizely

“At Optimizely, we have built the world’s leading experimentation platform, which ingests billions of click-stream events a day from millions of visitors for analysis. Apache Samza has been a great asset to Optimizely's Event ingestion pipeline allowing us to perform large scale, real time stream computing such as aggregations (e.g. session computations) and data enrichment on a multiple billion events / day scale. The programming model, durability and the close integration with Apache Kafka fit our needs perfectly” said Vignesh Sukumar, Senior Engineering Manager at Optimizely”

In addition, stream processing is also applied to other use cases such as data enrichment, event stream partitioning and metrics processing at Optimizely.

Key Samza features: *Stateful processing*, *Windowing*, *Kafka-integration*, *Scalability*, *Fault-tolerant*

More information

-   [https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-1-aed2051dd7a3](https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-1-aed2051dd7a3)
    
-   [https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-2-b596350a7820](https://medium.com/engineers-optimizely/from-batching-to-streaming-real-time-session-metrics-using-samza-part-2-b596350a7820)
    
-   [https://docs.google.com/document/d/1xqiLIIBYfY_o9ByQ4xlhUY6yo3kQlMbZAc3ut95EyZU/edit](https://docs.google.com/document/d/1xqiLIIBYfY_o9ByQ4xlhUY6yo3kQlMbZAc3ut95EyZU/edit)
