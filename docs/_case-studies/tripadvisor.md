---
layout: case-study
hide_title: true # so we have control in case-study layout, but can still use page
title: Hedwig - Converting Hadoop M/R ETL systems to Stream Processing
study_domain: tripadvisor.com
menu_title: TripAdvisor
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

Hedwig - Converting Hadoop M/R ETL systems to Stream Processing

<!--more-->

TripAdvisor is one of the world’s largest travel website that provides hotel 
and restaurant reviews, accommodation bookings and other travel-related 
content. It produces and processes billions events processed everyday 
including billing records, reports, monitoring events and application 
notifications.

Prior to migrating to Samza, TripAdvisor used Hadoop to ETL its data. Raw 
data was rolled up to hourly and daily in a number of stages with joins 
and sliding windows applied, session data is then produced from daily data. 
About 300 million sessions are produced daily. With this solution, the 
engineering team were faced with a few challenges

-   Long lag time to downstream that is business critical
-   Difficult to debug and troubleshoot due to scripts, environments, etc.
-   Adding more nodes doesn’t help to scale
 
The engineering team at TripAdvisor decided to replace the Hadoop solution 
with a multi-stage Samza pipeline. In the new solution, after raw data is 
first collected by Flume and ingested through a Kafka cluster, it is first 
parsed, cleansed and partitioned; then processing logic such as windowing, 
grouping, joining, fraud detection are applied, RocksDB is used as the local 
store for intermediate states; finally results are uploaded to HDFS, 
ElasticSearch, S3 and Hive. The new solution brought significant improvements.

-   Processing time is reduced from 3 hours to 1 hour
-   Hardware requirement is reduced to ⅓ thanks to optimized usage
-   Much simpler to debug and test the solution
 
Key Samza features: *Stateful processing*, *Windowing*, *Kafka-integration*

More information

-   [https://www.youtube.com/watch?v=KQ5OnL2hMBY](https://www.youtube.com/watch?v=KQ5OnL2hMBY)
    
-   [https://www.tripadvisor.com/](https://www.tripadvisor.com/)
    