---
layout: case-study
hide_title: true # so we have control in case-study layout, but can still use page
title: Low Latency Web Scale Fraud Prevention
study_domain: ebay.com
menu_title: eBay
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

Low Latency Web Scale Fraud Prevention

<!--more-->

eBay Enterprise is the world’s largest omni-channel commerce provider with 
hundreds millions of units shipped annually, as commerce gets more 
convenient and complex, so does fraud. The engineering team at eBay 
Enterprise selected Samza as the platform to build the horizontally 
scalable, realtime (sub-seconds) and fault tolerant abnormality detection 
system. For example, the system computes and evaluates key metrics to 
detect abnormal behaviors

-   Transaction velocity (#tnx/day) and change (#tnx/day vs #tnx/day over n days)
-   Amount velocity ($tnx/day) and change ($tnx/day vs $tnx/day over n days)

A wide range of realtime and historical adjunct data from various sources 
including people, places, interests, social and connections are ingested 
through Kafka, and stored in local RocksDB state store with changelog 
enabled for recovery. Incoming transaction data is aggregated using 
windowing and then joined with adjunct data stores in multiple stages. 
The system generates potential fraud cases for review real time. Finally, 
the engineering team at eBay Enterprise has built an OpenTSDB and Grafana 
based monitoring system using metrics collected through JMX.

Key Samza features: *Stateful processing*, *Windowing*, *Kafka-integration*,
*JMX-metrics*

More information

-   [https://www.slideshare.net/edibice/extremely-low-latency-web-scale-fraud-prevention-with-apache-samza-kafka-and-friends](https://www.slideshare.net/edibice/extremely-low-latency-web-scale-fraud-prevention-with-apache-samza-kafka-and-friends)
-   [http://ebayenterprise.com/](http://ebayenterprise.com/)
