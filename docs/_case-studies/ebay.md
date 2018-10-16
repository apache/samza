---
layout: case-study
hide_title: true # so we have control in case-study layout, but can still use page
title: Low Latency Web-Scale Fraud Prevention
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

How Samza powers low-latency, web-scale fraud prevention at Ebay?

<!--more-->

eBay Enterprise is the world’s largest omni-channel commerce provider. The engineering team at eBay chose Apache Samza to build _PreCog_, their 
horizontally scalable anomaly detection system. 

_PreCog_ extensively leverages Samza's high-performance, fault-tolerant local storage. Its architecture had the following requirements, for which Samza perfectly fit the bill: <br/>

_Web-scale:_ Scale to a large number of users and large volume of data per-user. Additionally, should be possible to add more commodity hardware and scale horizontally. <br/>
_Low-latency:_ Process customer interactions real-time by reacting in milliseconds instead of hours. <br/>
_Fault-tolerance:_ Gracefully tolerate and handle hardware failures. <br/>

![diagram-large](/img/{{site.version}}/learn/documentation/case-study/ebay.png)

The PreCog anomaly-detection system comprises of multiple tiers, with each tier consisting of multiple Samza jobs, which process the output of the previous tier.

_Ingestion tier:_ In this tier, a variety of historical and realtime data from various
sources including people, places etc., is ingested into Kafka.

_Fanout tier:_ This tier consists of Samza jobs which process the Kafka events, fan them out and re-partition them based on various
facets like email-address, ip-address, credit-card number, shipping address etc. 

_Compute tier:_ The Samza jobs in this tier consume messages from the fan-out tier and compute various key metrics and derived features. Features used to evaluate fraud include: 

1. Number of transactions per-customer per-day <br/>
2. Change in the number of daily transactions over the past few days <br/>
3. Amount value ($$) of each transaction per-day <br/>
4. Change in the amount value of transactions over a sliding time-window <br/>
5. Number of transactions per shipping-address

_Assembly tier:_ This tier comprises of Samza jobs which join the output of the compute-tier with other additional data-sources
and make a final determination on transaction-fraud. 

For monitoring the _PreCog_ pipeline, EBay leverages Samza's [JMXMetricsReporter](/learn/documentation/{{site.version}}/operations/monitoring.html) and ingests the reported metrics into OpenTSDB/ HBase. The metrics are then 
visualzed using [Grafana](https://grafana.com/).


Key Samza features: *Stateful processing*, *Windowing*, *Kafka-integration*, *JMX-metrics*

More information:

-   [Slides: Low latency Fraud prevention with Apache Samza](https://www.slideshare.net/edibice/extremely-low-latency-web-scale-fraud-prevention-with-apache-samza-kafka-and-friends)
-   [http://ebayenterprise.com/](http://ebayenterprise.com/)