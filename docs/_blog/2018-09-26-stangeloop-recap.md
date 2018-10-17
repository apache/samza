---
layout: blog
title: Recap of Chasing Stream Processing Utopia
icon: analytics
authors:
    - name:
      website: 
      image: 
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

Talk By Kartik Paramasivam (Director of Engineering, LinkedIN)   
@Strange Loop, St. Louis, MO

<!--more-->

Over the last 15 years batch processing frameworks have thrived and ruled over big data processing. But now in the age of social computing, it is no longer acceptable to wait for data to land into a data-lake before it gets processed. 
We want our applications to react to new data as soon as it gets generated upstream. For a web site, members expect their feed to be updated as soon as some relevant activity, news, jobs etc. happens. 
We are talking seconds (or minutes). We also want to detect degraded site experience, fraud, security breaches, spam etc. instantaneously. Even business metrics (written in traditionally batch oriented languages like HIVE/PIG) are now expected to run in realtime. The current status-quo of real-time data processing (stream processing) is still very far from Utopia.

Kartik Paramasivam, The Director of Engineering presented Chasing the Stream Utopia at Strange Loop '18. The talk was inspired 
by the extensive growth in Streaming Data at Linkedin, which has experienced a growth of as high as 5 Trillion Messages per day in 2018. 
Linkedin supports close to 3000 applications in production using Kafka and Samza. He shed further light on Samza's claim
as State of the art Stream Processing framework in the streaming world, supporting use cases at LinkedIn, Slack, Uber, Intuit etc

His talk described LinkedIn's path on Chasing Utopia in Streaming world running apps at any complexity, any scale, 
any source, any language, and any environment! He shed light on all of the above with actual use cases from LinkedIn using Samza and Kafka in production. He touched Samza's battle tested Stateful and Stateless processing, and also on the 
newer available features like event time based processing using Beam Runner for Samza and Samza SQL. He further briefly explained running 
and managing Kafka at Scale. Covering an array of topics from Kafka Cluster Management Woes to Dynamic Load Balancing 
using Kafka Cruise Control. 

He further added the tooling ecosystem that supports these apps and streaming challanges that are faced at LinkedIn. He
concluded with the upcoming releases and features of Samza (Apache Samza 1.0) and Kafka (Apache Kafka 2.0). Please find more [here] (https://youtu.be/2y8QImf-RpI)

<br>