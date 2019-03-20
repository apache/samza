---
layout: blog
title: Our October 2018 meetup - A Report
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

On October 23rd, 2018, we hosted a community meetup at LinkedIn focussing on Apache Kafka, Apache Samza, and related streaming technologies.
The event featured technical deep dives by engineers from LinkedIn and Uber on the latest in the stream processing space...

<!--more-->

October 2018 - On October 19, 2018, we hosted a community meetup at LinkedIn focussing on Apache Kafka, Apache Samza, and related streaming technologies.
The event featured technical deep dives by engineers from LinkedIn and Uber on the latest in the stream processing space. Here is a brief summary
of the talks that were presented.

### [How LinkedIn navigates Streams Infrastructure using Cruise Control](https://youtu.be/jdo6F21gI8g)

_Speaker: Efe Gencer, LinkedIn_

Efe shared our work and experiences towards alleviating the management overhead of large-scale Kafka clusters using Cruise Control at LinkedIn. 
The first part of this talk provided an overview of Cruise Control, including the operational challenges that it solves, its high-level architecture, 
and some evaluation results from real-world scenarios. The second part went through a hands-on tutorial to demonstrate how we can manage a real Kafka 
cluster using Cruise Control.

### [Stream Analytics Manager](https://youtu.be/ULLE60su5cM)

_Speaker: Sriharsha Chintalapani, Uber_

Stream Analytics Manager provides a simplified UI interface to build complex big data applications. It makes it possible for the end user to not only 
build but also deploy and monitor streaming applications. It provides pluggable interfaces to provide user supplied business logic through Custom 
Processors, UDFs. Streamline’s main goal is to let developers build, deploy, manage, monitor streaming applications easily in minutes. In this talk 
Sriharsha went through how we can add other engines like Flink, Spark, Airflow into Streamline and allow users to build both Batch and Streaming applications.

### [Operating Samza at LinkedIn](https://youtu.be/AnNwkfJO4Us)

_Speaker: Abhishek Shivanna, Stephan Soileau, LinkedIn_

Operating Samza at LinkedIn, which, processes around a trillion of messages a day with over several thousand jobs, is a daunting task. 
Abhishek and Stephan went go over the best practices of running Samza as a managed service and took a look at how SREs at LinkedIn use intelligent 
automation to operate at LinkedIn scale.


<!--more-->
