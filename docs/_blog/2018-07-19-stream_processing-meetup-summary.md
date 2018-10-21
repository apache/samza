---
layout: blog
title: Our July 2018 meetup - A Report
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

On July 19, 2018, we hosted a community meetup at LinkedIn focussing on Apache Kafka, Apache Samza, and related streaming technologies. 
The event featured technical deep dives by engineers from LinkedIn and Uber on the latest in the stream processing space... 

<!--more-->

July 2018 - On July 19, 2018, we hosted a community meetup at LinkedIn focussing on Apache Kafka, Apache Samza, and related streaming technologies. 
The event featured technical deep dives by engineers from LinkedIn and Uber on the latest in the stream processing space. Here is a brief summary
of the talks that were presented. 

### [Beam me up Samza: How we built a Samza Runner for Apache Beam](https://youtu.be/o5GaifLoZho)

LinkedIn's Xinyu Liu presented [Beam me up Samza](https://bit.ly/2Nyc4pl), describing how Linkedin is harnessing cutting edge features of Beam. 
Apache Beam provides an easy-to-use, and powerful model for state-of-the-art stream and batch processing, portability 
across a variety of languages, and the ability to converge offline and nearline data processing. In this talk,
he discussed the Beam API and its implementation in Samza and the benefits of Beam Runner to the Samza and Beam community.
He also explored various use cases of Beam at LinkedIN and future work on it. 


### [uReplicator: Uber Engineering’s Scalable Robust Kafka Replicator](https://bit.ly/2NxvFpz)


Uber operates more than 20 Kafka clusters to collect system, application logs and event data from rider and driver apps. 
Uber's Hongliang Xu shared his insignts on Uber's approch for replicating data between Kafka clusters across multiple data centers. 
He covered the history behind [uReplicator](https://bit.ly/2NxvFpz) and gave the high level architecture. Furthermore he also discussed the
scalability challenges and operational overhead as the Uber exapanded and how did they build Federated uReplicator 
which addressed challanges at scale


### [Concourse - Near real time notifications platform at Linkedin](https://youtu.be/Fszo6jThq0I)


[Concourse](https://bit.ly/2zXNwUJ) is LinkedIn’s first near-real-time targeting and scoring platform for notifications. In this talk LinkedIn's Ajith Muralidharan & Vivek Nelamangala provided an in-depth overview of the design and various scaling optimizations. 
Concourse has an ability to score millions of notifications per second, while supporting the use of feature-rich machine learning 
models based on terabytes of feature data.


<!--more-->


