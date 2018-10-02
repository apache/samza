---
layout: case-study # the layout to use
hide_title: true # so we have control in case-study layout, but can still use page
title: Realtime Notifications at Redfin
study_domain: redfin.com # just the domain, not the protocol
priority: 3
menu_title: Redfin # what shows up in the menu
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

Realtime Notifications

<!--more-->

Redfin is a leading full-service real estate brokerage that uses modern technology 
to help people buy and sell homes. Notification is the critical feature to 
communicate with Redfin’s customers, notification includes recommendations, instant 
emails, scheduled digests and push notifications. Thousands of emails are delivered 
to customers every minute at peak. 

The notification system used to be a monolithic system, which served the company 
well. However, as business grew and requirements evolved, it became harder and 
harder to maintain and scale. 

![Samza pipeline at Redfin](/img/case-studies/redfin.svg)

The engineering team at Redfin decided to replace 
the existing system with Samza primarily for Samza’s performance, scalability, 
support for stateful processing and Kafka-integration. A multi-stage stream 
processing pipeline was developed. At the Identify stage, external events 
such as new Listings are identified as candidates for new notification;
then potential recipients of notifications are determined by analyzing data in 
events and customer profiles, results are grouped by customer at the end of 
each time window at the Match Stage; once recipients and notification outlines are 
identified, the Organize stage retrieves adjunct data necessary to appear in each 
notification from various data sources by joining them with notification and 
customer profiles, results are stored/merged in local RocksDB state store; finally 
notifications are formatted at the Format stage and sent to notification
 delivery system at the Notify stage. 

With the new notification system

-   The system is more performant and horizontally scalable
-   It is now easier to add support for new use cases
-   Reduced pressure on other system due to the use of local RocksDB state store
-   Processing stages can be scaled individually

Other engineering teams at Redfin are also using Samza for business metrics 
calculation, document processing, event scheduling.

Key Samza Features: *Stateful processing*, *Windowing*, *Kafka-integration*

More information

-   [https://www.youtube.com/watch?v=cfy0xjJJf7Y](https://www.youtube.com/watch?v=cfy0xjJJf7Y)
-   [https://www.redfin.com/](https://www.redfin.com/)
