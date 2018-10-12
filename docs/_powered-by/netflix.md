---
name: Netflix
domain: netflix.com
priority: 2
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

<a class="external-link" href="www.netflix.com" rel="nofollow">Netflix</a> uses single-stage Samza jobs to route over 700 billion events / 1 peta byte per day from fronting Kafka clusters to s3/hive. A portion of these events are routed to Kafka and ElasticSearch with support for custom index creation, basic filtering and projection. We run over 10,000 samza jobs in that many docker containers.