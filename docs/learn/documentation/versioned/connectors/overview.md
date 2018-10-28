---
layout: page
title: Connectors overview
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

Stream processing applications often read data from external sources like Kafka or HDFS. Likewise, they require processed
results to be written to external system or data stores. Samza is pluggable and designed to support a variety of [producers](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/system/SystemProducer.html) and [consumers](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/system/SystemConsumer.html) for your data. You can 
integrate Samza with any streaming system by implementing the [SystemFactory](/learn/documentation/{{site.version}}/api/javadocs/org/apache/samza/system/SystemFactory.html) interface. 

The following integrations are supported out-of-the-box:

Consumers:

- [Apache Kafka](kafka) 

- [Microsoft Azure Eventhubs](eventhubs) 

- [Amazon AWS Kinesis Streams](kinesis) 

- [Hadoop Filesystem](hdfs) 

Producers:

- [Apache Kafka](kafka) 

- [Microsoft Azure Eventhubs](eventhubs) 

- [Hadoop Filesystem](hdfs) 

- [Elasticsearch](https://github.com/apache/samza/blob/master/samza-elasticsearch/src/main/java/org/apache/samza/system/elasticsearch/ElasticsearchSystemProducer.java)
