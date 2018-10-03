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
results to be written to external system or data stores. As of the 1.0 release, Samza integrates with the following systems
out-of-the-box:

- [Apache Kafka](kafka) (consumer/producer)
- [Microsoft Azure Eventhubs](eventhubs) (consumer/producer)
- [Amazon AWS Kinesis Streams](kinesis) (consumer)
- [Hadoop Filesystem](hdfs) (consumer/producer)
- [Elasticsearch](https://github.com/apache/samza/blob/master/samza-elasticsearch/src/main/java/org/apache/samza/system/elasticsearch/ElasticsearchSystemProducer.java) (producer)

Instructions on how to use these connectors can be found in the corresponding subsections. Please note that the
connector API is different from [Samza Table API](../api/table-api), where the data could be read from and written to
data stores.

Samza is pluggable and designed to support a variety of producers and consumers. You can provide your own producer or
consumer by implementing the SystemFactory interface.

To associate a system with a Samza Connector, the user needs to set the following config:

{% highlight jproperties %}
systems.<system-name>.samza.factory=org.apache.samza.system.kafka.KafkaSystemFactory
{% endhighlight %}

Any system specific configs, could be defined as below:

{% highlight jproperties %}
systems.<system-name>.param1=value1
systems.<system-name>.consumer.param2=value2
systems.<system-name>.producer.param3=value3
{% endhighlight %}

