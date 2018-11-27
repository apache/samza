---
layout: page
title: HDFS Connector
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

### Overview

The HDFS connector allows your Samza jobs to read data stored in HDFS files. Likewise, you can write processed results to HDFS. 
To interact with HDFS, Samza requires your job to run on the same YARN cluster.

### Consuming from HDFS
#### Input Partitioning

Partitioning works at the level of individual directories and files. Each directory is treated as its own stream and each of its files is treated as a _partition_. For example, Samza creates 5 partitions when it's reading from a directory containing 5 files. There is no way to parallelize the consumption when reading from a single file - you can only have one container to process the file.

#### Input Event format
Samza supports avro natively, and it's easy to extend to other serialization formats. Each avro record read from HDFS is wrapped into a message-envelope. The [envelope](../api/javadocs/org/apache/samza/system/IncomingMessageEnvelope.html) contains these 3 fields:

- The key, which is empty

- The value, which is set to the avro [GenericRecord](https://avro.apache.org/docs/1.7.6/api/java/org/apache/avro/generic/GenericRecord.html)

- The partition, which is set to the name of the HDFS file

To support non-avro input formats, you can implement the [SingleFileHdfsReader](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/reader/SingleFileHdfsReader.java) interface.

#### EndOfStream

While streaming sources like Kafka are unbounded, files on HDFS have finite data and have a notion of EOF. When reading from HDFS, your Samza job automatically exits after consuming all the data. You can implement [EndOfStreamListenerTask](../api/javadocs/org/apache/samza/task/EndOfStreamListenerTask.html) to get a callback once EOF has been reached. 


#### Defining streams

Samza uses the notion of a _system_ to describe any I/O source it interacts with. To consume from HDFS, you should create a new system that points to - `HdfsSystemFactory`. You can then associate multiple streams with this _system_. Each stream should have a _physical name_, which should be set to the name of the directory on HDFS.

{% highlight jproperties %}
systems.hdfs.samza.factory=org.apache.samza.system.hdfs.HdfsSystemFactory

streams.hdfs-clickstream.samza.system=hdfs
streams.hdfs-clickstream.samza.physical.name=hdfs:/data/clickstream/2016/09/11

{% endhighlight %}

The above example defines a stream called `hdfs-clickstream` that reads data from the `/data/clickstream/2016/09/11` directory. 

#### Whitelists & Blacklists
If you only want to consume from files that match a certain pattern, you can configure a whitelist. Likewise, you can also blacklist consuming from certain files. When both are specified, the _whitelist_ selects the files to be filtered and the _blacklist_ is later applied on its results. 

{% highlight jproperties %}
systems.hdfs.partitioner.defaultPartitioner.whitelist=.*avro
systems.hdfs.partitioner.defaultPartitioner.blacklist=somefile.avro
{% endhighlight %}


### Producing to HDFS

#### Output format

Samza allows writing your output results to HDFS in AVRO format. You can either use avro's GenericRecords or have Samza automatically infer the schema for your object using reflection. 

{% highlight jproperties %}
# set the SystemFactory implementation to instantiate HdfsSystemProducer aliased to 'hdfs'
systems.hdfs.samza.factory=org.apache.samza.system.hdfs.HdfsSystemFactory
systems.hdfs.producer.hdfs.writer.class=org.apache.samza.system.hdfs.writer.AvroDataFileHdfsWriter
{% endhighlight %}


If your output is non-avro, you can describe its format by implementing your own serializer.
{% highlight jproperties %}
systems.hdfs.producer.hdfs.writer.class=org.apache.samza.system.hdfs.writer.TextSequenceFileHdfsWriter
serializers.registry.my-serde-name.class=MySerdeFactory
systems.hdfs.samza.msg.serde=my-serde-name
{% endhighlight %}


#### Output directory structure

Samza allows you to control the base HDFS directory to write your output. You can also organize the output into sub-directories depending on the time your application ran, by configuring a date-formatter. 
{% highlight jproperties %}
systems.hdfs.producer.hdfs.base.output.dir=/user/me/analytics/clickstream_data
systems.hdfs.producer.hdfs.bucketer.date.path.format=yyyy_MM_dd
{% endhighlight %}

You can configure the maximum size of each file or the maximum number of records per-file. Once either limits have been reached, Samza will create a new file.

{% highlight jproperties %}
systems.hdfs.producer.hdfs.write.batch.size.bytes=134217728
systems.hdfs.producer.hdfs.write.batch.size.records=10000
{% endhighlight %}

### Security 

You can access Kerberos-enabled HDFS clusters by providing your principal and the path to your key-tab file. Samza takes care of automatically creating and renewing your Kerberos tokens periodically. 

{% highlight jproperties %}
job.security.manager.factory=org.apache.samza.job.yarn.SamzaYarnSecurityManagerFactory

# Kerberos principal
yarn.kerberos.principal=your-principal-name

# Path of the keytab file (local path)
yarn.kerberos.keytab=/tmp/keytab
{% endhighlight %}
