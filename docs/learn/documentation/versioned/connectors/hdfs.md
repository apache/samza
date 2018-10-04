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

## Overview

Samza applications can read and process data stored in HDFS. Likewise, you can also write processed results to HDFS.

### Environment Requirement

Your job needs to run on the same YARN cluster which hosts the HDFS you want to consume from (or write into).

## Consuming from HDFS

You can configure your Samza job to read from HDFS files with the [HdfsSystemConsumer](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/HdfsSystemConsumer.java). Avro encoded records are supported out of the box and it is easy to extend to support other formats (plain text, csv, json etc). See Event Format section below.

### Partitioning

Partitioning works at the level of individual directories and files. Each directory is treated as its own stream, while each of its files is treated as a partition. For example, when reading from a directory on HDFS with 10 files, there will be 10 partitions created. This means that you can have up-to 10 containers to process them. If you want to read from a single HDFS file, there is currently no way to break down the consumption - you can only have one container to process the file.

### Event format

Samza's HDFS consumer wraps each avro record read from HDFS into a message-envelope. The [Envelope](../api/javadocs/org/apache/samza/system/IncomingMessageEnvelope.html) contains three fields of interest:

1. The key, which is empty
2. The message, which is set to the avro [GenericRecord](https://avro.apache.org/docs/1.7.6/api/java/org/apache/avro/generic/GenericRecord.html)
3. The stream partition, which is set to the name of the HDFS file

To support input formats which are not avro, you can implement the [SingleFileHdfsReader](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/reader/SingleFileHdfsReader.java) interface (example: [AvroFileHdfsReader](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/reader/AvroFileHdfsReader.java))

### End of stream support

While streaming sources like Kafka are unbounded, files on HDFS have finite data and have a notion of end-of-file.

When reading from HDFS, your Samza job automatically exits after consuming all the data. You can choose to implement [EndOfStreamListenerTask](../api/javadocs/org/apache/samza/task/EndOfStreamListenerTask.html) to receive a callback when reaching end of stream. 

### Basic Configuration

Here is a few of the basic configs which are required to set up HdfsSystemConsumer:

{% highlight jproperties %}
# The HDFS system consumer is implemented under the org.apache.samza.system.hdfs package,
# so use HdfsSystemFactory as the system factory for your system
systems.hdfs.samza.factory=org.apache.samza.system.hdfs.HdfsSystemFactory

# Define the hdfs stream
streams.hdfs-clickstream.samza.system=hdfs

# You need to specify the path of files you want to consume
streams.hdfs-clickstream.samza.physical.name=hdfs:/data/clickstream/2016/09/11

# You can specify a white list of files you want your job to process (in Java Pattern style)
systems.hdfs.partitioner.defaultPartitioner.whitelist=.*avro

# You can specify a black list of files you don't want your job to process (in Java Pattern style),
# by default it's empty.
# Note that you can have both white list and black list, in which case both will be applied.
systems.hdfs.partitioner.defaultPartitioner.blacklist=somefile.avro
{% endhighlight %}

### Security Configuration

The following additional configs are required when accessing HDFS clusters that have kerberos enabled:

{% highlight jproperties %}
# When the job is running in a secure environment, use the SamzaYarnSecurityManagerFactory, which fetches and renews the Kerberos delegation tokens
job.security.manager.factory=org.apache.samza.job.yarn.SamzaYarnSecurityManagerFactory

# Kerberos principal
yarn.kerberos.principal=your-principal-name

# Path of the keytab file (local path)
yarn.kerberos.keytab=/tmp/keytab
{% endhighlight %}

### Advanced Configuration

Some of the advanced configuration you might need to set up:

{% highlight jproperties %}
# Specify the group pattern for advanced partitioning.
systems.hdfs-clickstream.partitioner.defaultPartitioner.groupPattern=part-[id]-.*

# Specify the type of files your job want to process (support avro only for now)
systems.hdfs-clickstream.consumer.reader=avro

# Max number of retries (per-partition) before the container fails.
system.hdfs-clickstream.consumer.numMaxRetries=10
{% endhighlight %}

The advanced partitioning goes beyond the basic assumption that each file is a partition. With advanced partitioning you can group files into partitions arbitrarily. For example, if you have a set of files as [part-01-a.avro, part-01-b.avro, part-02-a.avro, part-02-b.avro, part-03-a.avro] that you want to organize into three partitions as (part-01-a.avro, part-01-b.avro), (part-02-a.avro, part-02-b.avro), (part-03-a.avro), where the numbers in the middle act as a “group identifier”, you can then set this property to be “part-[id]-.” (note that "[id]" is a reserved term here, i.e. you have to literally put it as [id]). The partitioner will apply this pattern to all file names and extract the “group identifier” (“[id]” in the pattern), then use the “group identifier” to group files into partitions.

## Producing to HDFS

The samza-hdfs module implements a Samza Producer to write to HDFS. The current implementation includes a ready-to-use HdfsSystemProducer, and two HdfsWriters: One that writes messages of raw bytes to a SequenceFile of BytesWritable keys and values. Another writes out Avro data files including the schema automatically reflected from the POJO objects fed to it.

### Configuring an HdfsSystemProducer

You can configure an HdfsSystemProducer like any other Samza system: using configuration keys and values set in a job.properties file. You might configure the system producer for use by your StreamTasks like this:

{% highlight jproperties %}
# set the SystemFactory implementation to instantiate HdfsSystemProducer aliased to 'hdfs'
systems.hdfs.samza.factory=org.apache.samza.system.hdfs.HdfsSystemFactory

# Assign the implementation class for this system's HdfsWriter
systems.hdfs.producer.hdfs.writer.class=org.apache.samza.system.hdfs.writer.TextSequenceFileHdfsWriter
#systems.hdfs.producer.hdfs.writer.class=org.apache.samza.system.hdfs.writer.AvroDataFileHdfsWriter
# define a serializer/deserializer for the hdfs system
# DO NOT define (i.e. comment out) a SerDe when using the AvroDataFileHdfsWriter so it can reflect the schema
systems.hdfs.samza.msg.serde=some-serde-impl

# Assign a serde implementation to be used for the stream called "metrics"
systems.hdfs.streams.metrics.samza.msg.serde=some-metrics-impl

# Set compression type supported by chosen Writer.
# AvroDataFileHdfsWriter supports snappy, bzip2, deflate or none
systems.hdfs.producer.hdfs.compression.type=snappy

# The base dir for HDFS output. Output is structured into buckets. The default Bucketer for SequenceFile HdfsWriters
# is currently /BASE/JOB_NAME/DATE_PATH/FILES, where BASE is set below
systems.hdfs.producer.hdfs.base.output.dir=/user/me/analytics/clickstream_data

# Assign the implementation class for the HdfsWriter's Bucketer
systems.hdfs.producer.hdfs.bucketer.class=org.apache.samza.system.hdfs.writer.JobNameDateTimeBucketer

# Configure the DATE_PATH the Bucketer will set to bucket output files by day for this job run.
systems.hdfs.producer.hdfs.bucketer.date.path.format=yyyy_MM_dd

# Optionally set the max output bytes (records for AvroDataFileHdfsWriter) per file.
# A new file will be cut and output continued on the next write call each time this many bytes
# (records for AvroDataFileHdfsWriter) have been written.
systems.hdfs.producer.hdfs.write.batch.size.bytes=134217728
#systems.hdfs.producer.hdfs.write.batch.size.records=10000
{% endhighlight %}

The above configuration assumes a Metrics and Serde implementation has been properly configured against the some-metrics-impl and some-serde-impl and labels somewhere else in the same job.properties file. Each of these properties has a reasonable default, so you can leave out the ones you don’t need to customize for your job run.