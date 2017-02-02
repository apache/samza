---
layout: page
title: Reading from HDFS
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

You can configure your Samza job to read from HDFS files. The [HdfsSystemConsumer](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/HdfsSystemConsumer.java) can read from HDFS files. Avro encoded records are supported out of the box and it is easy to extend to support other formats (plain text, csv, json etc). See `Event format` section below.

### Environment

Your job needs to run on the same YARN cluster which hosts the HDFS you want to consume from.

### Partitioning

Partitioning works at the level of individual HDFS files. Each file is treated as a stream partition, while a directory that contains these files is a stream. For example, if you want to read from a HDFS path which contains 10 individual files, there will naturally be 10 partitions created. You can configure up to 10 Samza containers to process these partitions. If you want to read from a single HDFS file, there is currently no way to break down the consumption - you can only have one container to process the file.

### Event format

[HdfsSystemConsumer](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/HdfsSystemConsumer.java) currently supports reading from avro files. The received [IncomingMessageEnvelope](../api/javadocs/org/apache/samza/system/IncomingMessageEnvelope.html) contains three significant fields:

1. The key which is empty
2. The message which is set to the avro [GenericRecord](https://avro.apache.org/docs/1.7.6/api/java/org/apache/avro/generic/GenericRecord.html)
3. The stream partition which is set to the name of the HDFS file

To extend the support beyond avro files (e.g. json, csv, etc.), you can implement the interface [SingleFileHdfsReader](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/reader/SingleFileHdfsReader.java) (take a look at the implementation of [AvroFileHdfsReader](https://github.com/apache/samza/blob/master/samza-hdfs/src/main/java/org/apache/samza/system/hdfs/reader/AvroFileHdfsReader.java) as a sample).

### End of stream support

One major difference between HDFS data and Kafka data is that while a kafka topic has an unbounded stream of messages, HDFS files are bounded and have a notion of EOF.

You can choose to implement [EndOfStreamListenerTask](../api/javadocs/org/apache/samza/task/EndOfStreamListenerTask.html) to receive a callback when all partitions are at end of stream. When all partitions being processed by the task are at end of stream (i.e. EOF has been reached for all files), the Samza job exits automatically.

### Basic Configuration

Here is a few of the basic configs to set up HdfsSystemConsumer:

```
# The HDFS system consumer is implemented under the org.apache.samza.system.hdfs package,
# so use HdfsSystemFactory as the system factory for your system
systems.hdfs-clickstream.samza.factory=org.apache.samza.system.hdfs.HdfsSystemFactory

# You need to specify the path of files you want to consume in task.inputs
task.inputs=hdfs-clickstream.hdfs:/data/clickstream/2016/09/11

# You can specify a white list of files you want your job to process (in Java Pattern style)
systems.hdfs-clickstream.partitioner.defaultPartitioner.whitelist=.*avro

# You can specify a black list of files you don't want your job to process (in Java Pattern style),
# by default it's empty.
# Note that you can have both white list and black list, in which case both will be applied.
systems.hdfs-clickstream.partitioner.defaultPartitioner.blacklist=somefile.avro

```

### Security Configuration

The following additional configs are required when accessing HDFS clusters that have kerberos enabled:

```
# Use the SamzaYarnSecurityManagerFactory, which fetches and renews the Kerberos delegation tokens when the job is running in a secure environment.
job.security.manager.factory=org.apache.samza.job.yarn.SamzaYarnSecurityManagerFactory

# Kerberos principal
yarn.kerberos.principal=your-principal-name

# Path of the keytab file (local path)
yarn.kerberos.keytab=/tmp/keytab
```

### Advanced Configuration

Some of the advanced configuration you might need to set up:

```
# Specify the group pattern for advanced partitioning.
systems.hdfs-clickstream.partitioner.defaultPartitioner.groupPattern=part-[id]-.*
```

The advanced partitioning goes beyond the basic assumption that each file is a partition. With advanced partitioning you can group files into partitions arbitrarily. For example, if you have a set of files as [part-01-a.avro, part-01-b.avro, part-02-a.avro, part-02-b.avro, part-03-a.avro] that you want to organize into three partitions as (part-01-a.avro, part-01-b.avro), (part-02-a.avro, part-02-b.avro), (part-03-a.avro), where the numbers in the middle act as a "group identifier", you can then set this property to be "part-[id]-.*" (note that **[id]** is a reserved term here, i.e. you have to literally put it as **[id]**). The partitioner will apply this pattern to all file names and extract the "group identifier" ("[id]" in the pattern), then use the "group identifier" to group files into partitions.

```
# Specify the type of files your job want to process (support avro only for now)
systems.hdfs-clickstream.consumer.reader=avro

# Max number of retries (per-partition) before the container fails.
system.hdfs-clickstream.consumer.numMaxRetries=10

```

For the list of all configs, check out the configuration table page [here](../jobs/configuration-table.html)

### More Information
[HdfsSystemConsumer design doc](https://issues.apache.org/jira/secure/attachment/12827670/HDFSSystemConsumer.pdf)

## [Security &raquo;](../operations/security.html)