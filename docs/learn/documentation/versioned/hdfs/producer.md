---
layout: page
title: Isolation
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

### Writing to HDFS from Samza

The `samza-hdfs` module implements a Samza Producer to write to HDFS. The current implementation includes a ready-to-use `HdfsSystemProducer`, and two `HdfsWriter`s: One that writes messages of raw bytes to a `SequenceFile` of `BytesWritable` keys and values. The other writes UTF-8 `String`s to a `SequenceFile` with `LongWritable` keys and `Text` values.

### Configuring an HdfsSystemProducer

You can configure an HdfsSystemProducer like any other Samza system: using configuration keys and values set in a `job.properties` file.
You might configure the system producer for use by your `StreamTasks` like this:

```
# set the SystemFactory implementation to instantiate HdfsSystemProducer aliased to 'hdfs-clickstream'
systems.hdfs-clickstream.samza.factory=org.apache.samza.system.hdfs.HdfsSystemFactory

# define a serializer/deserializer for the hdfs-clickstream system
systems.hdfs-clickstream.samza.msg.serde=some-serde-impl

# consumer configs not needed for HDFS system, reader is not implemented yet 

# Assign a Metrics implementation via a label we defined earlier in the props file
systems.hdfs-clickstream.streams.metrics.samza.msg.serde=some-metrics-impl

# Assign the implementation class for this system's HdfsWriter
systems.hdfs-clickstream.producer.hdfs.writer.class=org.apache.samza.system.hdfs.writer.TextSequenceFileHdfsWriter

# Set HDFS SequenceFile compression type. Only BLOCK compression is supported currently
systems.hdfs-clickstream.producer.hdfs.compression.type=snappy

# The base dir for HDFS output. The default Bucketer for SequenceFile HdfsWriters
# is currently /BASE/JOB_NAME/DATE_PATH/FILES, where BASE is set below
systems.hdfs-clickstream.producer.hdfs.base.output.dir=/user/me/analytics/clickstream_data

# Assign the implementation class for the HdfsWriter's Bucketer
systems.hdfs-clickstream.producer.hdfs.bucketer.class=org.apache.samza.system.hdfs.writer.JobNameDateTimeBucketer

# Configure the DATE_PATH the Bucketer will set to bucket output files by day for this job run.
systems.hdfs-clickstream.producer.hdfs.bucketer.date.path.format=yyyy_MM_dd

# Optionally set the max output bytes per file. A new file will be cut and output
# continued on the next write call each time this many bytes are written.
systems.hdfs-clickstream.producer.hdfs.write.batch.size.bytes=134217728
```

The above configuration assumes a Metrics and Serde implemnetation has been properly configured against the `some-serde-impl` and `some-metrics-impl` labels somewhere else in the same `job.properties` file. Each of these properties has a reasonable default, so you can leave out the ones you don't need to customize for your job run.

