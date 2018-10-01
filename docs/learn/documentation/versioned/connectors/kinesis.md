---
layout: page
title: Kinesis Connector
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

The Samza Kinesis connector provides access to [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams),
Amazon’s data streaming service. A Kinesis Data Stream is similar to a Kafka topic and can have multiple partitions.
Each message consumed from a Kinesis Data Stream is an instance of [Record](http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/Record).
Samza’s [KinesisSystemConsumer](https://github.com/apache/samza/blob/master/samza-aws/src/main/java/org/apache/samza/system/kinesis/consumer/KinesisSystemConsumer.java)
wraps the Record into a [KinesisIncomingMessageEnvelope](https://github.com/apache/samza/blob/master/samza-aws/src/main/java/org/apache/samza/system/kinesis/consumer/KinesisIncomingMessageEnvelope.java).

## Consuming from Kinesis

### Basic Configuration

You can configure your Samza jobs to process data from Kinesis Streams. To configure Samza job to consume from Kinesis
streams, please add the below configuration:

{% highlight jproperties %}
// define a kinesis system factory with your identifier. eg: kinesis-system
systems.kinesis-system.samza.factory=org.apache.samza.system.eventhub.KinesisSystemFactory

// kinesis system consumer works with only AllSspToSingleTaskGrouperFactory
job.systemstreampartition.grouper.factory=org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory

// define your streams
task.inputs=kinesis-system.input0

// define required properties for your streams
systems.kinesis-system.streams.input0.aws.region=YOUR-STREAM-REGION
systems.kinesis-system.streams.input0.aws.accessKey=YOUR-ACCESS_KEY
sensitive.systems.kinesis-system.streams.input0.aws.secretKey=YOUR-SECRET-KEY
{% endhighlight %}

The tuple required to access the Kinesis data stream must be provided, namely the following fields:<br>
**YOUR-STREAM-REGION**, **YOUR-ACCESS-KEY**, **YOUR-SECRET-KEY**.


### Advanced Configuration

#### AWS Client configs
You can configure any [AWS client config](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html)
with the prefix **systems.system-name.aws.clientConfig.***

{% highlight jproperties %}
systems.system-name.aws.clientConfig.CONFIG-PARAM=CONFIG-VALUE
{% endhighlight %}

As an example, to set a *proxy host* and *proxy port* for the AWS Client:

{% highlight jproperties %}
systems.system-name.aws.clientConfig.ProxyHost=my-proxy-host.com
systems.system-name.aws.clientConfig.ProxyPort=my-proxy-port
{% endhighlight %}

#### Kinesis Client Library Configs
Samza Kinesis Connector uses [Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html#kinesis-record-processor-overview-kcl)
(KCL) to access the Kinesis data streams. You can set any [Kinesis Client Lib Configuration](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/coordinator/KinesisClientLibConfiguration.java)
for a stream by configuring it under **systems.system-name.streams.stream-name.aws.kcl.***

{% highlight jproperties %}
systems.system-name.streams.stream-name.aws.kcl.CONFIG-PARAM=CONFIG-VALUE
{% endhighlight %}

Obtain the config param from the public functions in [Kinesis Client Lib Configuration](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/coordinator/KinesisClientLibConfiguration.java)
by removing the *"with"* prefix. For example: config param corresponding to **withTableName()** is **TableName**.

### Resetting Offsets

The source of truth for checkpointing while using Kinesis Connector is not the Samza checkpoint topic but Kinesis itself.
The Kinesis Client Library (KCL) [uses DynamoDB](https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-ddb.html)
to store it’s checkpoints. By default, Kinesis Connector reads from the latest offset in the stream.

To reset the checkpoints and consume from earliest/latest offset of a Kinesis data stream, please change the KCL TableName
and set the appropriate starting position for the stream as shown below.

{% highlight jproperties %}
// change the TableName to a unique name to reset checkpoint.
systems.kinesis-system.streams.input0.aws.kcl.TableName=my-app-table-name
// set the starting position to either TRIM_HORIZON (oldest) or LATEST (latest)
systems.kinesis-system.streams.input0.aws.kcl.InitialPositionInStream=my-start-position
{% endhighlight %}

To manipulate checkpoints to start from a particular position in the Kinesis stream, in lieu of Samza CheckpointTool,
please login to the AWS Console and change the offsets in the DynamoDB Table with the table name that you have specified
in the config above. By default, the table name has the following format:
"\<job name\>-\<job id\>-\<kinesis stream\>".

### Known Limitations

The following limitations apply to Samza jobs consuming from Kinesis streams using the Samza consumer:

- Stateful processing (eg: windows or joins) is not supported on Kinesis streams. However, you can accomplish this by
chaining two Samza jobs where the first job reads from Kinesis and sends to Kafka while the second job processes the
data from Kafka.
- Kinesis streams cannot be configured as [bootstrap](https://samza.apache.org/learn/documentation/latest/container/streams.html)
or [broadcast](https://samza.apache.org/learn/documentation/latest/container/samza-container.html) streams.
- Kinesis streams must be used ONLY with the [AllSspToSingleTaskGrouperFactory](https://github.com/apache/samza/blob/master/samza-core/src/main/java/org/apache/samza/container/grouper/stream/AllSspToSingleTaskGrouperFactory.java)
as the Kinesis consumer does the partition management by itself. No other grouper is supported.
- A Samza job that consumes from Kinesis cannot consume from any other input source. However, you can send your results
to any destination (eg: Kafka, EventHubs), and have another Samza job consume them.

## Producing to Kinesis

The KinesisSystemProducer for Samza is not yet implemented.

