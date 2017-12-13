---
layout: page
title: Connecting to Kinesis
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

You can configure your Samza jobs to process data from [AWS Kinesis](https://aws.amazon.com/kinesis/data-streams), Amazon's data streaming service. A `Kinesis data stream` is similar to a Kafka topic and can have multiple partitions. Each message consumed from a Kinesis data stream is an instance of [Record](http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/Record).

### Consuming from Kinesis:

Samza's [KinesisSystemConsumer](https://github.com/apache/samza/blob/master/samza-aws/src/main/java/org/apache/samza/system/kinesis/consumer/KinesisSystemConsumer.java) wraps the Record into a [KinesisIncomingMessageEnvelope](https://github.com/apache/samza/blob/master/samza-aws/src/main/java/org/apache/samza/system/kinesis/consumer/KinesisIncomingMessageEnvelope.java). The key of the message is set to partition key of the Record. The message is obtained from the Record body.

To configure Samza to consume from Kinesis streams:

```
# define a kinesis system factory with your identifier. eg: kinesis-system
systems.kinesis-system.samza.factory=org.apache.samza.system.eventhub.KinesisSystemFactory

# kinesis system consumer works with only AllSspToSingleTaskGrouperFactory
job.systemstreampartition.grouper.factory=org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory

# define your streams
task.inputs=kinesis-system.input0

# define required properties for your streams
systems.kinesis-system.streams.input0.aws.region=YOUR-STREAM-REGION
systems.kinesis-system.streams.input0.aws.accessKey=YOUR-ACCESS_KEY
sensitive.systems.kinesis-system.streams.input0.aws.secretKey=YOUR-SECRET-KEY

The tuple required to access the Kinesis data stream must be provided, namely the fields `YOUR-STREAM-REGION`, `YOUR-ACCESS-KEY`, `YOUR-SECRET-KEY`.
```

#### Advanced Configuration:

##### AWS Client Configs:

You can configure any [AWS client config](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html) with the prefix system.system-name.aws.clientConfig.*
```
system.system-name.aws.clientConfig.CONFIG-NAME=CONFIG-VALUE
```

As an example, to set a proxy host and proxy port for the AWS Client:
```
systems.system-name.aws.clientConfig.ProxyHost=my-proxy-host.com
systems.system-name.aws.clientConfig.ProxyPort=my-proxy-port
```

##### KCL Configs:

Similarly, you can set any [Kinesis Client Library config](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java) for a stream by configuring it under systems.system-name.streams.stream-name.aws.kcl.*
```
systems.system-name.streams.stream-name.aws.kcl.CONFIG-NAME=CONFIG-VALUE
```

As an example, to reset the checkpoint and set the starting position for a stream:
```
systems.kinesis-system.streams.input0.aws.kcl.TableName=my-app-table-name
# set the starting position to either TRIM_HORIZON (oldest) or LATEST (latest)
systems.kinesis-system.streams.input0.aws.kcl.InitialPositionInStream=my-start-position
```

#### Limitations

The following limitations apply for Samza jobs consuming from Kinesis streams using the Samza consumer:
* Stateful processing (eg: windows or joins) is not supported on Kinesis streams. However, you can accomplish this by chaining two Samza jobs where the first job reads from Kinesis and sends to Kafka (or EventHubs) while the second job processes the data from Kafka (or EventHubs).
* Kinesis streams cannot be configured as [bootstrap](https://samza.apache.org/learn/documentation/latest/container/streams.html) or [broadcast](https://samza.apache.org/learn/documentation/latest/container/samza-container.html) streams.
* Kinesis streams must be used with the [AllSspToSingleTaskGrouperFactory](https://github.com/apache/samza/blob/master/samza-core/src/main/java/org/apache/samza/container/grouper/stream/AllSspToSingleTaskGrouperFactory.java). No other grouper is supported.
* A Samza job that consumes from Kinesis cannot consume from any other input source. However, you can send your results to any destination (eg: Kafka, EventHubs), and have another Samza job consume them.


### Producing to Kinesis:

The KinesisSystemProducer for Samza is not yet implemented.


### How to configure Samza job to consume from Kinesis data stream ?

This tutorial uses [hello-samza](../../../startup/hello-samza/{{site.version}}/) to illustrate how to run a Samza job on Yarn consuming from Kinesis data stream.

For this tutorial, we will pick KinesisHelloSamza from the hello-samza examples. Please create a Kinesis data stream if you have not already.

#### Update properties file

Update the following properties in the kinesis-hello-samza.properties file:

* task.inputs=kinesis.<kinesis-stream>
* systems.kinesis.streams.<kinesis-stream>.aws.region=<kinesis-stream-region>
* systems.kinesis.streams.<kinesis-stream>.aws.accessKey=<your-access-key>
* sensitive.systems.kinesis.streams.<kinesis-stream>.aws.region=<your-secret-key>

Then you should be able to run the Samza job on Yarn as described in [hello-samza](../../../startup/hello-samza/{{site.version}}/). Check the log file for the Kinesis records that are being read from the Kinesis data stream.
