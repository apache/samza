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

#### Advanced Configuration

Samza also provides the following Kinesis system specific passthrough configs:
* [AWS client configs](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html)
* [AWS Kinesis Client Library configs](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java)

```
# configure AWS client passthrough configs.
systems.kinesis-system.aws.clientConfig.CLIENT-CONFIG=YOUR-CLIENT-CONFIG-VALUE

# set a proxy
systems.kinesis-system.aws.clientConfig.ProxyHost=YOUR-PROXY-HOST
systems.kinesis-system.aws.clientConfig.ProxyPort=YOUR-PROXY-PORT

# configure AWS Kinesis Client Library passthrough configs.
systems.kinesis-system.streams.input0.aws.kcl.KCL-CONFIG=YOUR-KCL-CONFIG-VALUE

# to reset checkpoint and read from the oldest (TRIM_HORIZON) or the default upcoming offset (LATEST), change the table name and set the read position.
systems.kinesis-system.streams.input0.aws.kcl.TableName=YOUR-APP-TABLE-NAME
systems.kinesis-system.streams.input0.aws.kcl.InitialPositionInStream=YOUR-INIT-READ-POSITION
```

#### Limitations

The following limitations apply for Samza jobs consuming from Kinesis streams using the Samza consumer:
* No support for stateful processing: It could be done as a second stage job after repartitioning to kafka in the first stage.
* No support for broadcast streams.
* Kinesis streams cannot be configured as bootstrap streams.
* SystemStreamPartitionGroupers other than AllSspToSingleTaskGrouper are not supported.
* A single Samza job cannot be configured to consume from a combination of Kinesis and other system inputs (like Kafka, HDFS, EventHubs, etc).


### Producing to Kinesis:

The KinesisSystemProducer for Samza is not yet implemented.