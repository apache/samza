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

### Kinesis I/O: Quickstart

The Samza Kinesis connector allows you to interact with [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams),
Amazon’s data streaming service. The `hello-samza` project includes an example of processing Kinesis streams using Samza. Here is the complete [source code](https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/kinesis/KinesisHelloSamza.java) and [configs](https://github.com/apache/samza-hello-samza/blob/master/src/main/config/kinesis-hello-samza.properties).
You can build and run this example using this [tutorial](https://github.com/apache/samza-hello-samza#hello-samza).


###Data Format
Like a Kafka topic, a Kinesis stream can have multiple shards with producers and consumers.
Each message consumed from the stream is an instance of a Kinesis [Record](http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/Record).
Samza’s [KinesisSystemConsumer](https://github.com/apache/samza/blob/master/samza-aws/src/main/java/org/apache/samza/system/kinesis/consumer/KinesisSystemConsumer.java)
wraps the Record into a [KinesisIncomingMessageEnvelope](https://github.com/apache/samza/blob/master/samza-aws/src/main/java/org/apache/samza/system/kinesis/consumer/KinesisIncomingMessageEnvelope.java).

### Consuming from Kinesis

#### Basic Configuration

Here is the required configuration for consuming messages from Kinesis. 

{% highlight jproperties %}
// Define a Kinesis system factory with your identifier. eg: kinesis-system
systems.kinesis-system.samza.factory=org.apache.samza.system.kinesis.KinesisSystemFactory

// Kinesis consumer works with only AllSspToSingleTaskGrouperFactory
job.systemstreampartition.grouper.factory=org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory

// Define your streams
task.inputs=kinesis-system.input0

// Define required properties for your streams
systems.kinesis-system.streams.input0.aws.region=YOUR-STREAM-REGION
systems.kinesis-system.streams.input0.aws.accessKey=YOUR-ACCESS_KEY
sensitive.systems.kinesis-system.streams.input0.aws.secretKey=YOUR-SECRET-KEY
{% endhighlight %}

####Coordination
The Kinesis system consumer does not rely on Samza's coordination mechanism. Instead, it uses the Kinesis client library (KCL) for coordination and distributing available shards among available instances. Hence, you should
set your `grouper` configuration to `AllSspToSingleTaskGrouperFactory`.

{% highlight jproperties %}
job.systemstreampartition.grouper.factory=org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory
{% endhighlight %}

####Security

Each Kinesis stream in a given AWS [region](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html) can be accessed by providing an [access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys). An Access key consists of two parts: an access key ID (for example, `AKIAIOSFODNN7EXAMPLE`) and a secret access key (for example, `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`) which you can use to send programmatic requests to AWS. 

{% highlight jproperties %}
systems.kinesis-system.streams.input0.aws.region=YOUR-STREAM-REGION
systems.kinesis-system.streams.input0.aws.accessKey=YOUR-ACCESS_KEY
sensitive.systems.kinesis-system.streams.input0.aws.secretKey=YOUR-SECRET-KEY
{% endhighlight %}

### Advanced Configuration

#### Kinesis Client Library Configs
Samza Kinesis Connector uses the [Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html#kinesis-record-processor-overview-kcl)
(KCL) to access the Kinesis data streams. You can set any [KCL Configuration](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/coordinator/KinesisClientLibConfiguration.java)
for a stream by configuring it with the **systems.system-name.streams.stream-name.aws.kcl.*** prefix.

{% highlight jproperties %}
systems.system-name.streams.stream-name.aws.kcl.CONFIG-PARAM=CONFIG-VALUE
{% endhighlight %}

As an example, the below configuration is equivalent to invoking `kclClient#WithTableName(myTable)` on the KCL instance.
{% highlight jproperties %}
systems.system-name.streams.stream-name.aws.kcl.TableName=myTable
{% endhighlight %}

#### AWS Client configs
Samza allows you to specify any [AWS client configs](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html) to connect to your Kinesis instance.
You can configure any [AWS client configuration](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html) with the `systems.your-system-name.aws.clientConfig.*` prefix.

{% highlight jproperties %}
systems.system-name.aws.clientConfig.CONFIG-PARAM=CONFIG-VALUE
{% endhighlight %}

As an example, to set the *proxy host* and *proxy port* to be used by the Kinesis Client:
{% highlight jproperties %}
systems.system-name.aws.clientConfig.ProxyHost=my-proxy-host.com
systems.system-name.aws.clientConfig.ProxyPort=my-proxy-port
{% endhighlight %}

### Resetting Offsets

Unlike other connectors where Samza stores and manages checkpointed offsets, Kinesis checkpoints are stored in a [DynamoDB](https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-ddb.html) table.
These checkpoints are stored and managed by the KCL library internally. You can reset the checkpoints by configuring a different name for the DynamoDB table. 

{% highlight jproperties %}
// change the TableName to a unique name to reset checkpoints.
systems.kinesis-system.streams.input0.aws.kcl.TableName=my-app-table-name
{% endhighlight %}

When you reset checkpoints, you can configure your job to start consuming from either the earliest or latest offset in the stream.  

{% highlight jproperties %}
// set the starting position to either TRIM_HORIZON (oldest) or LATEST (latest)
systems.kinesis-system.streams.input0.aws.kcl.InitialPositionInStream=LATEST
{% endhighlight %}

Alternately, if you want to start from a particular offset in the Kinesis stream, you can login to the [AWS console](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ConsoleDynamoDB.html) and edit the offsets in your DynamoDB Table.
By default, the table-name has the following format: "\<job name\>-\<job id\>-\<kinesis stream\>".

### Known Limitations

The following limitations apply to Samza jobs consuming from Kinesis streams :

- Stateful processing (eg: windows or joins) is not supported on Kinesis streams. However, you can accomplish this by
chaining two Samza jobs where the first job reads from Kinesis and sends to Kafka while the second job processes the
data from Kafka.
- Kinesis streams cannot be configured as [bootstrap](https://samza.apache.org/learn/documentation/latest/container/streams.html)
or [broadcast](https://samza.apache.org/learn/documentation/latest/container/samza-container.html) streams.
- Kinesis streams must be used only with the [AllSspToSingleTaskGrouperFactory](https://github.com/apache/samza/blob/master/samza-core/src/main/java/org/apache/samza/container/grouper/stream/AllSspToSingleTaskGrouperFactory.java)
as the Kinesis consumer does the partition management by itself. No other grouper is currently supported.
- A Samza job that consumes from Kinesis cannot consume from any other input source. However, you can send your results
to any destination (eg: Kafka, EventHubs), and have another Samza job consume them.

## Producing to Kinesis

The KinesisSystemProducer for Samza is not yet implemented.

