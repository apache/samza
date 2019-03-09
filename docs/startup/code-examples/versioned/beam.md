---
layout: page
title: Beam Code Examples
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

The [samza-beam-examples](https://github.com/apache/samza-beam-examples) project contains examples to demonstrate running Beam pipelines with SamzaRunner locally, in Yarn cluster, or in standalone cluster with Zookeeper. More complex pipelines can be built from this project and run in similar manner.  

### Example Pipelines

The following examples are included:

1. [`WordCount`](https://github.com/apache/samza-beam-examples/blob/master/src/main/java/org/apache/beam/examples/WordCount.java) reads a file as input (bounded data source), and computes word frequencies. 

1. [`KafkaWordCount`](https://github.com/apache/samza-beam-examples/blob/master/src/main/java/org/apache/beam/examples/KafkaWordCount.java) does the same word-count computation but reading from a Kafka stream (unbounded data source). It uses a fixed 10-sec window to aggregate the counts.

### Run Examples

Each example can be run locally, in Yarn cluster or in standalone cluster. Here we use WordCount as an example.

#### Set Up

1. Download and install [JDK version 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html). Verify that the JAVA_HOME environment variable is set and points to your JDK installation.

1. Download and install [Apache Maven](http://maven.apache.org/download.cgi) by following Maven’s [installation guide](http://maven.apache.org/install.html) for your specific operating system.

1. A script named "grid" is included in this project which allows you to easily download and install Zookeeper, Kafka, and Yarn.
You can run the following to bring them all up running in your local machine:

```
$ scripts/grid bootstrap
```

All the downloaded package files will be put under `deploy` folder. Once the grid command completes, 
you can verify that Yarn is up and running by going to http://localhost:8088. You can also choose to
bring them up separately, e.g.:

```
$ scripts/grid install zookeeper
$ scripts/grid start zookeeper
```
   
#### Local Run
You can run directly within the project using maven:

```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--runner=SamzaRunner" -P samza-runner
```

#### Packaging Your Application
To execute the example in either Yarn or standalone, you need to package it first.
After packaging, we deploy and explode the tgz in the deploy folder:

```
 $ mkdir -p deploy/examples
 $ mvn package && tar -xvf target/samza-beam-examples-0.1-dist.tar.gz -C deploy/examples/
```

#### Standalone Cluster with Zookeeper
You can use the `run-beam-standalone.sh` script included in this repo to run an example
in standalone mode. The config file is provided as `config/standalone.properties`. Note by
default we create one single input partition for the whole input. To set the number of 
partitions, you can add "--maxSourceParallelism=" argument. For example, "--maxSourceParallelism=2"
will create two partitions of the input file, based on size.  

```
$ deploy/examples/bin/run-beam-standalone.sh org.apache.beam.examples.WordCount \
    --configFilePath=$PWD/deploy/examples/config/standalone.properties \
    --inputFile=/Users/xiliu/opensource/samza-beam-examples/pom.xml --output=word-counts.txt \
    --maxSourceParallelism=2
```

If the example consumes from Kafka, we can set a large "maxSourceParallelism" value so each kafka
partition be assigned to a Samza task (the total number of tasks will be bounded by 
maxSourceParallelism). E.g.

```
$ deploy/examples/bin/run-beam-standalone.sh org.apache.beam.examples.KafkaWordCount \
    --configFilePath=$PWD/deploy/examples/config/standalone.properties \
    --maxSourceParallelism=1024
```

####  Yarn Cluster
Similar to running standalone, we can use the `run-beam-yarn.sh` to run the examples
in Yarn cluster. The config file is provided as `config/yarn.properties`. To run the 
WordCount example in yarn:

```
 $ deploy/examples/bin/run-beam-yarn.sh org.apache.beam.examples.WordCount \
    --configFilePath=$PWD/deploy/examples/config/yarn.properties \
    --inputFile=/Users/xiliu/opensource/samza-beam-examples/pom.xml \
    --output=/tmp/word-counts.txt --maxSourceParallelism=2
```

Same as Standalone, we can provide a large "maxSourceParallelism" value to have better parallism
in Kafka case.

### Beyond Examples
Feel free to build more complex pipelines based on the examples above, and reach out to us:

* Subscribe and mail to [user@beam.apache.org](mailto:user@beam.apache.org) for any Beam questions.

* Subscribe and mail to [user@samza.apache.org](mailto:user@samza.apache.org) for any Samza questions.

### More Information

* [Apache Beam](http://beam.apache.org)
* [Apache Samza](https://samza.apache.org/)
* Quickstart: [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go)