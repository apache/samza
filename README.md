## What is Samza?

[Apache Samza](http://samza.apache.org/) is a distributed stream processing framework. It uses [Apache Kafka](http://kafka.apache.org) for messaging, and [Apache Hadoop YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) to provide fault tolerance, processor isolation, security, and resource management.

Samza's key features include:

* **Simple API:** Unlike most low-level messaging system APIs, Samza provides a very simple callback-based "process message" API comparable to MapReduce.
* **Managed state:** Samza manages snapshotting and restoration of a stream processor's state. When the processor is restarted, Samza restores its state to a consistent snapshot. Samza is built to handle large amounts of state (many gigabytes per partition).
* **Fault tolerance:** Whenever a machine in the cluster fails, Samza works with YARN to transparently migrate your tasks to another machine.
* **Durability:** Samza uses Kafka to guarantee that messages are processed in the order they were written to a partition, and that no messages are ever lost.
* **Scalability:** Samza is partitioned and distributed at every level. Kafka provides ordered, partitioned, replayable, fault-tolerant streams. YARN provides a distributed environment for Samza containers to run in.
* **Pluggable:** Though Samza works out of the box with Kafka and YARN, Samza provides a pluggable API that lets you run Samza with other messaging systems and execution environments.
* **Processor isolation:** Samza works with Apache YARN, which supports Hadoop's security model, and resource isolation through Linux CGroups.

Check out [Hello Samza](https://samza.apache.org/startup/hello-samza/0.8/) to try Samza. Read the [Background](https://samza.apache.org/learn/documentation/0.8/introduction/background.html) page to learn more about Samza.

### Building Samza

To build Samza from a git checkout, run:

    ./gradlew clean build

To build Samza from a source release, it is first necessary to download the gradle wrapper script above. This bootstrapping process requires Gradle to be installed on the source machine.  Gradle is available through most package managers or directly from [its website](http://www.gradle.org/).  To bootstrap the wrapper, run:

    gradle -b bootstrap.gradle

After the bootstrap script has completed, the regular gradlew instructions below are available.

#### Scala and YARN

Samza builds with [Scala](http://www.scala-lang.org/) 2.10 and [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) 2.4.0, by default. Use the -PscalaVersion switches to change Scala versions. Samza supports building Scala with 2.10.

    ./gradlew -PscalaVersion=2.10 clean build

### Testing Samza

To run all tests:

    ./gradlew clean test

To run a single test:

    ./gradlew clean :samza-test:test -Dtest.single=TestStatefulTask

To run key-value performance tests:

    ./gradlew samza-shell:kvPerformanceTest -PconfigPath=file://$PWD/samza-test/src/main/resources/perf/kv-perf.properties

To run all integration tests:

    ./bin/integration-tests.sh <dir>

### Job Management

To run a job (defined in a properties file):

    ./gradlew samza-shell:runJob -PconfigPath=file:///path/to/job/config.properties

To inspect a job's latest checkpoint:

    ./gradlew samza-shell:checkpointTool -PconfigPath=file:///path/to/job/config.properties

To modify a job's checkpoint (assumes that the job is not currently running), give it a file with the new offset for each partition, in the format `systems.<system>.streams.<topic>.partitions.<partition>=<offset>`:

    ./gradlew samza-shell:checkpointTool -PconfigPath=file:///path/to/job/config.properties \
        -PnewOffsets=file:///path/to/new/offsets.properties

### Developers

To get Eclipse projects, run:

    ./gradlew eclipse

For IntelliJ, run:

    ./gradlew idea

### Apache Software Foundation

Apache Samza is a top level project of the [Apache Software Foundation](http://www.apache.org/).

![Apache Software Foundation Logo](http://www.apache.org/images/feather.gif)
