## What is Samza?

Apache Incubator Samza is a distributed stream processing framework. It uses <a target="_blank" href="http://kafka.apache.org">Apache Kafka</a> for messaging, and <a target="_blank" href="http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html">Apache Hadoop YARN</a> to provide fault tolerance, processor isolation, security, and resource management.

* **Simple API:** Unlike most low-level messaging system APIs, Samza provides a very simple call-back based "process message" API that should be familiar to anyone that's used Map/Reduce.
* **Managed state:** Samza manages snapshotting and restoration of a stream processor's state. Samza will restore a stream processor's state to a snapshot consistent with the processor's last read messages when the processor is restarted.
* **Fault tolerance:** Samza will work with YARN to restart your stream processor if there is a machine or processor failure.
* **Durability:** Samza uses Kafka to guarantee that messages will be processed in the order they were written to a partition, and that no messages will ever be lost.
* **Scalability:** Samza is partitioned and distributed at every level. Kafka provides ordered, partitioned, re-playable, fault-tolerant streams. YARN provides a distributed environment for Samza containers to run in.
* **Pluggable:** Though Samza works out of the box with Kafka and YARN, Samza provides a pluggable API that lets you run Samza with other messaging systems and execution environments.
* **Processor isolation:** Samza works with Apache YARN, which supports processor security through Hadoop's security model, and resource isolation through Linux CGroups.

Check out [Hello Samza](https://samza.incubator.apache.org/startup/hello-samza/0.7.0/) to try Samza. Read the [Background](https://samza.incubator.apache.org/learn/documentation/0.7.0/introduction/background.html) page to learn more about Samza.

### Building Samza

To build Samza, run:

    ./gradlew clean build

#### Scala and YARN

Samza builds with [Scala](http://www.scala-lang.org/) 2.10 and [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) 2.2.0, by default. Use the -PscalaVersion switches to change Scala versions. Samza supports building Scala with 2.9.2, or 2.10.

    ./gradlew -PscalaVersion=2.9.2 clean build

### Testing Samza

To run all tests:

    ./gradlew clean test

To run a single test:

    ./gradlew clean :samza-test:test -Dtest.single=TestStatefulTask

### Job Management

To run a job (defined in a properties file):

    ./gradlew samza-shell:runJob -PconfigPath=file:///path/to/job/config.properties

To inspect a job's latest checkpoint:

    ./gradlew samza-shell:checkpointTool -PconfigPath=file:///path/to/job/config.properties

To modify a job's checkpoint (assumes that the job is not currently running), give it a file with the new offset for each partition, in the format `systems.<system>.streams.<topic>.partitions.<partition>=<offset>`:

    ./gradlew samza-shell:checkpointTool -PconfigPath=file:///path/to/job/config.properties \
        -PnewOffsets=file:///path/to/new/offsets.properties

#### Maven

Samza uses Kafka, which is not managed by Maven. To use Kafka as though it were a Maven artifact, Samza installs Kafka into a local repository using the `mvn install` command. You must have Maven installed to build Samza.

### Developers

To get eclipse projects, run:

    ./gradlew eclipse

For IntelliJ, run:

    ./gradlew idea

### Pardon our Dust

Apache Samza is currently undergoing incubation at the [Apache Software Foundation](http://www.apache.org/).
