---
layout: default
---

## What is Samza?

Apache Samza is a distributed stream processing framework. It uses <a target="_blank" href="http://kafka.apache.org">Apache Kafka</a> for messaging, and <a target="_blank" href="http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html">Apache Hadoop YARN</a> to provide fault tolerance, processor isolation, security, and resource management.

* **Simpe API:** Unlike most low-level messaging system APIs, Samza provides a very simple call-back based "process message" API that should be familiar to anyone that's used Map/Reduce.
* **Managed state:** Samza manages snapshotting and restoration of a stream processor's state. Samza will restore a stream processor's state to a snapshot consistent with the processor's last read messages when the processor is restarted.
* **Fault tolerance:** Samza will work with YARN to restart your stream processor if there is a machine or processor failure.
* **Durability:** Samza uses Kafka to guarantee that messages will be processed in the order they were written to a partition, and that no messages will ever be lost.
* **Scalability:** Samza is partitioned and distributed at every level. Kafka provides ordered, partitioned, re-playable, fault-tolerant streams. YARN provides a distributed environment for Samza containers to run in.
* **Pluggable:** Though Samza works out of the box with Kafka and YARN, Samza provides a pluggable API that lets you run Samza with other messaging systems and execution environments.
* **Processor isolation:** Samza works with Apache YARN, which supports processor security through Hadoop's security model, and resource isolation through Linux CGroups.

Check out [Hello Samza](/startup/hello-samza/0.7.0) to try Samza. Read the [Background](/learn/documentation/0.7.0/introduction/background.html) page to learn more about Samza.

### Limitations

We are just moving our code to open source. This newly open sourced version has a few limitations:

 * It depends on a snapshot version of Kafka that will not officially be released for a few months.
 * This branch represents our trunk, not the production version at LinkedIn. This rollout is pending.
 * We have not yet fully implemented our plans around fault-tolerance semantics.

### Pardon our Dust

Apache Samza is currently undergoing incubation at the [Apache Software Foundation](http://www.apache.org/).

![Apache Incubator Logo](img/apache-egg-logo.png)
