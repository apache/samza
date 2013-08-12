---
layout: page
title: Download
---

<!-- TODO update maven dependency versions appropriately -->

If you want to play around with Samza for the first time, go to [Hello Samza](/startup/hello-samza/0.7.0).

<!--
### Maven

All Samza JARs are published through Maven.

#### Artifacts

A Samza project that runs with Kafka and YARN should depend on the following artifacts.

    <dependency>
      <groupId>samza</groupId>
      <artifactId>samza-api</artifactId>
      <version>0.7.0</version>
    </dependency>
    <dependency>
      <groupId>samza</groupId>
      <artifactId>samza-core_2.9.2</artifactId>
      <version>0.7.0</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>samza</groupId>
      <artifactId>samza-serializers_2.9.2</artifactId>
      <version>0.7.0</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>samza</groupId>
      <artifactId>samza-yarn_2.9.2</artifactId>
      <version>0.7.0</version>
      <classifier>yarn-2.0.5-alpha</classifier>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>samza</groupId>
      <artifactId>samza-kafka_2.9.2</artifactId>
      <version>0.7.0</version>
      <scope>runtime</scope>
    </dependency>

#### Repositories

Samza is available in the Apache Maven repository.

    <repository>
      <id>apache-releases</id>
      <url>https://repository.apache.org/content/groups/public</url>
    </repository>

Snapshot builds are available in the Apache Maven snapshot repository.

    <repository>
      <id>apache-snapshots</id>
      <url>https://repository.apache.org/content/groups/snapshots</url>
    </repository>
-->

### Checking out and Building

If you're interested in working on Samza, or building the JARs from scratch, then you'll need to checkout and build the code. Samza does not have a binary release at this time. To check out and build Samza, run these commands.

```
git clone http://git-wip-us.apache.org/repos/asf/incubator-samza.git
cd incubator-samza
./gradlew clean build
```

See the README.md file for details on building.
