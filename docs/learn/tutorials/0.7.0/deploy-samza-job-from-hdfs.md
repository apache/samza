---
layout: page
title: Deploying a Samza job from HDFS
---

This tutorial uses [hello-samza](../../../startup/hello-samza/0.7.0/) to illustrate how to run a Samza job if you want to publish the Samza job's .tar.gz package to HDFS.

### Build a new Samza job package

Build a new Samza job package to include the hadoop-hdfs-version.jar.

* Add dependency statement in pom.xml of samza-job-package

```
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-hdfs</artifactId>
  <version>2.2.0</version>
</dependency>
```

* Add the following code to src/main/assembly/src.xml in samza-job-package.

```
<include>org.apache.hadoop:hadoop-hdfs</include>
```

* Create .tar.gz package

```
mvn clean pacakge
```

* Make sure hadoop-common-version.jar has the same version as your hadoop-hdfs-version.jar. Otherwise, you may still have errors.

### Upload the package

```
hadoop fs -put ./samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz /path/for/tgz
```

### Add HDFS configuration

Put the hdfs-site.xml file of your cluster into ~/.samza/conf directory. (The same place as the yarn-site.xml)

### Change properties file

Change the yarn.package.path in the properties file to your HDFS location.

```
yarn.package.path=hdfs://<hdfs name node ip>:<hdfs name node port>/path/to/tgz
```

Then you should be able to run the Samza job as described in [hello-samza](../../../startup/hello-samza/0.7.0/).
   
