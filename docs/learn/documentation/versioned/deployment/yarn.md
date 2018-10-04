---
layout: page
title: Run on YARN.
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

- [Introduction](#introduction)
- [Starting your application on YARN](#starting-your-application-on-yarn)
    - [Setting up a single node YARN cluster](#setting-up-a-single-node-yarn-cluster-optional)
    - [Submitting the application to YARN](#submitting-the-application-to-yarn)
- [Application Master UI](#application-master-ui)
- [Viewing logs](#viewing-logs)
- [Configuration](#configuration)
    - [Configuring parallelism](#configuring-parallelism)
    - [Configuring resources](#configuring-resources)
        - [Memory](#memory)
        - [CPU](#cpu)
    - [Configuring retries](#configuring-retries)
    - [Configuring RM high-availability and NM work-preserving recovery](#configuring-rm-high-availability-and-nm-work-preserving-recovery)
        - [Resource Manager high-availability](#resource-manager-high-availability)
        - [NodeManager work-preserving recovery](#nodemanager-work-preserving-recovery)
    - [Configuring host-affinity](#configuring-host-affinity)
    - [Configuring security](#configuring-security)
        - [Delegation token management strategy](#delegation-token-management-strategy)
        - [Security Components](#security-components)
            - [SecurityManager](#securitymanager)
        - [Security Configuration](#security-configuration)
            - [Job](#job)
            - [YARN](#yarn)
- [Coordinator Internals](#coordinator-internals)


# Introduction

YARN (Yet Another Resource Negotiator) is part of the Hadoop project and provides the ability to run distributed applications on a cluster. A YARN cluster minimally consists of a Resource Manager (RM) and multiple Node Managers (NM). The RM is responsible for coordinating allocations and tracks resources available on the NMs. The NM is an agent that executes on each node in the cluster and is responsible for running containers (user processes), monitoring their resource usage and reporting the same to the ResourceManager. Applications are run on the cluster by implementing a coordinator called an ApplicationMaster (AM). The AM is responsible for requesting resources (CPU, Memory etc) from the Resource Manager (RM) on behalf of the application. The RM allocates the requested resources on one or more NMs that   can accomodate the request made.

Samza provides an implementation of the AM in order to run a jobs alongside other application deployed on YARN. The AM makes decisions such as requesting allocation of containers, which machines a Samza job’s containers should run on, what to do when a container fails etc.


# Starting your application on YARN

## Setting up a single node YARN cluster (optional)

If you already have a YARN cluster setup to deploy jobs, please jump to [Submitting the application to YARN](#submitting-the-application-to-yarn). If not the following section will help set up a single node cluster to test a Samza job deploy.

We can use the `grid` script which is part of the [hello-samza](https://github.com/apache/samza-hello-samza/) repository to setup a single node YARN cluster (and optionally a Zookeeper and Kafka cluster as well).

Run the following to setup a single node YARN cluster:

```bash
./grid install yarn
./grid start yarn
```

## Submitting the application to YARN

Assuming you have a YARN cluster setup, let us take a look at building your application and deploying it to YARN. Samza provides shell scripts as part of the `samza-shell` module that help in submitting the application to YARN and you should include it as part of your dependencies jobs dependencies.

```xml
<dependency>
    <groupId>org.apache.samza</groupId>
    <artifactId>samza-shell</artifactId>
    <version>${samza.version}</version>
</dependency>
```

Samza jobs are usually deployed in a tarball and archive should contain the following as top-level directories.

```bash
samza-job-artifact-folder
├── bin
│   ├── run-app.sh
│   ├── run-class.sh
│   └── ...
├── config
│   └── application.properties
└── lib
    ├── samza-api-0.14.0.jar
    ├── samza-core_2.11-0.14.0.jar
    ├── samza-kafka_2.11-0.14.0.jar
    ├── samza-yarn_2.11-0.14.0.jar
    └── ...
```
The scripts in the `samza-shell` module make the assumption that the built artifact (tarball) has the exact directory structure as seen above. The scripts in the samza-shell module should be copied to a bin directory and all jars need to be part of lib as seen above. The hello-samza project is a good example on setting the structure of your application’s build.

Once the job is built, the `run-app.sh` script can be used to submit the application to the Resource Manager. The script takes 2 CLI parameters - the config factory and the config file for the application. It can be invoked as follows:

```bash
$ /path/to/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://path/to/config/application.properties
```

Make sure that the following configurations are set in your configs.

```properties
job.factory.class=org.apache.samza.job.yarn.YarnJobFactory
yarn.package.path=https://url/to/artifact/artifact-version-dist.tar.gz
```

# Application Master UI

The AM implementation in Samza exposes metadata about the job via both a JSON REST interface and a Web UI.
This Web UI can be accessed by clicking the Tracking UI (*ApplicationMaster*) link on the YARN RM dashboard.

![diagram-medium](/img/{{site.version}}/learn/documentation/yarn/yarn-am-ui.png)

The Application Master UI provides you the ability to view:

 - Job level runtime metadata
![diagram-small](/img/{{site.version}}/learn/documentation/yarn/am-runtime-metadata.png)


 - Container information
![diagram-small](/img/{{site.version}}/learn/documentation/yarn/am-container-info.png)

 - Job model (SystemStreamPartition to Task and Container mapping)
![diagram-small](/img/{{site.version}}/learn/documentation/yarn/am-job-model.png)


- Runtime configs
![diagram-small](/img/{{site.version}}/learn/documentation/yarn/am-runtime-configs.png)


# Viewing logs

Each container produces logs and they can be easily accessed via the Container information page in ApplicationMaster UI described in the previous section. Clicking on the container name under the Running or Failed container section will take you to the logs page that corresponds to that specific container.

If there is a need to analyze logs across containers, it is recommended to set up a centralized logging system like ELK (Elasticsearch, Logstash and Kibana). Samza provides a StreamAppender that supports emitting your logs to Kafka for this purpose. The logs written to the stream can then be ingested by service like Logstash and indexed in Elasticsearch.


# Configuration

In the following section let's take a look at the different tunables that exist as part of Samza to control your deployment on YARN

## Configuring parallelism

As a refresher, Samza scales your applications by breaking them into multiple tasks. On YARN, these tasks are executed on one or more containers, each of which is a Java process. You can control the number of containers allocated to your job by configuring `cluster-manager.container.count`. For example If we had 2 input topics with 10 partitions each processor would consist of 10 Tasks, each processing 2 partitions each. Setting `cluster-manager.container.count` to 1 would run all 10 tasks in one JVM process, setting it to 2 will distribute the tasks equally among 2 JVM processes and so on.

Please note that it is not possible to distribute 10 tasks across more than 10 containers, therefore the upper bound for `cluster-manager.container.count` is less than or equal to the number of Tasks in your job (or more generally the max of number of partitions among all input streams).


## Configuring resources

When running Samza jobs in a shared environment, the stream processors can have an impact on each other’s performance. YARN prevents these issues by providing isolation when running applications on the cluster by enforcing strict limits on resources that each application is allowed to use. YARN (2.7.*) currently supports resource management for memory and CPU.

### Memory

All containers requests by the Application Master will have a max-memory size defined when they’re created. Samza supports configuring these memory limits using `cluster-manager.container.memory.mb` and `yarn.am.container.memory.mb`. If your container exceeds the configured memory-limits, it is automatically killed by YARN. Keep in mind that this is the maximum memory YARN will allow a Samza Container or ApplicationMaster to have and you will still need to configure your heap settings appropriately using `task.opts`, when using the JVM.

As a cluster administrator if you are running other processes on the same box as the Node Managers (eg: samza-rest) you will want to reserve appropriate amount of memory by configuring `yarn.nodemanager.resource.system-reserved-memory-mb`. Another behaviour to keep in mind is that the Resource Manager allocates resource on the cluster in increments of `yarn.scheduler.minimum-allocation-mb` and `yarn.scheduler.minimum-allocation-vcores`, therefore requesting allocations that are not multiples of the above configs can lead to resource fragmentation.


### CPU
Similar to memory configurations all containers also are CPU bound to a max number of vCores (Virtual cores) on a NM that they are configured to use. YARN has the concept of a virtual core which is generally set to the number of physical cores on the NMs, but can be bump to a higher number if you want to over-provision the NMs with respect to the CPU. Samza supports configuring the vCore of each container by setting `cluster-manager.container.cpu.cores`.

Unlike memory, which YARN can enforce limits by itself (by looking at the /proc folder), YARN can’t enforce CPU isolation, since this must be done at the Linux kernel level. One of YARN’s features is its support for Linux CGroups (used to control process utilization at the kernel level in Linux). If YARN is setup to use CGroups, then it will guarantee that a container will get at least the amount of CPU that it requires. Currently, by default YARN will give you more CPU to the container, if it’s available. If enforcing “at most” CPU usage for more predictable performance by your container at the cost of underutilization you can set `yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage` to `true`, see [this article](https://hortonworks.com/blog/apache-hadoop-yarn-in-hdp-2-2-isolation-of-cpu-resources-in-your-hadoop-yarn-clusters/) for more details. For an indepth look at using YARN with CGroups take a look at [this blog post](http://riccomini.name/posts/hadoop/2013-06-14-yarn-with-cgroups/).

## Configuring retries

Failures are common when running a distributed system and the AM is used to handle Samza Container failures gracefully by automatically restarting containers on failure.
It should also be noted that if a Samza Container keeps failing constantly it could indicate a deeper problem and we should kill the job rather than having the AM restart it indefinitely. `cluster-manager.container.retry.count` can be used to set the maximum number of times a failed container will be restarted within a time window (configured with `cluster-manager.container.retry.window.ms`), before shutting down the job.
YARN also provides us a way to automatically restart the job if the AM process fails due to external issues (network partitions, hardware failures etc). By configuring the value of `yarn.resourcemanager.am.max-attempts` YARN will automatically restart the AM process for a fixed number of times before requiring manual intervention to start the job again.

## Configuring RM high-availability and NM work-preserving recovery

Although this section is not Samza specific, it talks about some of the best practices for running a YARN cluster in production specifically around running a highly-available Resource Manager and NodeManager work preserving recovery.

### Resource Manager high-availability

The Resource Manager (RM) component of a YARN cluster is the source of truth regarding resource utilization and resource scheduling in the cluster. Losing the host running the RM process would kill every single application running on the cluster - making it a single point of failure. The High Availability feature introduced in Hadoop 2.4 adds redundancy in the form of standby Resource Managers to remove this single point of failure.

In order to configure YARN to run the highly available Resource Manager process set your yarn-site.xml file with the following configs:

```xml
<property>
  <name>yarn.resourcemanager.ha.enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.resourcemanager.cluster-id</name>
  <value>cluster1</value>
</property>
<property>
  <name>yarn.resourcemanager.ha.rm-ids</name>
  <value>rm1,rm2</value>
</property>
<property>
  <name>yarn.resourcemanager.hostname.rm1</name>
  <value>master1</value>
</property>
<property>
  <name>yarn.resourcemanager.hostname.rm2</name>
  <value>master2</value>
</property>
<property>
  <name>yarn.resourcemanager.webapp.address.rm1</name>
  <value>master1:8088</value>
</property>
<property>
  <name>yarn.resourcemanager.webapp.address.rm2</name>
  <value>master2:8088</value>
</property>
<property>
  <name>yarn.resourcemanager.zk-address</name>
  <value>zk1:2181,zk2:2181,zk3:2181</value>
</property>
```

### NodeManager work-preserving recovery

Turning on work-preserving recovery for the NM gives you the ability to perform maintenance on the cluster (kill NM process for a short duration) without having the containers that run on the node also get killed. You can turn on this feature by setting `yarn.nodemanager.recovery.enabled` to `true` in `yarn-site.xml`

It is also recommended that you change the value of `yarn.nodemanager.recovery.dir` as by default this directory is set to `${hadoop.tmp.dir}/yarn-nm-recovery` where `hadoop.tmp.dir` is set to `/tmp/hadoop-${user.name}` and usually the contents of the `/tmp` directory are not preserved across a reboots.


## Configuring host-affinity

When a stateful Samza job is deployed in YARN, the state stores for the tasks are co-located in the current working directory of YARN’s application attempt.

```properties
container_working_dir=${yarn.nodemanager.local-dirs}/usercache/${user}/appcache/application_${appid}/container_${contid}/
# Data Stores
ls ${container_working_dir}/state/${store-name}/${task_name}/
```

This allows the Node Manager’s (NM) DeletionService to clean-up the working directory once the application completes or fails. In order to re-use local state store, the state store needs to be persisted outside the scope of NM’s deletion service. The cluster administrator should set this location as an environment variable in YARN  ( `LOGGED_STORE_BASE_DIR`).

Since we store the state stores outside of the container’s working directory it is necessary to periodically clean-up unused or orphaned state stores on the machines to manage disk-space. This can be done by running a clean up periodically from the samza-rest service (*LocalStoreMonitor*) that is meant to be deployed on all Node Manager hosts.


## Configuring security

You can run a Samza job on a secure YARN cluster. YARN uses Kerberos as its authentication and authorization mechanism. Take a look at the official YARN [documentation](https://hadoop.apache.org/docs/r2.7.4/hadoop-project-dist/hadoop-common/SecureMode.html) page for more details.

### Delegation token management strategy

One of the challenges for long-lived applications running on a secure YARN cluster is its token renewal strategy. Samza handles this by having the AM periodically re-authenticate itself with the given principal and keytab. It periodically creates new delegation tokens and stores them in a job specific staging directory on HDFS accessible only by the AM and its Containers. In this process each running container will get new delegation tokens from the credentials file on HDFS before the current ones expire. The AM and Containers don’t need to communicate with each other in this process and each side proceeds independently by accessing the tokens on HDFS.

By default, any HDFS delegation token has a maximum life of 7 days (configured by `dfs.namenode.delegation.token.max-lifetime` in `hdfs-site.xml`) and the token is normally renewed every 24 hours (configured by `dfs.namenode.delegation.token.renew-interval` in `hdfs-site.xml`). What if the Application Master dies and needs restarts after 7 days? The original HDFS delegation token stored in the launcher context will be invalid no matter what. Luckily, Samza can rely on Resource Manager to handle this scenario. See the Configuration section below for details.

### Security Components

#### SecurityManager

When ApplicationMaster starts, it spawns `SamzaAppMasterSecurityManager`, which runs on its separate thread. The `SamzaAppMasterSecurityManager` is responsible for periodically logging in through the given Kerberos keytab and regenerates the HDFS delegation tokens regularly. After each run, it writes new tokens on a pre-defined job specific directory on HDFS. The frequency of this process is determined by `yarn.token.renewal.interval.seconds`.

Each container, upon start, runs a `SamzaContainerSecurityManager`. It reads from the credentials file on HDFS and refreshes its delegation tokens at the same interval.

### Security configuration

For the Samza job, the following job configurations are required on a YARN cluster with security enabled.

#### Job

```properties
job.security.manager.factory=org.apache.samza.job.yarn.SamzaYarnSecurityManagerFactory
```

#### YARN

```properties
yarn.kerberos.principal=user/localhost
yarn.kerberos.keytab=/etc/krb5.keytab.user
yarn.token.renewal.interval.seconds=86400
```
Configure the Hadoop cluster to enable Resource Manager to recreate and renew the delegation token on behalf of the application user. This will address the following 2 scenarios.

- When Application Master dies unexpectedly and needs a restart after 7 days (the default maximum lifespan a delegation token can be renewed).
- When the Samza job terminates and log aggregation is turned on for the job. Node managers need to be able to upload all the local application logs to HDFS.

Enable the resource manager as a privileged user in yarn-site.xml.

```xml
<property>
    <name>yarn.resourcemanager.proxy-user-privileges.enabled</name>
    <value>true</value>
</property>
```

Make `yarn` as a proxy user, in `core-site.xml`

```xml
<property>
    <name>hadoop.proxyuser.yarn.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.yarn.groups</name>
    <value>*</value>
</property>
```

# Coordinator Internals

The `ClusterBasedJobCoordinator` is used as the control hub for a running Samza job in a cluster like YARN. Among other things it is responsible for bootstrapping configs from the Coordinator Stream on job startup, constructing the JobModel, managing container allocations and handling callbacks from the cluster manager (in YARN’s case the Resource Manager). Just like most other components in the framework, Samza has a plugable interface for managing container allocations and is configured using the key `samza.cluster-manager.factory`.


The `ClusterBasedJobCoordinator` contains a component called the `ContainerProcessManager` to handle metadata regarding container allocations. It uses the information (eg: host affinity) obtained from configs and the `CoordinatorStream` in order to make container allocation requests to the cluster manager (RM). In the case of YARN the config for `samza.cluster-manager.factory` which encapsulates the Application Master, is configured to `org.apache.samza.job.yarn.YarnResourceManagerFactory` and the `ContainerProcessManager` uses `YarnResourceManager` to interact with the RM.

![diagram-small](/img/{{site.version}}/learn/documentation/yarn/coordinator-internals.png)


The following is a walkthrough of the different actions taken when the `run-job.sh` script is run:
- When the job is submitted using `run-app.sh` the JobRunner invoked as part of this script first writes all the configs to the coordinator stream.
- The JobRunner then uses the configured StreamJob (YarnJob) to submit the request to start the AM to the RM.
- The ResourceManager allocates the AM on an available NM and starts the ClusterBasedJobCoordinator.
- The ClusterBasedJobCoordinator bootstraps the configs written to the Coordinator Stream in step (1) and constructs the JobModel, check for host-affinity if configured and instantiates the ClusterResourceManager (YarnClusterResourceManager).
- The YarnClusterResourceManager is then used to make requests to the RM to start job.container.count number of containers. The RM then issues callbacks to the process when the containers are allocated.
- When the containers are returned by the RM, the YarnClusterResourceManager allocates a SamzaContainer ID to the YARN containers to indicate which subset of tasks in the JobModel the YARN container should process on startup.
- When the containers start up, they read the configs and the JobModel from the configs and use their own SamzaContainer ID and the JobModel to pick specific tasks and start message processing.


During the course of processing message all container failures will result in a callback from the RM to the YarnClusterResourceManager. These callbacks can then be used to request for a new container and restart processing from the last checkpoint, thus making YARN deployments resilient to container failures.
