---
layout: page
title: Application Master
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

YARN is Hadoop's next-generation cluster manager. It allows developers to deploy and execute arbitrary commands on a grid. If you're unfamiliar with YARN, or the concept of an ApplicationMaster (AM), please read Hadoop's [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) page.

### Integration

Samza's main integration with YARN comes in the form of a Samza ApplicationMaster. This is the chunk of code responsible for managing a Samza job in a YARN grid. It decides what to do when a stream processor fails, which machines a Samza job's [containers](../container/samza-container.html) should run on, and so on.

When the Samza ApplicationMaster starts up, it does the following:

1. Creates the [Job Coordinator](../container/coordinator-stream.html#JobCoordinator) which bootstraps the Job Model and config from the [Coordinator Stream](../container/coordinator-stream.html).
2. Starts a JMX server on a random port.
3. Instantiates a metrics registry and reporters to keep track of relevant metrics.
4. Registers the AM with YARN's RM.
5. Gets the total number of partitions for the Samza job using each input stream's PartitionManager (see the [Streams](../container/streams.html) page for details).
6. Read the total number of containers requested from the Samza job's configuration.
7. Assign each partition to a container (called a Task Group in Samza's AM dashboard).
8. Make a [ResourceRequest](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/yarn/api/records/ResourceRequest.html) to YARN for each container. If [host-affinity](yarn-host-affinity.html) is enabled on the job, the AM uses the container locality information provided by the Job Coordinator and requests for the same host in the ResourceRequest.
9. Starts a ContainerAllocator thread that matches allocated containers and starts the container process.
10. Poll the YARN RM every second to check for allocated and released containers.

From this point on, the ApplicationMaster just reacts to events from the RM and delegates it to the ContainerAllocator thread.

### Fault Tolerance

Whenever a container is allocated, the AM will work with the YARN NM to start a SamzaContainer (with appropriate partitions assigned to it) in the container. If a container fails with a non-zero return code, the AM will request a new container, and restart the SamzaContainer. If a SamzaContainer fails too many times, too quickly, the ApplicationMaster will fail the whole Samza job with a non-zero return code. See the yarn.container.retry.count and yarn.container.retry.window.ms [configuration](../jobs/configuration.html) parameters for details.

When the AM receives a reboot signal from YARN, it will throw a SamzaException. This will trigger a clean and successful shutdown of the AM (YARN won't think the AM failed).

If the AM, itself, fails, YARN will handle restarting the AM. When the AM is restarted, all containers that were running will be killed, and the AM will start from scratch. The same list of operations, shown above, will be executed. The AM will request new containers for its SamzaContainers, and proceed as though it has just started for the first time. YARN has a yarn.resourcemanager.am.max-retries configuration parameter that's defined in [yarn-site.xml](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-common/yarn-default.xml). This configuration defaults to 1, which means that, by default, a single AM failure will cause your Samza job to stop running.

### Security

The Samza dashboard's HTTP access is currently un-secured, even when using YARN in secure-mode. This means that users with access to a YARN grid could port-scan a Samza ApplicationMaster's HTTP server, and open the dashboard in a browser to view its contents. Sensitive configuration can be viewed by anyone, in this way, and care should be taken. There are plans to secure Samza's ApplicationMaster using [Hadoop's security](http://docs.hortonworks.com/HDPDocuments/HDP1/HDP-1.3.0/bk_installing_manually_book/content/rpm-chap14-2-3-1.html) features ([SPENAGO](http://en.wikipedia.org/wiki/SPNEGO)).

See Samza's [security](../operations/security.html) page for more details.

## [Isolation &raquo;](isolation.html)
