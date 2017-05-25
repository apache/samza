---
layout: page
title: YARN Security
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

You can run a Samza job on a secure YARN cluster. YARN uses Kerberos as its authentication and authorization mechanism. See [this article](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SecureMode.html) for details on Hadoop in secure mode.

### Delegation token management strategy

One of the challenges for long-lived application running on a secure YARN cluster is its token renewal strategy. Samza takes the following approach to manage token creation and renewal.

1. Client running Samza app needs to kinit into KDC with his credentials and add the HDFS delegation tokens to the launcher context before submitting the application.

2. Next, client prepares the local resources for the application as follows.
2.1. First, it creates a staging directory on HDFS. This directory is only accessible by the running user and used to store resources required for Application Master (AM) and Containers.
2.2. Client then adds the keytab as a local resource in the container launcher context for AM.
2.3. Finally, it sends the corresponding principal and the path to the keytab file in the staging directory to the coordinator stream. Samza currently uses the staging directory to store both the keytab and refreshed tokens because the access to the directory is secured via Kerberos.

3. Once the resource is allocated for the Application Master, the Node Manager will localizes app resources from HDFS using the HDFS delegation tokens in the launcher context. Same rule applies to Container localization too. 

4. When Application Master starts, it localizes the keytab file into its working directory and reads the principal from the coordinator stream.

5. The Application Master periodically re-authenticate itself with the given principal and keytab. In each iteration, it creates new delegation tokens and stores them in the given job specific staging directory on HDFS.

6. Each running container will get new delegation tokens from the credentials file on HDFS before the current ones expire.

7. Application Master and Containers don't communicate with each other for that matter. Each side proceeds independently by reading or writing the tokens on HDFS.

By default, any HDFS delegation token has a maximum life of 7 days (configured by `dfs.namenode.delegation.token.max-lifetime` in hdfs-site.xml) and the token is normally renewed every 24 hours (configured by `dfs.namenode.delegation.token.renew-interval` in hdfs-site.xml). What if the Application Master dies and needs restarts after 7 days? The original HDFS delegation token stored in the launcher context will be invalid no matter what. Luckily, Samza can rely on Resource Manager to handle this scenario. See the Configuration section below for details.  

### Components

#### SecurityManager

When ApplicationMaster starts, it spawns `SamzaAppMasterSecurityManager`, which runs on its separate thread. The `SamzaAppMasterSecurityManager` is responsible for periodically logging in through the given Kerberos keytab and regenerates the HDFS delegation tokens regularly. After each run, it writes new tokens on a pre-defined job specific directory on HDFS. The frequency of this process is determined by `yarn.token.renewal.interval.seconds`.

Each container, upon start, runs a `SamzaContainerSecurityManager`. It reads from the credentials file on HDFS and refreshes its delegation tokens at the same interval.

### Configuration

1. For the Samza job, the following job configurations are required on a YARN cluster with security enabled.
# Job
job.security.manager.factory=org.apache.samza.job.yarn.SamzaYarnSecurityManagerFactory

# YARN
{% highlight properties %}
yarn.kerberos.principal=user/localhost
yarn.kerberos.keytab=/etc/krb5.keytab.user
yarn.token.renewal.interval.seconds=86400
{% endhighlight %}

2. Configure the Hadoop cluster to enable Resource Manager to recreate and renew the delegation token on behalf of the application user. This will address the following 2 scenarios.

    * When Application Master dies unexpectedly and needs a restart after 7 days (the default maximum lifespan a delegation token can be renewed).

    * When the Samza job terminates and log aggregation is turned on for the job. Node managers need to be able to upload all the local application logs to HDFS.

    1. Enable the resource manager as a privileged user in yarn-site.xml.
    {% highlight xml %}
        <property>
            <name>yarn.resourcemanager.proxy-user-privileges.enabled</name>
            <value>true</value>
        </property>
    {% endhighlight %}

    2. Make `yarn` as a proxy user, in core-site.xml
    {% highlight xml %}
        <property>
            <name>hadoop.proxyuser.yarn.hosts</name>
            <value>*</value>
        </property>
        <property>
            <name>hadoop.proxyuser.yarn.groups</name>
            <value>*</value>
        </property>
    {% endhighlight %}

## [Writing to HDFS &raquo;](../hdfs/producer.html)
