---
layout: page
title: Download
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

Samza is released as a source artifact, and also through Maven.

If you just want to play around with Samza for the first time, go to [Hello Samza](/startup/hello-samza/{{site.version}}).

### JDK Notice

Starting from 2016, Samza will begin requiring JDK8 or higher. Please see [this mailing list thread](http://mail-archives.apache.org/mod_mbox/samza-dev/201610.mbox/%3CCAHUevGGnOQD_VmLWEdpFNq3Lv%2B6gQQmw_JKx9jDr5Cw%2BxFfGtQ%40mail.gmail.com%3E) for details on this decision.

### Samza Tools
 
 Samza tools package contains command line tools that user can run to use Samza and it's input/output systems. 
 
 * [samza-tools-0.14.1.tgz](http://www-us.apache.org/dist/samza/0.14.1/samza-tools-0.14.1.tgz)

### Source Releases

 * [samza-sources-0.14.1.tgz](http://www.apache.org/dyn/closer.lua/samza/0.14.1)
 * [samza-sources-0.14.0.tgz](http://www.apache.org/dyn/closer.lua/samza/0.14.0)
 * [samza-sources-0.13.1.tgz](http://www.apache.org/dyn/closer.lua/samza/0.13.1)
 * [samza-sources-0.13.0.tgz](http://www.apache.org/dyn/closer.lua/samza/0.13.0)
 * [samza-sources-0.12.0.tgz](http://www.apache.org/dyn/closer.lua/samza/0.12.0)
 * [samza-sources-0.11.0.tgz](http://www.apache.org/dyn/closer.lua/samza/0.11.0)
 * [samza-sources-0.10.1.tgz](http://www.apache.org/dyn/closer.lua/samza/0.10.1)
 * [samza-sources-0.10.0.tgz](http://www.apache.org/dyn/closer.lua/samza/0.10.0)
 * [samza-sources-0.9.1.tgz](http://www.apache.org/dyn/closer.lua/samza/0.9.1)
 * [samza-sources-0.9.0.tgz](http://www.apache.org/dyn/closer.lua/samza/0.9.0)
 * [samza-sources-0.8.0-incubating.tgz](https://archive.apache.org/dist/incubator/samza/0.8.0-incubating)
 * [samza-sources-0.7.0-incubating.tgz](https://archive.apache.org/dist/incubator/samza/0.7.0-incubating)


### Maven

All Samza JARs are published through [Apache's Maven repository](https://repository.apache.org/content/groups/public/org/apache/samza/).

#### Artifacts

A Maven-based Samza project can pull in all required dependencies Samza dependencies this XML block:

{% highlight xml %}
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-api</artifactId>
  <version>0.14.1</version>
</dependency>
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-core_2.11</artifactId>
  <version>0.14.1</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-shell</artifactId>
  <classifier>dist</classifier>
  <type>tgz</type>
  <version>0.14.1</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-yarn_2.11</artifactId>
  <version>0.14.1</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-kv_2.11</artifactId>
  <version>0.14.1</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-kv-rocksdb_2.11</artifactId>
  <version>0.14.1</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-kv-inmemory_2.11</artifactId>
  <version>0.14.1</version>
  <scope>runtime</scope>
</dependency>
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-kafka_2.11</artifactId>
  <version>0.14.1</version>
  <scope>runtime</scope>
</dependency>
{% endhighlight %}

Samza versions less than 0.12 should use artifacts with scala version 2.10 as suffix. For example,

{% highlight xml %}
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-yarn_2.10</artifactId>
  <version>0.11.0</version>
</dependency>
{% endhighlight %}

Samza versions less than 0.9 should include this additional dependency.

{% highlight xml %}
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-serializers_2.10</artifactId>
  <version>0.8.1</version>
</dependency>
{% endhighlight %}

[Hello Samza](/startup/hello-samza/{{site.version}}) is a working Maven project that illustrates how to build projects that have Samza jobs in them.

#### Repositories

Samza is available in the Apache Maven repository.

{% highlight xml %}
<repository>
  <id>apache-releases</id>
  <url>https://repository.apache.org/content/groups/public</url>
</repository>
{% endhighlight %}

Snapshot builds are available in the Apache Maven snapshot repository.

{% highlight xml %}
<repository>
  <id>apache-snapshots</id>
  <url>https://repository.apache.org/content/groups/snapshots</url>
</repository>
{% endhighlight %}

### Checking out and Building

If you're interested in working on Samza, or building the JARs from scratch, then you'll need to checkout and build the code. Samza does not have a binary release at this time. To check out and build Samza, run these commands.

{% highlight bash %}
git clone http://git-wip-us.apache.org/repos/asf/samza.git
cd samza
./gradlew clean build
{% endhighlight %}

See the README.md file for details on building.
