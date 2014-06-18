---
layout: page
title: Deploying a Samza job from HDFS
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

This tutorial uses [hello-samza](../../../startup/hello-samza/0.7.0/) to illustrate how to run a Samza job if you want to publish the Samza job's .tar.gz package to HDFS.

### Build a new Samza job package

Build a new Samza job package to include the hadoop-hdfs-version.jar.

* Add dependency statement in pom.xml of samza-job-package

{% highlight xml %}
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-hdfs</artifactId>
  <version>2.2.0</version>
</dependency>
{% endhighlight %}

* Add the following code to src/main/assembly/src.xml in samza-job-package.

{% highlight xml %}
<include>org.apache.hadoop:hadoop-hdfs</include>
{% endhighlight %}

* Create .tar.gz package

{% highlight bash %}
mvn clean pacakge
{% endhighlight %}

* Make sure hadoop-common-version.jar has the same version as your hadoop-hdfs-version.jar. Otherwise, you may still have errors.

### Upload the package

{% highlight bash %}
hadoop fs -put ./samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz /path/for/tgz
{% endhighlight %}

### Add HDFS configuration

Put the hdfs-site.xml file of your cluster into ~/.samza/conf directory. (The same place as the yarn-site.xml)

### Change properties file

Change the yarn.package.path in the properties file to your HDFS location.

{% highlight jproperties %}
yarn.package.path=hdfs://<hdfs name node ip>:<hdfs name node port>/path/to/tgz
{% endhighlight %}

Then you should be able to run the Samza job as described in [hello-samza](../../../startup/hello-samza/0.7.0/).

