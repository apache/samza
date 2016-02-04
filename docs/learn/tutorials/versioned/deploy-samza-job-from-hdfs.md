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

This tutorial uses [hello-samza](../../../startup/hello-samza/{{site.version}}/) to illustrate how to run a Samza job if you want to publish the Samza job's .tar.gz package to HDFS.

### Upload the package

{% highlight bash %}
hadoop fs -put ./target/hello-samza-0.9.1-dist.tar.gz /path/for/tgz
{% endhighlight %}

### Add HDFS configuration

Put the hdfs-site.xml file of your cluster into ~/.samza/conf directory (The same place as the yarn-site.xml). If you set HADOOP\_CONF\_DIR, put the hdfs-site.xml in your configuration directory if the hdfs-site.xml is not there.

### Change properties file

Change the yarn.package.path in the properties file to your HDFS location.

{% highlight jproperties %}
yarn.package.path=hdfs://<hdfs name node ip>:<hdfs name node port>/path/to/tgz
{% endhighlight %}

Then you should be able to run the Samza job as described in [hello-samza](../../../startup/hello-samza/{{site.version}}/).
