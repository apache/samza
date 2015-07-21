---
layout: page
title: Deploy Samza Job To CDH
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

The tutorial assumes you have successfully run [hello-samza](../../../startup/hello-samza/{{site.version}}/) and now you want to deploy the job to your Cloudera Data Hub ([CDH](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh.html)). This tutorial is based on CDH 5.0.0 and uses hello-samza as the example job.

### Upload Package to Cluster

There are a few ways of uploading the package to the cluster's HDFS. If you do not have the job package in your cluster, **scp** from you local machine to the cluster. Then run

{% highlight bash %}
hadoop fs -put path/to/hello-samza-0.9.1-dist.tar.gz /path/for/tgz
{% endhighlight %}

### Get Deloying Scripts

Untar the job package (assume you will run from the current directory)

{% highlight bash %}
tar -xvf path/to/samza-job-package-0.9.1-dist.tar.gz -C ./
{% endhighlight %}

### Add Package Path to Properties File

{% highlight bash %}
vim config/wikipedia-parser.properties
{% endhighlight %}

Change the yarn package path:

{% highlight jproperties %}
yarn.package.path=hdfs://<hdfs name node ip>:<hdfs name node port>/path/to/tgz
{% endhighlight %}

### Set Yarn Environment Variable

{% highlight bash %}
export HADOOP_CONF_DIR=/etc/hadoop/conf
{% endhighlight %}

### Run Samza Job

{% highlight bash %}
bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/wikipedia-parser.properties
{% endhighlight %}
