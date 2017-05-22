---
layout: page
title: YARN Resource Localization
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

When Samza jobs run on YARN clusters, sometimes there are needs to preload some files or data (called as resources in this doc) before job starts, such as preparing the job package, fetching job certificate, or etc., Samza supports a general configuration way to localize difference resources.

### Resource Localization Process

For the Samza jobs running on YARN, the resource localization leverages the YARN node manager localization service. Here is a good [deep dive](https://hortonworks.com/blog/resource-localization-in-yarn-deep-dive/) from Horton Works on how localization works in YARN. 

Depending on where and how the resource comes from, fetching the resource is associated with a scheme in the path, such as `http`, `https`, `hdfs`, `ftp`, `file`, etc., which maps to a certain FileSystem for handling the localization. 

If there is an implementation of [FileSystem](https://hadoop.apache.org/docs/stable/api/index.html?org/apache/hadoop/fs/FileSystem.html) on YARN supporting a scheme, then that scheme can be used for resource localization. 

There are some predefined file systems in Hadoop or Samza, which are provided if you run Samza jobs on YARN:

* `org.apache.samza.util.hadoop.HttpFileSystem`: used for fetching resources based on http, or https without client side authentication requirement.
* `org.apache.hadoop.hdfs.DistributedFileSystem`: used for fetching resource from DFS system on Hadoop.
* `org.apache.hadoop.fs.LocalFileSystem`: used for copying resources from local file system to the job directory.
* `org.apache.hadoop.fs.ftp.FTPFileSystem`: used for fetching resources based on ftp.
* ...

If you would like to have your own file system, you need to implement a class which extends from `org.apache.hadoop.fs.FileSystem`. 

### Job Configuration
With the configuration properly defined, the resources a job requiring from external or internal locations may be prepared automatically before it runs.

For each resource with the name `<resourceName>` in the Samza job, the following set of job configurations are used when running on a YARN cluster. The first one which definiing resource path is required, but the others are optional and they have default values.

1. `yarn.resources.<resourceName>.path`
    * Required
    * The path for fetching the resource for localization, e.g. http://hostname.com/packages/mySamzaJob
2. `yarn.resources.<resourceName>.local.name`
    * Optional 
    * The local name used for the localized resource.
    * If not set, the default one will be `<resourceName>` from the config key.
3. `yarn.resources.<resourceName>.local.type`
    * Optional 
    * Localized resource type with valid values from: `ARCHIVE`, `FILE`, `PATTERN`.
        * ARCHIVE: the localized resource will be an archived directory;
        * FILE: the localized resource will be a file;
        * PATTERN: the localized resource will be the entries extracted from the archive with the pattern.
    * If not set, the default value is `FILE`.
4. `yarn.resources.<resourceName>.local.visibility`
    * Optional
    * Localized resource visibility for the resource, and it can be a value from `PUBLIC`, `PRIVATE`, `APPLICATION`
        * PUBLIC: visible to everyone 
        * PRIVATE: visible to just the account which runs the job
        * APPLICATION: visible only to the specific application job which has the resource configuration
    * If not set, the default value is `APPLICATION`

It is up to you how to name the resource, but `<resourceName>` should be the same in the above configurations to apply to the same resource. 

### YARN Configuration
Make sure the scheme used in the yarn.resources.&lt;resourceName&gt;.path is configured in YARN core-site.xml with a FileSystem implementation. For example, for scheme `http`, you should have the following property in YARN core-site.xml:

{% highlight xml %}
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
      <name>fs.http.impl</name>
      <value>org.apache.samza.util.hadoop.HttpFileSystem</value>
    </property>
</configuration>
{% endhighlight %}

You can override a behavior for a scheme by linking it to another file system. For example, you have a special need for localizing a resource for your job through http request, you may implement your own Http File System by extending [FileSystem](https://hadoop.apache.org/docs/stable/api/index.html?org/apache/hadoop/fs/FileSystem.html), and have the following configuration:

{% highlight xml %}
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
      <name>fs.http.impl</name>
      <value>com.myCompany.MyHttpFileSystem</value>
    </property>
</configuration>
{% endhighlight %}

If you are using other scheme which is not defined in Hadoop or Samza, for example, `yarn.resources.mySampleResource.path=myScheme://host.com/test`, in your job configuration, you may implement your own [FileSystem](https://hadoop.apache.org/docs/stable/api/index.html?org/apache/hadoop/fs/FileSystem.html) such as com.myCompany.MySchemeFileSystem and link it with your own scheme in yarn core-site.xml configuration.

{% highlight xml %}
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
      <name>fs.myScheme.impl</name>
      <value>com.myCompany.MySchemeFileSystem</value>
    </property>
</configuration>
{% endhighlight %}

## [Yarn Security &raquo;](../yarn/yarn-security.html)
