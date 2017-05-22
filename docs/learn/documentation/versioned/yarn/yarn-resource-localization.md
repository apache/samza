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
When running Samza jobs on YARN clusters, you may need to download some resources before startup (For example, downloading the job binaries, fetching certificate files etc.) This step is called as Resource Localization.

### Resource Localization Process

For Samza jobs running on YARN, resource localization leverages the YARN node manager's localization service. Here is a [deep dive](https://hortonworks.com/blog/resource-localization-in-yarn-deep-dive/) on how localization works in YARN. 

Depending on where and how the resource comes from, fetching the resource is associated with a scheme in the path (such as `http`, `https`, `hdfs`, `ftp`, `file`, etc). The scheme maps to a corresponding `FileSystem` implementation for handling the localization. 

There are some predefined `FileSystem` implementations in Hadoop or Samza, which are provided if you run Samza jobs on YARN:

* `org.apache.samza.util.hadoop.HttpFileSystem`: used for fetching resources based on http, or https without client side authentication.
* `org.apache.hadoop.hdfs.DistributedFileSystem`: used for fetching resource from DFS system on Hadoop.
* `org.apache.hadoop.fs.LocalFileSystem`: used for copying resources from local file system to the job directory.
* `org.apache.hadoop.fs.ftp.FTPFileSystem`: used for fetching resources based on ftp.

If you would like to have your own file system, you should implement a class which extends from `org.apache.hadoop.fs.FileSystem`. 

### Resource Configuration
You can specify a resource to be localized by the following configuration.

#### Required Configuration
1. `yarn.resources.<resourceName>.path`
    * The path for fetching the resource for localization, e.g. http://hostname.com/packages/myResource

#### Optional Configuration
2. `yarn.resources.<resourceName>.local.name`
    * The local name used for the localized resource.
    * If it is not set, the default will be the `<resourceName>` specified in `yarn.resources.<resourceName>.path`
3. `yarn.resources.<resourceName>.local.type`
    * The type of the resource with valid values from: `ARCHIVE`, `FILE`, `PATTERN`.
        * ARCHIVE: the localized resource will be an archived directory;
        * FILE: the localized resource will be a file;
        * PATTERN: the localized resource will be the entries extracted from the archive with the pattern.
    * If it is not set, the default value is `FILE`.
4. `yarn.resources.<resourceName>.local.visibility`
    * Visibility for the resource with valid values from `PUBLIC`, `PRIVATE`, `APPLICATION`
        * PUBLIC: visible to everyone 
        * PRIVATE: visible to just the account which runs the job
        * APPLICATION: visible only to the specific application job which has the resource configuration
    * If it is not set, the default value is `APPLICATION`

### YARN Configuration
Make sure the scheme used in the `yarn.resources.<resourceName>.path` is configured with a corresponding FileSystem implementation YARN core-site.xml.

{% highlight xml %}
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
      <name>fs.http.impl</name>
      <value>org.apache.samza.util.hadoop.HttpFileSystem</value>
    </property>
</configuration>
{% endhighlight %}

If you are using your own scheme (for example, `yarn.resources.myResource.path=myScheme://host.com/test`), you can link your [FileSystem](https://hadoop.apache.org/docs/stable/api/index.html?org/apache/hadoop/fs/FileSystem.html) implementation with it as follows.

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
