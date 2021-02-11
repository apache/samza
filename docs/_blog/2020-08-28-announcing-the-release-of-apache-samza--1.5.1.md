---
layout: blog
title: Announcing the release of Apache Samza 1.5.1
icon: git-pull-request
authors:
    - name: Bharath Kumarasubramanian
      website:
      image:
excerpt_separator: <!--more-->
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

# **Announcing the release of Apache Samza 1.5.1**

<!--more-->

We are thrilled to announce the release of Apache Samza 1.5.1.

### Bug Fixes:
In 1.5 release, we enabled transactional state by default for all samza jobs. We identified a critical bug related to trimming the state and have addressed the issue in 1.5.1.
For applications that are already on Samza 1.5.0, updating your dependencies to use Samza 1.5.1 should be sufficient to upgrade.
For applications that are on version 1.4 & below, please follow the upgrade instructions below.


### Upgrading your application to Apache Samza 1.5.0
ConfigFactory is deprecated as Job Runner does not load full job config anymore. Instead, ConfigLoaderFactory is introduced to be executed on ClusterBasedJobCoordinator to fetch full job config.
If you are using the default PropertiesConfigFactory, simply switching to use the default PropertiesConfigLoaderFactory will work, otherwise if you are using a custom ConfigFactory, kindly creates its new counterpart following ConfigLoaderFactory. 

Configs related to job submission must be explicitly provided to Job Runner as it is no longer loading full job config anymore. These configs include

* Configs directly related to job submission, such as yarn.package.path, job.name etc.
* Configs needed by the config loader on AM to fetch job config, such as path to the property file in the tarball, all of such configs will have a job.config.loader.properties prefix.
* Configs that users would like to override

Full list of the job submission configurations can be found [here](https://cwiki.apache.org/confluence/display/SAMZA/SEP-23%3A+Simplify+Job+Runner#SEP23:SimplifyJobRunner-References)

#### Usage Instructions
Alternative way when submitting job,
{% highlight bash %}
deploy/samza/bin/run-app.sh
 --config yarn.package.path=<package_path>
 --config job.name=<job_name>
{% endhighlight %}
can be simplified to
{% highlight bash %}
deploy/samza/bin/run-app.sh
 --config-path=/path/to/submission/properties/file/submission.properties
{% endhighlight %}
where submission.properties contains
{% highlight jproperties %}
yarn.package.path=<package_path>
job.name=<job_name>
{% endhighlight %}

#### Rollback Instructions
In case of a problem in Samza 1.5.1, users can rollback to Samza 1.4 and keep the old start up flow using _config-path_ & _config-factory_.

### Bug Fixes
[SAMZA-2578](https://issues.apache.org/jira/browse/SAMZA-2578) Excessive trimming during transactional state restore

### Sources downloads
A source download of Samza 1.5.1 is available [here](https://dist.apache.org/repos/dist/release/samza/1.5.1/), and is also available in Apache’s Maven repository. See Samza’s download [page](https://samza.apache.org/startup/download/) for details and Samza’s feature preview for new features.
