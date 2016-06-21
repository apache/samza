---
layout: page
title: YARN Jobs
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

When you define `job.factory.class=org.apache.samza.job.yarn.YarnJobFactory` in your job's configuration, Samza will use YARN to execute your job. The YarnJobFactory will use the `HADOOP_YARN_HOME` environment variable on the machine that run-job.sh is executed on to get the appropriate YARN configuration, which will define where the YARN resource manager is. The YarnJob will work with the resource manager to get your job started on the YARN cluster.

If you want to use YARN to run your Samza job, you'll also need to define the location of your Samza job's package. For example, you might say:

{% highlight jproperties %}
yarn.package.path=http://my.http.server/jobs/ingraphs-package-0.0.55.tgz
{% endhighlight %}

This .tgz file follows the conventions outlined on the [Packaging](packaging.html) page (it has bin/run-am.sh and bin/run-container.sh). YARN NodeManagers will take responsibility for downloading this .tgz file on the appropriate machines, and untar'ing them. From there, YARN will execute run-am.sh or run-container.sh for the Samza Application Master, and SamzaContainer, respectively.

If you want to run Samza job in the specific YARN queue not the default one, you can set `yarn.queue` property in your job's configuration. For example, with the following property setting,

{% highlight jproperties %}
yarn.queue=root.adhoc
{% endhighlight %}

Samza job will run in the root.adhoc queue.

<!-- TODO document yarn.container.count and other key configs -->

## [Logging &raquo;](logging.html)
