---
layout: page
title: Overview
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

Samza provides a REST service that is deployable on any node in the cluster and has pluggable interfaces to add custom [Resources](resources.html) and [Monitors](monitors.html). It is intended to be a node-local delegate for common actions such as starting jobs, sampling the local state, measuring disk usage, taking heap dumps, verifying liveness, and so on.

The Samza REST Service does not yet have SSL enabled or authentication, so it is initially geared for more backend and operations use cases. It would not be wise to expose it as a user-facing API in environments where users are not allowed to stop each other's jobs, for example.

Samza REST is packaged and configured very similarly to Samza jobs. A notable difference is Samza REST must be deployed and executed on each host you want it to run on, whereas a Samza Job is typically started on a master node in the cluster manager and the master deploys the job to the other nodes.

### Deployment
Samza REST is intended to be a proxy for all operations which need to be executed from the nodes of the Samza cluster. It can be deployed to all the hosts in the cluster and may serve different purposes on different hosts. In such cases it may be useful to deploy the same release tarball with different configs to customize the functionality for the role of the hosts. For example, Samza REST may be deployed on a YARN cluster with one config for the ResourceManager (RM) hosts and another config for the NodeManager (NM) hosts.

Deploying the service is very similar to running a Samza job. First build the tarball using:
{% highlight bash %}
./gradlew samza-rest:clean releaseRestServiceTar
{% endhighlight %}


Then from the extracted location, run the service using:
{% highlight bash %}
samza-example/target/bin/run-samza-rest-service.sh  \
  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
  --config-path=file://$PWD/config/samza-rest.properties
{% endhighlight %}

The two config parameters have the same purpose as they do for [run-job.sh](../jobs/job-runner.html).

Follow the [getting started tutorial](../../../tutorials/{{site.version}}/samza-rest-getting-started.html) to quickly deploy and test the Samza REST Service for the first time.

### Configuration
The Samza REST Service relies on the same configuration system as Samza Jobs. However, the Samza REST Service config file itself is completely separate and unrelated to the config files for your Samza jobs.

The configuration may provide values for the core configs as well as any additional configs needed for Resources or Monitors that you may have added to the service.  A basic configuration file which includes configs for the core service as well as the [JobsResource](resources/jobs.html#configuration) looks like this:

{% highlight jproperties %}
# Service port. Set to 0 for a dynamic port.
services.rest.port=9139

# JobProxy
job.proxy.factory.class=org.apache.samza.rest.proxy.job.SimpleYarnJobProxyFactory
# Installation path for hello-samza project. Your root may vary.
job.installations.path=/hello-samza-ROOT/deploy/samza
{% endhighlight %}

###### Core Configuration
<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Name</th>
      <th>Default</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>services.rest.port</td><td></td><td><b>Required:</b> The port to use on the local host for the Samza REST Service. If 0, an available port will be dynamically chosen.</td>
    </tr>
    <tr>
      <td>rest.resource.factory.classes</td><td></td><td>A comma-delimited list of class names that implement ResourceFactory. These factories will be used to create specific instances of resources and can pull whatever properties they need from the provided server config. The instance returned will be used for the lifetime of the server. If no value is provided for this property or <pre>rest.resource.classes</pre> then <pre>org.apache.samza.rest.resources.DefaultResourceFactory</pre> will be used as a default.</td>
    </tr>
    <tr>
      <td>rest.resource.classes</td><td></td><td>A comma-delimited list of class names of resources to register with the server. These classes can be instantiated as often as each request, the life cycle is not guaranteed to match the server. Also, the instances do not receive any config. Note that the lifecycle and ability to receive config are the primary differences between resources added via this property versus rest.resource.factory.classes</td>
    </tr>
  </tbody>
</table>

### Logging
Samza REST uses SLF4J for logging. The `run-samza-rest-service.sh` script mentioned above by default expects a log4j.xml in the package's bin directory and writes the logs to a logs directory in the package root. However, since the script invokes the same `run-class.sh` script used to run Samza jobs, it can be reconfigured very similarly to [logging for Samza jobs](../jobs/logging.html).

## [Resources &raquo;](resources.html)