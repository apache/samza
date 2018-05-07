---
layout: page
title: Getting Started with Samza REST
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

This tutorial depends on [hello-samza](../../../startup/hello-samza/{{site.version}}/) to start some example jobs on a local cluster, which you will then access via the [JobsResource](../../documentation/{{site.version}}/rest/resources/jobs.html). After completing this tutorial, you will have built and deployed the Samza REST resource locally, changed the configuration for the JobsResource, and executed a couple of basic curl requests to verify the service works.

Lets get started.

### Run Hello Samza Jobs Locally 

Follow the [hello-samza](../../../startup/hello-samza/{{site.version}}/) tutorial to setup a local grid and run the wikipedia jobs. Skip the [shutdown step](../../../startup/hello-samza/{{site.version}}/#shutdown) because you need the grid to still be running to query the REST service for jobs. You can optionally skip all the ```kafka-console-consumer.sh``` commands if you don't want to verify the output of the jobs.

Take note of the path where you cloned hello-samza. You will need this to configure the installations path for the JobsResource.


#### Build the Samza REST Service package
The source code for Samza REST is in the samza-rest module of the Samza repository. To build it, execute the following gradle task from the root of the project.
{% highlight bash %}
./gradlew samza-rest:clean releaseRestServiceTar
{% endhighlight %}

#### Deploy the Samza REST Service Locally
To deploy the service, you simply extract the tarball to the desired location. Here, we will deploy the tarball on the local host in

```
SAMZA_ROOT/samza-rest/build/distributions/deploy/samza-rest
```
where ```SAMZA_ROOT``` is the path to the root of your Samza project.

Run the following commands:
{% highlight bash %}
cd samza-rest/build/distributions/
mkdir -p deploy/samza-rest
tar -xvf ./samza-rest-0.14.1.tgz -C deploy/samza-rest
{% endhighlight %}

#### Configure the Installations Path
The JobsResource has a required config [job.installations.path](../../documentation/{{site.version}}/rest/resources/jobs.html#configuration) which specifies the path where the jobs are installed. Edit the configuration file:

```
deploy/samza-rest/config/samza-rest.properties
```

Set the job.installations.path to:

```
job.installations.path=/hello-samza-ROOT/deploy
```

where ```hello-samza-ROOT``` is the path to your hello-samza clone, noted above. This tells the JobsResource to crawl this location to find all the installed jobs.

#### Start the Samza REST Service
To deploy the service, run the run-samza-rest-service.sh script from the extracted directory.
{% highlight bash %}
cd deploy/samza-rest
./bin/run-samza-rest-service.sh  \
  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
  --config-path=file://$PWD/config/samza-rest.properties
{% endhighlight %}

You provide two parameters to the run-samza-rest-service.sh script. One is the config location, and the other, optional, parameter is a factory class that is used to read your configuration file. The SamzaRestService uses your ConfigFactory to get a Config object from the config path. The ConfigFactory is covered in more detail on the [Job Runner page](../../documentation/{{site.version}}/jobs/job-runner.html). The run-samza-rest-service.sh script will block until the SamzaRestService terminates.

Note: With the default settings, the JobsResource will expect a YARN cluster with a local Resource Manager accessible via the ApplicationCLI. Without YARN, the JobsResource will not respond to any requests. So it's important to walk through hello-samza demo before the next step.


### Curl the Default REST Service
Curl the JobsResource to get all installed jobs

```
curl localhost:9139/v1/jobs
[{"jobName":"wikipedia-stats","jobId":"1","status":"STARTED","statusDetail":RUNNING},{"jobName":"wikipedia-parser","jobId":"1","status":"STARTED","statusDetail":RUNNING},{"jobName":"wikipedia-feed","jobId":"1","status":"STARTED","statusDetail":RUNNING}
```

Now curl the JobsResource to stop one of the jobs

```
curl -X PUT localhost:9139/v1/jobs/wikipedia-feed/1?status=stopped
{"jobId":"1","jobName":"wikipedia-feed","status":"STOPPED","statusDetail":"FINISHED"}
```

Congratulations, you've successfully deployed the Samza REST Service and used the JobsResource to list jobs and stop a job!

See the [JobsResource documentation](../../documentation/{{site.version}}/rest/resources/jobs.html) for the rest of its API.

See the [Resources documentation](../../documentation/{{site.version}}/rest/resources.html) for more information about Resources and how you can add your own.
