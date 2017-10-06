---
layout: page
title: Jobs Resource
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

Exposes operations at the job scope (as opposed to the cluster, container, or task scope). The initial implementation includes the ability to list all jobs, get the status of a particular job, and start or stop an individual job.

#API
The following sections provide general information about the response structure and detailed descriptions of each of the requests.

## Response Structure
All responses will contain either a job status or an error message.

**Job Status**

Job status will be of the form:

{% highlight json %}
{
    "status":"STOPPED",
    "statusDetail":"KILLED",
    "jobName":"wikipedia-parser",
    "jobId":"1"
}
{% endhighlight %}

`status` is the abstract Samza status for the job. Initially it will be one of {STARTING, STARTED, STOPPED, UNKNOWN}.

`statusDetail` is the implementation-specific status for the job. For YARN, it will be one of the values in the YarnApplicationState enum.

**Error Message**

Every error response have the following structure:

{% highlight json %}
{
    "message": "Unrecognized status parameter: null"
}
{% endhighlight %}

`message` is the only field in the response and contains a description of the problem.
<br/>

##Get All Jobs
Lists all the jobs installed on the host and provides their status.

######Request
    GET /v1/jobs

######Response
	Status: 200 OK
{% highlight json %}[

    {
        "status":"STOPPED",
        "statusDetail":"KILLED",
        "jobName":"wikipedia-parser",
        "jobId":"1"
    },
    {
        "status":"STARTED",
        "statusDetail":"RUNNING",
        "jobName":"wikipedia-feed",
        "jobId":"1"
    },
    {
        "status":"STOPPED",
        "statusDetail":null,
        "jobName":"wikipedia-stats",
        "jobId":"1"
    }
]
{% endhighlight %}

######Response codes
<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Status</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>200 OK</td><td>The operation completed successfully and the current job status for each job is returned.</td>
    </tr>
    <tr>
      <td>500 Server Error</td><td>There was an error executing the command on the server. e.g. The command timed out.{% highlight json %}
{
    "message": "Timeout waiting for job status."
}
{% endhighlight %}</td>
    </tr>
  </tbody>
</table>
<br/>

##Get Job
Gets the status of the specified job.

######Format
    GET /v1/jobs/{jobName}/{jobId}
The `{jobName}` and `{jobId}` path parameters reflect the values of 'job.name' and 'job.id' in the job config.

######Request
    GET /v1/jobs/wikipedia-feed/1

######Response
	Status: 200 OK
{% highlight json %}
{
    "status":"STARTED",
    "statusDetail":"RUNNING",
    "jobName":"wikipedia-feed",
    "jobId":"1"
}
{% endhighlight %}

######Response codes
<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Status</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>200 OK</td><td>The operation completed successfully and the current job status is returned.</td>
    </tr>
    <tr>
      <td>404 Not Found</td><td>The specified job+instance was not found.{% highlight json %}
{
    "message": "Job wikipedia-¯\_(ツ)_/¯-feed instance 1 is not installed on this host."
}
{% endhighlight %}</td>
    </tr>
    <tr>
      <td>500 Server Error</td><td>There was an error executing the command on the server. e.g. The command timed out.{% highlight json %}
{
    "message": "Timeout waiting for job status."
}
{% endhighlight %}</td>
    </tr>
  </tbody>
</table>
<br/>

##Start Job
Starts the job with the specified app name if it's not already started. The command will return when it has initiated the start operation.

######Format
    PUT /v1/jobs/{jobName}/{jobId}?status=started

Form parameter `status`	is the intended status of the job at the end of the request.

######Example
    PUT /v1/jobs/wikipedia-feed/1?status=started
######Response
	Status: 202 Accepted
{% highlight json %}
{
    "status":"STARTING",
    "statusDetail":"ACCEPTED",
    "jobName": "wikipedia-feed",
    "jobId": "1"
}
{% endhighlight %}

######Response codes

<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Status</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>200 OK</td><td>The operation completed successfully and the current job status is returned.</td>
    </tr>
    <tr>
      <td>400 Bad Request</td><td>There was a problem with the request. e.g. an invalid status parameter.{% highlight json %}
{
    "message": "Unrecognized status parameter: null"
}
{% endhighlight %}</td>
    <tr>
      <td>404 Not Found</td><td>The specified job+instance was not found.{% highlight json %}
{
    "message": "Job wikipedia-¯\_(ツ)_/¯-feed instance 1 is not installed on this host."
}
{% endhighlight %}</td>
    </tr>
    <tr>
      <td>500 Server Error</td><td>There was an error executing the command on the server. e.g. The command timed out.{% highlight json %}
{
    "message": "Timeout waiting for job status."
}
{% endhighlight %}</td>
    </tr>
  </tbody>
</table>
<br/>

##Stop Job
Stops the job with the specified app name if it's not already stopped.

######Format
    PUT /v1/jobs/{jobName}/{jobId}?status=stopped

Form parameter `status`	is the intended status of the job at the end of the request.

######Example
    PUT /v1/jobs/wikipedia-feed/1?status=stopped
######Response
	Status: 202 Accepted
{% highlight json %}
{
    "status":"STOPPED",
    "statusDetail":"KILLED",
    "jobName": "wikipedia-feed",
    "jobId": "1"
}
{% endhighlight %}

######Response codes
<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Status</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>200 OK</td><td>The operation completed successfully and the current job status is returned.</td>
    </tr>
    <tr>
      <td>400 Bad Request</td><td>There was a problem with the request. e.g. an invalid status parameter.{% highlight json %}
{
    "message": "Unrecognized status parameter: null"
}
{% endhighlight %}</td>
    <tr>
      <td>404 Not Found</td><td>The specified job+instance was not found.{% highlight json %}
{
    "message": "Job wikipedia-¯\_(ツ)_/¯-feed instance 1 is not installed on this host."
}
{% endhighlight %}</td>
    </tr>
    <tr>
      <td>500 Server Error</td><td>There was an error executing the command on the server. e.g. The command timed out.{% highlight json %}
{
    "message": "Timeout waiting for job status."
}
{% endhighlight %}</td>
    </tr>
  </tbody>
</table>
<br/>

# Design
###Abstractions
There are three primary abstractions used by the JobsResource that users can implement to handle any details specific to their environment.

1.  **JobProxy**: The JobProxy is the central point of interacting with Samza jobs. It exposes generic methods to start, stop, and get the status of a Samza job. Implementations of this interface can employ custom code to implement these methods tailored to the specific API for any cluster manager.
2.  **InstallationFinder**: The InstallationFinder provides a generic interface to discover all the installed jobs, hiding any customizations in the job package structure and its location (e.g. local vs remote host). The InstallationFinder also resolves the job configuration, which is used to validate and identify the job.
3.  **JobStatusProvider**: The JobStatusProvider allows the JobProxy to get the status of a job in a generic way. The same interface can be used to get the job status on Yarn, Mesos, or standalone jobs. It also enables different implementations for the same cluster. With Yarn, for example, one implementation may get job status via command line and another via the ResourceManager REST API.

The [configuration](jobs.html#configuration) must specify a JobProxy factory class explicitly. By contrast, the InstallationFinder and JobStatusProvider abstractions are natural extensions of the JobProxy and are solely provided to demonstrate a pattern for discovering installed jobs and fetching job status. However, they are not an explicit requirement.

The `SimpleYarnJobProxy` that ships with Samza REST is intended to demonstrate a functional implementation of a JobProxy which works with the Hello Samza jobs. See the [tutorial](../../../../tutorials/{{site.version}}/samza-rest-getting-started.html) to try it out. You can implement your own JobProxy to adapt the JobsResource to the specifics of your job packaging and deployment model.


### Request Flow
After validating each request, the JobsResource invokes the appropriate JobProxy command. The JobProxy uses the InstallationFinder to enumerate the installed jobs and the JobStatusProvider to get the runtime status of the jobs.

The provided [SimpleInstallationFinder](../javadocs/org/apache/samza/rest/proxy/installation/SimpleInstallationFinder.html) crawls the file system, starting in the directory specified by the `job.installations.path` looking for valid Samza job config files. It extracts the `job.name` and `job.id` property values and creates an [InstallationRecord](../javadocs/org/apache/samza/rest/proxy/installation/InstallationRecord.html) for the each job instance. The InstallationRecord contains all the information needed to start, stop, and get the status for the job.

The provided [YarnRestJobStatusProvider](../javadocs/org/apache/samza/rest/proxy/job/YarnRestJobStatusProvider.html) uses the Resource Manager's REST API to fetch the job status.

The [SimpleYarnJobProxy](../javadocs/org/apache/samza/rest/proxy/job/SimpleYarnJobProxy.html) relies on the scripts in the InstallationRecord scriptFilePath (`/bin`) directory to start and stop jobs.

The following is a depiction of the implementation that ships with Samza REST:

<img src="/img/{{site.version}}/learn/documentation/rest/JobsResource.png" alt="Jobs resource component diagram" style="max-width: 100%; height: auto;" onclick="window.open(this.src)"/>

## Configuration
The JobsResource properties should be specified in the same file as the Samza REST configuration. They are specified here for clarity.

<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Name</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>job.proxy.factory.class</td><td><b>Required:</b> The Job Proxy Factory that will be used to create a Job Proxy to control the job. The value is a fully-qualified class name which must implement JobProxyFactory. Samza ships with one implementation: <pre>org.apache.samza.rest.proxy.job.SimpleYarnJobProxy</pre> <li> Executes shell scripts to start, stop, and get the status of jobs, relying primarily on the YARN ApplicationCLI. It uses the <pre>SimpleInstallationRecord</pre> abstraction to interact with Samza jobs installed on disk.</li></td>
    </tr>
    <tr>
      <td>job.installations.path</td><td><b>Required:</b> The file system path which contains the Samza job installations. The path must be on the same host as the Samza REST Service. Each installation must be a directory with structure conforming to the expectations of the InstallationRecord implementation used by the JobProxy.</td>
    </tr>
    <tr>
      <td>job.config.factory.class</td><td>The config factory to use for reading Samza job configs. This is used to fetch the job.name and job.id properties for each job instance in the InstallationRecord. It's also used to validate that a particular directory within the installations path actually contains Samza jobs. If not specified <pre>org.apache.samza.config.factories.PropertiesConfigFactory</pre> will be used. </td>
    </tr>
  </tbody>
</table>
