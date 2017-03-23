---
layout: page
title: Tasks Resource
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

This resource exposes endpoints to support operations at the tasks scope. The initial implementation includes the ability to list all the tasks for a particular job.
This is a sub-resource of the [Jobs Resource &raquo;](jobs.html) and is not intended to be used independently.

Responses of individual endpoints will vary in accordance with their functionality and scope. However, the error
messages of all of the tasks resource end points will be of the following form.

**Error Message**

Every error response will have the following structure:

{% highlight json %}
{
    "message": "Unrecognized status parameter: null"
}
{% endhighlight %}
`message` is the only field in the response and contains a description of the problem.
<br>

## Get All Tasks
Lists the complete details about all the tasks for a particular job

######Request
    GET /v1/jobs/{jobName}/{jobId}/tasks

######Response
Status: 200 OK

{% highlight json %}
 [
{
   "preferredHost" : "samza-preferredHost",
   "taskName" : "Samza task",
   "containerId" : "0",
   "partitions" : [{
                      "system" : "kafka",
                      "stream" : "topic-name",
                      "partitionId" : "0"
                    }]
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
      <td>200 OK</td><td>The operation completed successfully and all the tasks that for 
      the job are returned.</td>
    </tr>
    <tr>
      <td>404 Not Found</td><td>Invalid job instance was provided as an argument.{% highlight json %}
{
    "message": "Invalid arguments for getTasks. jobName: SamzaJobName jobId: SamzaJobId."
}
{% endhighlight %}</td>
    </tr>
    <tr>
      <td>500 Server Error</td><td>There was an error executing the command on the server. e.g. The command timed out.{% highlight json %}
{
    "message": "Timeout waiting for get all tasks."
}
{% endhighlight %}</td>
    </tr>
  </tbody>
</table>
<br/>

<br/>

###Design
###Abstractions
There are two primary abstractions that are required by the TasksResource that users can implement to handle any details specific to their environment.

1.  **TaskProxy**: This interface is the central point of interacting with Samza tasks. It exposes a method to get all the tasks of a Samza job.
2.  **InstallationFinder**: The InstallationFinder provides a generic interface to discover all the installed jobs, hiding any customizations in the job package structure and its location. The InstallationFinder also resolves the job configuration, which is used to validate and identify the job.

## Configuration
The TasksResource properties should be specified in the same file as the Samza REST configuration.

<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Name</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>task.proxy.factory.class</td><td><b>Required:</b> The TaskProxyFactory that will be used to create the TaskProxy instances. The value is a fully-qualified class name which must implement TaskProxyFactory. Samza ships with one implementation: <pre>org.apache.samza.rest.proxy.task.SamzaTaskProxy</pre> <li> gets the details of all the tasks of a job. It uses the <pre>SimpleInstallationRecord</pre> to interact with Samza jobs installed on disk.</li></td>
    </tr>
    <tr>
      <td>job.installations.path</td><td><b>Required:</b> The file system path which contains the Samza job installations. The path must be on the same host as the Samza REST Service. Each installation must be a directory with structure conforming to the expectations of the InstallationRecord implementation used by the JobProxy.</td>
    </tr>
    <tr>
      <td>job.config.factory.class</td><td>The config factory to use for reading Samza job configs. This is used to fetch the job.name and job.id properties for each job instance in the InstallationRecord. It's also used to validate that a particular directory within the installation path actually contains Samza jobs. If not specified <pre>org.apache.samza.config.factories.PropertiesConfigFactory</pre> will be used. </td>
    </tr>
  </tbody>
</table>
