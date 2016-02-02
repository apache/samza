---
layout: page
title: Coordinator Stream
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
Samza job is completely driven by the job configuration. Thus, job configurations tend to be pretty large. In order to easily serialize such large configs and persist them between job executions, Samza writes all configurations to a durable stream called the *Coordinator Stream* when a job is submitted.

A Coordinator Stream is single partitioned stream to which the configurations are written to. It shares the same characteristics as any input stream that can be configured in Samza - ordered, replayable and fault-tolerant. The stream will contain three major types of messages:

1. Job configuration messages
2. Task changelog partition assignment messages
3. Container locality message

### Coordinator Stream Naming

The naming convention is very similar to that of the checkpoint topic that get's created.

```java
"__samza_coordinator_%s_%s" format (jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
```

### Coordinator Stream Message Model
Coordinator stream messages are modeled as key/value pairs. The key is a list of well defined fields: *version*, *type*, and *key*. The value is a *map*. There are some pre-defined fields (such as timestamp, host, etc) for the value map, which are common to all messages.

The full structure for a CoordinatorStreamMessage is:

```json
key => ["<version-number>", "<message-type>", "<key>"]

message => {
    "host": "<hostname>",
    "username": "<username>",
    "source": "<source-for-this-message>",
    "timestamp": <timestamp-of-the-message>,
    "values": { }
}
```

The messages are essentially serialized and transmitted over the wire as JSON blobs. Hence, for serialization to work correctly, it is very important to not have any unnecessary white spaces. The white spaces in the above JSON blob have been shown for legibility only.  

The most important fields are type, key, and values:

* type - defines the kind of message
* key - defines a key to associate with the values
* values map - defined on a per-message-type basis, and defines a set of values associated with the type

The coordinator stream messages that are currently supported are listed below:
<style>
            table th, table td {
                text-align: left;
                vertical-align: top;
                padding: 12px;
                border-bottom: 1px solid #ccc;
                border-top: 1px solid #ccc;
                border-left: 0;
                border-right: 0;
            }

            table td.property, table td.default {
                white-space: nowrap;
            }

            table th {
                background-color: #eee;
            }
</style>
<table>
    <tr>
        <th>Message</th>
        <th>Type</th>
        <th>Key</th>
        <th>Values Map</th>
    </tr>
    <tr>
        <td> Configuration Message <br />
            (Applies to all configuration <br />
             options listed in [Configuration](../jobs/configuration-table.html)) </td>
        <td> set-config </td>
        <td> &lt;config-name&gt; </td>
        <td> 'value' => &lt;config-value&gt; </td>
    </tr>
    <tr>
        <td> Task-ChangelogPartition Assignment Message </td>
        <td> set-changelog </td>
        <td> &lt;[TaskName](../api/org/apache/samza/container/TaskName.java)&gt; </td>
        <td> 'partition' => &lt;Changelog-Partition-Id&gt;
        </td>
    </tr>
    <tr>
        <td> Container Locality Message </td>
        <td> set-container-host-assignment </td>
        <td> &lt;Container-Id&gt; </td>
        <td> 'hostname' => &lt;HostName&gt;
        </td>
    </tr>
</table>

### Coordinator Stream Writer
Samza provides a command line tool to write Job Configuration messages to the coordinator stream. The tool can be used as follows:
{% highlight bash %}
samza-example/target/bin/run-coordinator-stream-writer.sh \
  --config-path=file:///path/to/job/config.properties \
  --type set-config \
  --key yarn.container.count \
  --value 8
{% endhighlight %}


## <a name="JobCoordinator"></a>Job Coordinator

The Job Coordinator bootstraps configuration from the coordinator stream each time upon job start-up. It periodically catches up with any new data written to the coordinator stream and updates the *Job Model*.

Job Model is the data model used to represent a Samza job, which also incorporates the Job configuration. The hierarchy of a Samza job - job has containers, and each of the containers has tasks - is encapsulated in the Job Model, along with relevant information such as container id, task names, partition information, etc.

The Job Coordinator exposes the Job Model and Job Configuration via an HTTP service. The URL for the Job Coordinator's HTTP service is passed as an environment variable to the Samza Containers when the containers are launched. Containers may write meta-information, such as locality - the hostname of the machine on which the container is running. However, they will read the Job Model and Configuration by querying the Job Coordinator via the HTTP service.

Thus, Job Coorindator is the single component that has the latest view of the entire job status. This is very useful as it allows us to extend functionality of the Job Coordinator, in the future, to manage the lifecycle of the job (such as start/stop container, modify task assignment etc).


### Job Coordinator Availability

The Job Coordinator resides in the same container as the Samza Application Master. Thus, the availability of the Job Coordinator is tied to the availability of the Application Master (AM) in the Yarn cluster. The Samza containers are started only after initializing the Job Coordinator from the Coordinator Stream. In stable condition, when the Samza container comes up, it should be able to read the JobModel from the Job Coordinator without timing out. 

## Benefits of Coordinator Stream Model
Writing the configuration to a durable stream opens the door for Samza to do a couple of things:

1. Removes the size-bound on the Job configuration
2. Exposes job-related configuration and metadata to the containers using a standard data model and communication interface (See [Job Coordinator](#JobCoordinator) for details)
3. Certain configurations should only be set one time. Changing them in future deployment amounts to resetting the entire state of the job because it may re-shuffle input partitions to the containers. For example, changing [SystemStreamPartitionGrouper](../api/javadocs/org/apache/samza/container/grouper/stream/SystemStreamPartitionGrouper.java) on a stateful Samza job would inter-mingle state from different StreamTasks in a single changelog partition. Without persistent configuration, there is no easy way to check whether a job's current configuration is valid or not.
4. Job configuration can be dynamically changed by writing to the Coorinator Stream. This can enable features that require the job to be reactive to configuration change (eg. host-affinity, auto-scaling, dynamic reconfiguration etc).
5. Provides a unified view of the job state, enabling Samza with more powerful ways of controlling container controls (See [Job Coordinator](#JobCoordinator) for details)
6. Enables future design of Job Coordinator fail-over since it serves as a single source of truth of the current job state


For other interesting features that can leverage this model, please refer the [design document](https://issues.apache.org/jira/secure/attachment/12670650/DESIGN-SAMZA-348-1.pdf).
