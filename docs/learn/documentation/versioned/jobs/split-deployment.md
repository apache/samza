---
layout: page
title: Separating Samza Framework and Jobs Deployment
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


### Motivation
Currently all Samza jobs are deployed as a single unit/package which combines all the Samza libraries, user code and configs together. Typically in a large organization the team that manages the Samza cluster is not the same as the teams that are running applications on top of Samza. In this case, the current way of deployment presents two major problems:

* **Samza software releases**:
Every time Samza team releases a new version (for example a bug fix), the only way to deploy it is to rebuild all users packages and redeploy them. It would be much more efficient if the team could release the Samza framework separately, at its own cadence, and a simple job restart would pick up the new version.

* **Packages incompatibilities**:
If both Samza and a job depend on the same software, but on different (especially backward incompatible) versions, they cannot be released together, because it will most likely cause some runtime issue. Ideally, each one of them would load the packages it needs separately.
<b>NOTE.</b>This problem is not addressed here.

To address the first problem, we separate the deployment of the Samza framework from user jobs by defining two deployable units:

* **Samza framework** - This contains Samza libraries only, and is deployed separately to all the machines in a cluster.
* **User's job** - This contains user code only, and uses the pre-deployed Samza framework to run.

Split deployment allows upgrading the Samza framework without forcing developers to explicitly upgrade their running applications. It also allows different versions of Samza framework with simple config changes. This means we can support canary, upgrade and rollback scenarios commonly
required in organizations that run tens or hundreds of jobs.

### Deployment sequence

#### Pre-requisite for split deployment
Each deployment will now consist of two separate packages:<p>

1. **Samza framework** - This includes all Samza libraries and scripts, such as samza-api, samza-core, samza-log4j, samza-kafka, samza-yarn, samza-timeSeriesValue, samza-timeSeriesValue-inmemory, samza-timeSeriesValue-rocksdb, samza-shell, samza-hdfs and all their dependencies.
2. **User's job** - This includes the job package: all user code for the StreamTask implementation, configs, and other libraries required by the job. The job's package should depend only samza-api and no other Samza libraries. The package won't be able to start by itself. In order to start, it will need to use the Samza framework.

#### Deployment steps
To run a job in split deployment mode:

1. **Deploy the framework**:
The Samza framework package should be deployed to ALL the machines of a cluster into a predefined, fixed location. This could be done by merely copying the jars, or creating a meta package that would deploy all of them. Let's assume that 'samza-framework' package is installed into the '/.../samza-fwk/0.12.0' directory.

2. **Create symbolic link**:
A symbolic link needs to be created for the **stable** version of the framework to point to the framework location, e.g.: {% highlight bash %} ln -s /.../samza-fwk/0.12.0 /.../samza-fwk/STABLE' {% endhighlight %}

3. **Deploy user job**:
In the job's config, the following property is required to enable split deployment, e.g. for Samza framework path at '/.../samza-fwk': {% highlight jproperties %} samza.fwk.path=/.../samza-fwk {% endhighlight %} By default Samza will look for the **stable** link inside the folder to find the framework. You can also override the version by configuring: {% highlight jproperties %} samza.fwk.version=0.11.1 {% endhighlight %} In this case Samza will pick '/.../samza-fwk/0.11.1' as the framework location. This way users can perform canary, upgrade and rollback their jobs easily by changing version in the config.
