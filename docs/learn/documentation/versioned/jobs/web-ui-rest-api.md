---
layout: page
title: Web UI and REST API
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

### Dashboard

Samza's ApplicationMaster comes with a dashboard to show useful information such as:

1. Where containers are located.
2. Links to logs.
3. The Samza job's configuration.
4. Container failure count.

You can find this dashboard by going to your YARN grid's ResourceManager page (usually something like [http://localhost:8088/cluster](http://localhost:8088/cluster)), and clicking on the "ApplicationMaster" link of a running Samza job.

<img src="/img/{{site.version}}/learn/documentation/yarn/samza-am-dashboard.png" alt="Screenshot of ApplicationMaster dashboard" class="diagram-large">

### REST API

REST API produces JSON and provides information about metrics, task context, config, containers and status.

<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Endpoint</th>
      <th>Meaning</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>/metrics</td><td>HahsMap that represents metrics for every 60 seconds.</td>
    </tr>
    <tr>
      <td>/task-context</td><td>HashMap that provides information about task context including task name and task ID.</td>
    </tr>
    <tr>
      <td>/am</td><td>HashMap that represents information about containers and status.</td>
    </tr>
    <tr>
      <td>/config</td><td>HashMap that represents the config.</td>
    </tr>
  </tbody>
</table>

## [Application Master &raquo;](../yarn/application-master.html)
