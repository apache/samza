---
layout: page
title: Monitors
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


Samza REST supports the ability to add Monitors to the service. The initial implementation is very basic. Monitors are essentially tasks that can be scheduled to run periodically. They do not read the config and they are all scheduled at the same global interval. More capabilities will be added later, but the initial implementation supports simple cases like monitoring the YARN NodeManager and restarting it if it dies.

## Implementing a New Monitor
Implement the [Monitor](javadocs/org/apache/samza/monitor/Monitor.html) interface with some behavior that should be executed periodically. The Monitor is Java code that invokes some method on the SAMZA Rest Service, runs a bash script to restart a failed NodeManager, or cleans old RocksDB sst files left by Host Affinity, for example.

## Adding a New Monitor to the Samza REST Service
Add the fully-qualified class name of the Monitor implementation to the `monitor.classes` property in the service config.

Set the `monitor.run.interval.ms` property to the appropriate interval. The `monitor()` method will be invoked at this interval.

For more information on these properties, see the config table in the [Overview page.](overview.html)

## [Resource Reference &raquo;](resource-directory.html)
