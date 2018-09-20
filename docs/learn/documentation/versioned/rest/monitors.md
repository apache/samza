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


Samza REST supports the ability to add Monitors to the service. Monitors are essentially tasks that can be scheduled to run periodically.
It provides the capability to the users to define configurations that are specific to individual Monitors.
These configurations are injected into the monitor instances through the Config instances.

## Monitor configuration
All of the configuration keys for the monitors should be prefixed with monitor.{monitorName}.
Since each monitor is expected to have an unique name, these prefixes provide the namespacing across
the monitor configurations.

The following configurations are required for each of the monitors.
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
            <td>monitor.monitorName.scheduling.interval.ms</td>
            <td></td>
            <td>This defines the periodic scheduling interval in milliseconds
            for a monitor named monitorName. If this configuration is
            not defined, it is defaulted to 60 seconds.</td>
          </tr>
          <tr>
            <td>monitor.monitorName.scheduling.jitter.percent</td>
            <td></td>
            <td>
            Defines the random jitter percentage that should be added to the monitor
            scheduling interval for a monitor named monitorName. If undefined,
            it is defaulted to zero.</td>
          </tr>
          <tr>
            <td>monitor.monitorName.factory.class</td>
            <td></td>
            <td>
            <b>Required:</b> This should contain a fully qualified name
            of a class that implements the MonitorFactory interface.
            Monitors that are instantiated by the factory implementation will be scheduled for periodic execution.
            Custom implementations of the MonitorFactory interface are expected to inject the Config
            and MetricsRegistry instances available in the createMonitor method into the Monitors.
            </td>
          </tr>
          </tr>
        </tbody>
  </table>

  For example, configurations for two monitors named NMTaskMonitor and RMTaskMonitor should be defined as follows.

  {% highlight jproperties %}
  monitor.RMTaskMonitor.factory.class=org.apache.samza.monitor.RMTaskMonitor

  monitor.RMTaskMonitor.scheduling.interval.ms=1000

  monitor.RMTaskMonitor.custom.config.key1=configValue1

  monitor.NMTaskMonitor.factory.class=org.apache.samza.monitor.NMTaskMonitor

  monitor.NMTaskMonitor.scheduling.interval.ms=2000

  monitor.NMTaskMonitor.custom.config.key2=configValue2

  {% endhighlight %}

## Implementing a New Monitor
Implement the [Monitor](javadocs/org/apache/samza/monitor/Monitor.html) interface with some behavior that should be executed periodically. The Monitor is Java code that invokes some method on the SAMZA Rest Service, runs a bash script to restart a failed NodeManager, or cleans old RocksDB sst files left by Host Affinity, for example.

Implement the [MonitorFactory](javadocs/org/apache/samza/monitor/MonitorFactory.html) interface,
which will be used to instantiate your Monitor. Each Monitor implementation should
have a associated MonitorFactory implementation, which is responsible for instantiating the monitors.

## Adding a New Monitor to the Samza REST Service
Add the fully-qualified class name of the MonitorFactory implementation to the `monitor.monitorName.factory.class` property in the service config.
Set the config key `monitor.monitorName.scheduling.interval.ms` to the scheduling interval in milliseconds.

The configuration key `monitor.monitorName.scheduling.interval.ms` defines the periodic scheduling interval of
the `monitor()` method in milli seconds.

## Reporting metrics from Monitors
Samza REST service allows the users to create and report metrics from their monitors. Reporting metrics to a metrics system is encapsulated by the metrics reporter, which should be defined in the samza-rest configuration file. Configurations for metrics reporters in Samza REST service are the same as [that of Samza Jobs](../container/metrics.md).

## [Resource Reference &raquo;](resource-directory.html)
