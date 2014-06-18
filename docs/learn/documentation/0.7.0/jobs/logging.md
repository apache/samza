---
layout: page
title: Logging
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

Samza uses [SLF4J](http://www.slf4j.org/) for all of its logging. By default, Samza only depends on slf4j-api, so you must add an SLF4J runtime dependency to your Samza packages for whichever underlying logging platform you wish to use.

### Log4j

The [hello-samza](/startup/hello-samza/0.7.0) project shows how to use [log4j](http://logging.apache.org/log4j/1.2/) with Samza. To turn on log4j logging, you just need to make sure slf4j-log4j12 is in your SamzaContainer's classpath. In Maven, this can be done by adding the following dependency to your Samza package project.

{% highlight xml %}
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-log4j12</artifactId>
  <scope>runtime</scope>
  <version>1.6.2</version>
</dependency>
{% endhighlight %}

If you're not using Maven, just make sure that slf4j-log4j12 ends up in your Samza package's lib directory.

#### Log4j configuration

Samza's [run-class.sh](packaging.html) script will automatically set the following setting if log4j.xml exists in your [Samza package's](packaging.html) lib directory.

{% highlight bash %}
-Dlog4j.configuration=file:$base_dir/lib/log4j.xml
{% endhighlight %}

The [run-class.sh](packaging.html) script will also set the following Java system properties:

{% highlight bash %}
-Dsamza.log.dir=$SAMZA_LOG_DIR -Dsamza.container.name=$SAMZA_CONTAINER_NAME=
{% endhighlight %}

These settings are very useful if you're using a file-based appender. For example, you can use a daily rolling appender by configuring log4j.xml like this:

{% highlight xml %}
<appender name="RollingAppender" class="org.apache.log4j.DailyRollingFileAppender">
   <param name="File" value="${samza.log.dir}/${samza.container.name}.log" />
   <param name="DatePattern" value="'.'yyyy-MM-dd" />
   <layout class="org.apache.log4j.PatternLayout">
    <param name="ConversionPattern" value="%d{yyyy-MM-dd HH:mm:ss} %c{1} [%p] %m%n" />
   </layout>
</appender>
{% endhighlight %}

Setting up a file-based appender is recommended as a better alternative to using standard out. Standard out log files (see below) don't roll, and can get quite large if used for logging.

**NOTE:** If you use the `task.opts` configuration property, the log configuration is disrupted. This is a known bug; please see [SAMZA-109](https://issues.apache.org/jira/browse/SAMZA-109) for a workaround.

### Log Directory

Samza will look for the `SAMZA_LOG_DIR` environment variable when it executes. If this variable is defined, all logs will be written to this directory. If the environment variable is empty, or not defined, then Samza will use /tmp. This environment variable can also be referenced inside log4j.xml files (see above).

### Garbage Collection Logging

Samza's will automatically set the following garbage collection logging setting, and will output it to `$SAMZA_LOG_DIR/gc.log`.

{% highlight bash %}
-XX:+PrintGCDateStamps -Xloggc:$SAMZA_LOG_DIR/gc.log
{% endhighlight %}

#### Rotation

In older versions of Java, it is impossible to have GC logs roll over based on time or size without the use of a secondary tool. This means that your GC logs will never be deleted until a Samza job ceases to run. As of [Java 6 Update 34](http://www.oracle.com/technetwork/java/javase/2col/6u34-bugfixes-1733379.html), and [Java 7 Update 2](http://www.oracle.com/technetwork/java/javase/7u2-relnotes-1394228.html), [new GC command line switches](http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6941923) have been added to support this functionality. If you are using a version of Java that supports GC log rotation, it's highly recommended that you turn it on.

### YARN

When a Samza job executes on a YARN grid, the `$SAMZA_LOG_DIR` environment variable will point to a directory that is secured such that only the user executing the Samza job can read and write to it, if YARN is [securely configured](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html).

#### STDOUT

Samza's [ApplicationMaster](../yarn/application-master.html) pipes all STDOUT and STDERR output to logs/stdout and logs/stderr, respectively. These files are never rotated.

## [Reprocessing &raquo;](reprocessing.html)
