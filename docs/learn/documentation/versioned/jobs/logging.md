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

Samza uses [SLF4J](http://www.slf4j.org/) for all of its logging. By default, Samza only depends on slf4j-api, so it can work for whichever underlying logging platform you wish to use. You simply need to add the SLF4J bridge corresponding to the logging implementation chosen. Samza logging has been thoroughly tested against Log4j and Log4j2. Samza provides bundled modules for each of the Log4j versions along with additional functionality.
### Logging with Log4j

To use Samza with [log4j](http://logging.apache.org/log4j/1.2/), you just need to make sure the following dependencies are present in your SamzaContainer’s classpath:
-	samza-log4j
-	slf4j-log4j12

In Maven, this can be done by adding the following dependencies to your Samza package project's pom.xml:

{% highlight xml %}
<dependency>
  <setId>org.slf4j</setId>
  <artifactId>slf4j-log4j12</artifactId>
  <scope>runtime</scope>
  <version>1.7.7</version>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-log4j</artifactId>
  <version>0.14.0</version>
</dependency>
{% endhighlight %}

If you're not using Maven, just make sure that both these dependencies end up in your Samza package's lib directory.

Next, you need to make sure that these dependencies are also listed in your Samza project's build.gradle:

{% highlight bash %}
    compile(group: 'org.slf4j', name: 'slf4j-log4j12', version: "$SLF4J_VERSION")
    runtime(group: 'org.apache.samza', name: 'samza-log4j', version: "$SAMZA_VERSION")
{% endhighlight %}

Note: Please make sure that no dependencies of Log4j2 are present in the classpath while working with Log4j.

#### Log4j configuration

Please ensure you have log4j.xml in your [Samza package's](packaging.html) lib directory. For example, in hello-samza application, the following lines are added to src.xml to ensure log4j.xml is present in the lib directory:

{% highlight xml %}
<files>
  <file>
    <source>${basedir}/src/main/resources/log4j.xml</source>
    <outputDirectory>lib</outputDirectory>
  </file>
</files>
{% endhighlight %}

Samza's [run-class.sh](packaging.html) script will automatically set the following setting if log4j.xml exists in your [Samza package's](packaging.html) lib directory.

{% highlight bash %}
-Dlog4j.configuration=file:$base_dir/lib/log4j.xml
{% endhighlight %}

The [run-class.sh](packaging.html) script will also set the following Java system properties:

{% highlight bash %}
-Dsamza.log.dir=$SAMZA_LOG_DIR
{% endhighlight %}

The [run-container.sh](packaging.html) will also set:

{% highlight bash %}
-Dsamza.container.id=$SAMZA_CONTAINER_ID -Dsamza.container.name=samza-container-$SAMZA_CONTAINER_ID"
{% endhighlight %}

Likewise, [run-am.sh](packaging.html) sets:

{% highlight bash %}
-Dsamza.container.name=samza-application-master
{% endhighlight %}

These settings are very useful if you're using a file-based appender. For example, you can use a rolling appender to separate log file when it reaches certain size by configuring log4j.xml like this:

{% highlight xml %}
<appender name="RollingAppender" class="org.apache.log4j.RollingFileAppender">
   <param name="File" value="${samza.log.dir}/${samza.container.name}.log" />
   <param name="MaxFileSize" value="256MB" />
   <param name="MaxBackupIndex" value="20" />
   <layout class="org.apache.log4j.PatternLayout">
    <param name="ConversionPattern" value="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} [%p] %m%n" />
   </layout>
</appender>
{% endhighlight %}

Setting up a file-based appender is recommended as a better alternative to using standard out. Standard out log files (see below) don't roll, and can get quite large if used for logging.

### Logging with Log4j2

To use Samza with [log4j2](https://logging.apache.org/log4j/2.x/), the following dependencies need to be present in SamzaContainer’s classpath:
-	samza-log4j2
-	log4j-slf4j-impl

In Maven, these can be done by adding the following dependencies to your Samza project's pom.xml:

{% highlight xml %}
<dependency>
  <groupId>org.apache.logging.log4j</groupId>
  <artifactId>log4j-slf4j-impl</artifactId>
  <version>2.8</version>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-log4j2</artifactId>
  <version>0.14.0</version>
</dependency>
{% endhighlight %}

If you’re not using Maven, please make sure both the above dependencies end up in your Samza package’s lib directory.

Next, you need to make sure that these dependencies are also listed in your Samza project's build.gradle:

{% highlight bash %}
    compile(group: 'org.apache.logging.log4j', name: 'log4j-slf4j-impl', version: "2.11.0")
    runtime(group: 'org.apache.samza', name: 'samza-log4j2', version: "$SAMZA_VERSION")
{% endhighlight %}


#### Log4j2 configuration

Please ensure you have log4j2.xml in your [Samza package's](packaging.html) lib directory. For example, in hello-samza application, the following lines are added to src.xml to ensure log4j2.xml is present in the lib directory:

{% highlight xml %}
<files>
  <file>
    <source>${basedir}/src/main/resources/log4j2.xml</source>
    <outputDirectory>lib</outputDirectory>
  </file>
</files>
{% endhighlight %}

Samza's [run-class.sh](packaging.html) script will automatically set the following setting if log4j2.xml exists in your lib directory.

{% highlight bash %}
-Dlog4j.configurationFile=file:$base_dir/lib/log4j2.xml
{% endhighlight %}

Rest all of the system properties will be set exactly like in the case of log4j, stated above.

#### Porting from Log4j to Log4j2

If you are already using log4j and want to upgrade to using log4j2, following are the changes you will need to make in your job:
-	Clean your lib directory. This will be rebuilt with new dependency JARs and xml files.

-	Replace log4j’s dependencies with log4j2’s in your pom.xml/build.gradle as mentioned above. Please ensure that none of log4j’s dependencies remain in pom.xml/build.gradle

-	Create a log4j2.xml to match your existing log4j.xml file. 
-	Rebuild your application

NOTE: Please ensure that your classpath does not contain dependencies for both log4j and log4j2, as this might cause the application logging to not work correctly. 


#### Startup logger
When using a rolling file appender, it is common for a long-running job to exceed the max file size and count. In such cases, the beginning of the logs will be lost. Since the beginning of the logs include some of the most critical information like configuration, it is important to not lose this information. To address this issue, Samza logs this critical information to a "startup logger" in addition to the normal logger. 

##### Log4j:
You can write these log messages to a separate, finite file by including the snippet below in your log4j.xml.

{% highlight xml %}
<appender name="StartupAppender" class="org.apache.log4j.RollingFileAppender">
   <param name="File" value="${samza.log.dir}/${samza.container.name}-startup.log" />
   <param name="MaxFileSize" value="256MB" />
   <param name="MaxBackupIndex" value="1" />
   <layout class="org.apache.log4j.PatternLayout">
    <param name="ConversionPattern" value="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} [%p] %m%n" />
   </layout>
</appender>
<logger name="STARTUP_LOGGER" additivity="false">
   <level value="info" />
   <appender-ref ref="StartupAppender"/>
</logger>
{% endhighlight %}

##### Log4j2:
This can be done in a similar way for log4j2.xml using its defined syntax for xml files. 


#### Changing log levels

##### Log4j:

Sometimes it's desirable to change the Log4J log level from `INFO` to `DEBUG` at runtime so that a developer can enable more logging for a Samza container that's exhibiting undesirable behavior. Samza provides a Log4j class called JmxAppender, which will allow you to dynamically modify log levels at runtime. The JmxAppender class is located in the samza-log4j package, and can be turned on by first adding a runtime dependency to the samza-log4j package:

{% highlight xml %}
<dependency>
  <setId>org.apache.samza</setId>
  <artifactId>samza-log4j</artifactId>
  <scope>runtime</scope>
  <version>${samza.version}</version>
</dependency>
{% endhighlight %}

And then updating your log4j.xml to include the appender:

{% highlight xml %}
<appender name="jmx" class="org.apache.samza.logging.log4j.JmxAppender" />
{% endhighlight %}

##### Log4j2:

Log4j2 provides built-in support for JMX where all LoggerContexts, LoggerConfigs and Appenders are instrumented with MBeans and can be remotely monitored and controlled. This eliminates the need for a dedicated JMX appender. The steps to analyze and change the logger/appender properties at runtime are documented [here](https://logging.apache.org/log4j/2.0/manual/jmx.html).

NOTE: If you use JMXAppender and are migrating from log4j to log4j2, simply remove it from your xml file. Don’t add it to your log4j2.xml file as it doesn’t exist in the samza-log4j2 module.  


#### Stream Appender

Samza provides a StreamAppender to publish the logs into a specific system. You can specify the system name using "task.log4j.system" and change name of log stream with param 'StreamName'. The [MDC](http://logback.qos.ch/manual/mdc.html) contains the keys "containerName", "jobName" and "jobId", which help identify the source of the log. In order to use this appender, define the system name by specifying the config as follows:
{% highlight xml %}
task.log4j.system="<system-name>"
{% endhighlight %}

Also, the following needs to be added to the respective log4j.xml/log4j2.xml files:

##### Log4j:

{% highlight xml %}
<appender name="StreamAppender" class="org.apache.samza.logging.log4j.StreamAppender">
   <!-- optional -->
   <param name="StreamName" value="EpicStreamName"/>
   <layout class="org.apache.log4j.PatternLayout">
     <param name="ConversionPattern" value="%X{containerName} %X{jobName} %X{jobId} %d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} [%p] %m%n" />
   </layout>
</appender>
{% endhighlight %}

and

{% highlight xml %}
<appender-ref ref="StreamAppender"/>
{% endhighlight %}

##### Log4j2:

{% highlight xml %}
<Stream name="StreamAppender" streamName="TestStreamName">
  <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} [%p] %m%n"/>
</Stream>
{% endhighlight %}

and

{% highlight xml %}
<AppenderRef ref="StreamAppender"/>
{% endhighlight %}



The default stream name for logger is generated using the following convention,
 ```java
 "__samza_%s_%s_logs" format (jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
 ```
though you can override it using the `StreamName` property in the xml files as shown above.

Configuring the StreamAppender will automatically encode messages using logstash's [Log4J JSON format](https://github.com/logstash/log4j-jsonevent-layout). Samza also supports pluggable serialization for those that prefer non-JSON logging events. This can be configured the same way other stream serializers are defined:

{% highlight jproperties %}
serializers.registry.log4j-string.class=org.apache.samza.logging.log4j.serializers.LoggingEventStringSerdeFactory
systems.mock.streams.__samza_jobname_jobid_logs.samza.msg.serde=log4j-string
{% endhighlight %}

The StreamAppender will always send messages to a job's log stream keyed by the container name.

### Log Directory

Samza will look for the `SAMZA_LOG_DIR` environment variable when it executes. If this variable is defined, all logs will be written to this directory. If the environment variable is empty, or not defined, then Samza will use `$base_dir`, which is the directory one level up from Samza's [run-class.sh](packaging.html) script. This environment variable can also be referenced inside log4j.xml files (see above).

### Garbage Collection Logging

Samza will automatically set the following garbage collection logging setting, and will output it to `$SAMZA_LOG_DIR/gc.log`.

{% highlight bash %}
-XX:+PrintGCDateStamps -Xloggc:$SAMZA_LOG_DIR/gc.log
{% endhighlight %}

#### Rotation

In older versions of Java, it is impossible to have GC logs roll over based on time or size without the use of a secondary tool. This means that your GC logs will never be deleted until a Samza job ceases to run. As of [Java 6 Update 34](http://www.oracle.com/technetwork/java/javase/2col/6u34-bugfixes-1733379.html), and [Java 7 Update 2](http://www.oracle.com/technetwork/java/javase/7u2-relnotes-1394228.html), [new GC command line switches](http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6941923) have been added to support this functionality. If GC log file rotation is supported by the JVM, Samza will also set:

{% highlight bash %}
-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10241024
{% endhighlight %}

### YARN

When a Samza job executes on a YARN grid, the `$SAMZA_LOG_DIR` environment variable will point to a directory that is secured such that only the user executing the Samza job can read and write to it, if YARN is [securely configured](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html).

#### STDOUT

Samza's [ApplicationMaster](../yarn/application-master.html) pipes all STDOUT and STDERR output to logs/stdout and logs/stderr, respectively. These files are never rotated.

## [Reprocessing &raquo;](reprocessing.html)
