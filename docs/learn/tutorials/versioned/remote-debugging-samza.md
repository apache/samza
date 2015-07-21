---
layout: page
title: Remote Debugging with Samza
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

Let's use Eclipse to attach a remote debugger to a Samza container. If you're an IntelliJ user, you'll have to fill in the blanks, but the process should be pretty similar. This tutorial assumes you've already run through the [Hello Samza](../../../startup/hello-samza/{{site.version}}/) tutorial.

### Get the Code

Start by checking out Samza, so we have access to the source.

{% highlight bash %}
git clone http://git-wip-us.apache.org/repos/asf/samza.git
{% endhighlight %}

Next, grab hello-samza.

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git
{% endhighlight %}

### Setup the Environment

Now, let's setup the Eclipse project files.

{% highlight bash %}
cd samza
./gradlew eclipse
{% endhighlight %}

Let's also release Samza to Maven's local repository, so hello-samza has access to the JARs that it needs.

{% highlight bash %}
./gradlew -PscalaVersion=2.10 clean publishToMavenLocal
{% endhighlight %}

Next, open Eclipse, and import the Samza source code into your workspace: "File" &gt; "Import" &gt; "Existing Projects into Workspace" &gt; "Browse". Select 'samza' folder, and hit 'finish'.

### Enable Remote Debugging

Now, go back to the hello-samza project, and edit ./src/main/config/wikipedia-feed.properties to add the following line:

{% highlight jproperties %}
task.opts=-agentlib:jdwp=transport=dt_socket,address=localhost:9009,server=y,suspend=y
{% endhighlight %}

The [task.opts](../../documentation/{{site.version}}/jobs/configuration-table.html) configuration parameter is a way to override Java parameters at runtime for your Samza containers. In this example, we're setting the agentlib parameter to enable remote debugging on localhost, port 9009. In a more realistic environment, you might also set Java heap settings (-Xmx, -Xms, etc), as well as garbage collection and logging settings.

*NOTE: If you're running multiple Samza containers on the same machine, there is a potential for port collisions. You must configure your task.opts to assign different ports for different Samza jobs. If a Samza job has more than one container (e.g. if you're using YARN with yarn.container.count=2), those containers must be run on different machines.*

### Start the Grid

Now that the Samza job has been setup to enable remote debugging when a Samza container starts, let's start the ZooKeeper, Kafka, and YARN.

{% highlight bash %}
bin/grid
{% endhighlight %}

If you get a complaint that JAVA_HOME is not set, then you'll need to set it. This can be done on OSX by running:

{% highlight bash %}
export JAVA_HOME=$(/usr/libexec/java_home)
{% endhighlight %}

Once the grid starts, you can start the wikipedia-feed Samza job.

{% highlight bash %}
mvn clean package
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-0.9.1-dist.tar.gz -C deploy/samza
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
{% endhighlight %}

When the wikipedia-feed job starts up, a single Samza container will be created to process all incoming messages. This is the container that we'll want to connect to from the remote debugger.

### Connect the Remote Debugger

Switch back to Eclipse, and set a break point in TaskInstance.process by clicking on a line inside TaskInstance.process, and clicking "Run" &gt; "Toggle Breakpoint". A blue circle should appear to the left of the line. This will let you see incoming messages as they arrive.

Setup a remote debugging session: "Run" &gt; "Debug Configurations..." &gt; right click on "Remote Java Application" &gt; "New". Set the name to 'wikipedia-feed-debug'. Set the port to 9009 (matching the port in the task.opts configuration). Click "Source" &gt; "Add..." &gt; "Java Project". Select all of the Samza projects that you imported (i.e. samza-api, samza-core, etc). If you would like to set breakpoints in your own Stream task, also add the project that contains your StreamTask implementation. Click 'Debug'.

After a few moments, Eclipse should connect to the wikipedia-feed job, and ask you to switch to Debug mode. Once in debug, you'll see that it's broken at the TaskInstance.process method. From here, you can step through code, inspect variable values, etc.

Congratulations, you've got a remote debug connection to your StreamTask!
