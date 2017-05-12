---
layout: page
title: Hello Samza Fluent Code Walkthrough
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

This tutorial covers the fluent API by showing you how the simple wikipedia application from the [hello samza fluent tutorial] (hello-samza-fluent.html) was written. Upon completion of this tutorial, you'll know how to implement and configure a [StreamApplication](/javadocs/djdjdjdjdjdj). You'll also see how to use some of the basic fluent operators as well as how to leverage KeyValueStores and metrics in an app.  

The same hello-samza project is used for this tutorial as many of the others. You will clone that project and by the end of the tutorial, you will have implemented a duplicate of the WikipediaApplication. 

Let's get started.

### Get the Code

Check out the hello-samza project:

{% highlight bash %}
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
{% endhighlight %}

This project already contains implementations of the wikipedia application using both the low-level task API and the high-level fluent API. The low-level task implementations are in the `samza.examples.wikipedia.task` package. The high-level application implementation is in the `samza.examples.wikipedia.application` package. This tutorial will create another high-level application with the same functionality to illustrate how to build stream applications with Samza.

### Wikipedia Consumer
In order to consume events from Wikipedia, the hello-samza project includes a `WikipediaSystemFactory` which implements the Samza [SystemFactory](javadocs/org/apache/samza/system/SystemFactory.html) interface and provides a `WikipediaConsumer`. The WikipediaConsumer is a [SystemConsumer](javadocs/org/apache/samza/system/SystemConsumer.html) implementation that can consume events from Wikipedia. It is also a listener for events from the `WikipediaFeed`. It's important to note that the events received in `onEvent` are of the type `WikipediaFeedEvent`, so we will expect that type for messages on our input streams. For other applications, the messages may come in the form of `byte[]`. In that case you may want to configure a samza [serde](/learn/documentation/{{site.version}}/container/serialization.html) and exepect the output type of that serde. 

Now that we know the types of inputs we'll be processing, we can proceed with creating our application.

### Create the Initial Config
In the hello-samza project, configs are kept in the _src/main/config/_ path. This is where we will add the config for our application.
Add a new file named _my-wikipedia-application.properties_ in this location.

#### Core Configuration
Let's start by adding some of the core properties to the file:

{% highlight bash %}
# Application / Job
app.class=samza.examples.wikipedia.application.MyWikipediaApplication
app.runner.class=org.apache.samza.runtime.RemoteApplicationRunner

job.factory.class=org.apache.samza.job.yarn.YarnJobFactory
job.name=my-wikipedia-application

# YARN
yarn.package.path=file://${basedir}/target/${project.artifactId}-${pom.version}-dist.tar.gz
{% endhighlight %}

Here's a brief summary of what we configured so far.

* **app.class**: the class that defines the application logic. We will create this class later.
* **app.runner.class**: the runner implementation which will launch our application. Since we are using YARN, we use `RemoteApplicationRunner` which is required for any cluster-based deployment.
* **job.factory.class**: the [factory](/learn/documentation/{{site.version}}/jobs/job-runner.html) that will create the runtime instances of our jobs. Since we are using YARN, we want each job to be created as a [YARN job](/learn/documentation/{{site.version}}/jobs/yarn-jobs.html), so we use `YarnJobFactory`
* **job.name**: the primary identifier for the job.
* **yarn.package.path**: tells YARN where to find the [job package](/learn/documentation/{{site.version}}/jobs/packaging.html) so the Node Managers can download it.

If the application had no interactions with the outside world, these configurations would be enough to run it in its most basic form (TODO TRUE?). Of course, a stream application is not very interesting if it doesn't consume any streams, so let's define those next.

#### Define Systems
#### Configure Atypical Streams
* Default system is kafka because we want all metadata (coord, chkpt) intermediate values (N/A here, but in general)and outputs to go there. Take a moment to explain that all streams for which a system is not explicitly defined will be bound to the default system. Maybe even give a shore expl of what a system is and why it's useful.
* Define wikipedia system 
* Map wikipedia input streams to wikipedia system, since they aren't the default
* Wikipedia uses special characters in channel names which are not allowed as Samza stream IDs in the fluent API, so lets create a mapping from streamID to channel

### Create a StreamApplication


* What to extend
* Look around at the methods and signatures. Point out what's important. 

### Define Application Logic
Let's create the app class you configured above. 

Create a new class named `MyWikipediaApplication` in the `samza.examples.wikipedia.application` package. The class must implement the `StreamApplication` interface and should look like this:

{% highlight java %}
package samza.examples.wikipedia.application;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;

public class MyWikipediaApplication implements StreamApplication{
  @Override
  public void init(StreamGraph streamGraph, Config config) {
    
  }
}
{% endhighlight %}

#### Inputs
* Declare inputs using streamIds from config. If they didn't have special characters, the physicalName and streamID could be used interchangablly and physicalName would not need to be configured.
* Note types are what come from consumer or serde, depending on impl. Here WikipediaFeedEvents (just like in the wikipedia feed job)

#### Merge
* Merge inputs to get one stream of events to operate on. This is not a join!

#### Parse
* Use Map to parse the events similarly to the Parse job.

#### Window
* Declare Stats object we want to aggregate. Describe how it will be used by window and as a carrier for the corresponding total value.
* Create a window aggregator

#### Output
* Use Map with a method to format output
* Declare output INLINE like Prateek showed. Here's the opportunity to illustrate streamId/phyName. "Why didn't we configure this stream?" Its on the default system and topic doesn't have special chars
* For readability, refactor output

#### KVStore
* Add KVStore persistent total

#### Metrics
* Add metrics

#### Run and View Plan
* Run with instructions from other tutorial
* Compare to execution plan

### Summary

