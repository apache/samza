---
layout: page
title: Samza Code Examples
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


### Checking out our examples

The [hello-samza](https://github.com/apache/samza-hello-samza) project contains several examples to help you create your Samza applications. To checkout the hello-samza project:

{% highlight bash %}
> git clone https://git.apache.org/samza-hello-samza.git hello-samza
{% endhighlight %}

#### High-level API examples
[The Samza Cookbook](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/cookbook) contains various recipes using the Samza high-level API.
These include:

- The [Filter example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/FilterExample.java) demonstrates how to perform stateless operations on a stream. 

- The [Join example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/JoinExample.java) demonstrates how you can join a Kafka stream of page-views with a stream of ad-clicks

- The [Stream-Table Join example](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/RemoteTableJoinExample.java) demonstrates how to use the Samza Table API. It joins a Kafka stream with a remote dataset accessed through a REST service.

- The [SessionWindow](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/SessionWindowExample.java) and [TumblingWindow](https://github.com/apache/samza-hello-samza/blob/latest/src/main/java/samza/examples/cookbook/TumblingWindowExample.java) examples illustrate Samza's rich windowing and triggering capabilities.


In addition to the cookbook, you can also consult these:

- [Wikipedia Parser](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/wikipedia): An advanced example that builds a streaming pipeline consuming a live-feed of wikipedia edits, parsing each message and generating statistics from them.


- [Amazon Kinesis](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/kinesis) and [Azure Eventhubs](https://github.com/apache/samza-hello-samza/tree/latest/src/main/java/samza/examples/azure) examples that cover how to consume input data from the respective systems.

#### Low-level API examples
The [Wikipedia Parser (low-level API)](https://github.com/apache/samza-hello-samza/tree/latest/src/main/java/samza/examples/wikipedia/task/application): 
Same example that builds a streaming pipeline consuming a live-feed of 
wikipedia edits, parsing each message and generating statistics from them, but
using low-level APIs. 

#### Samza SQL API examples
You can easily create a Samza job declaratively using 
[Samza SQL](https://samza.apache.org/learn/tutorials/latest/samza-sql.html).
