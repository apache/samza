---
layout: page
title: Event Loop
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

The event loop is the [container](samza-container.html)'s single thread that is in charge of [reading and writing messages](streams.html), [flushing metrics](metrics.html), [checkpointing](checkpointing.html), and [windowing](windowing.html).

Samza uses a single thread because every container is designed to use a single CPU core; to get more parallelism, simply run more containers. This uses a bit more memory than multithreaded parallelism, because each JVM has some overhead, but it simplifies resource management and improves isolation between jobs. This helps Samza jobs run reliably on a multitenant cluster, where many different jobs written by different people are running at the same time.

You are strongly discouraged from using threads in your job's code. Samza uses multiple threads internally for communicating with input and output streams, but all message processing and user code runs on a single-threaded event loop. In general, Samza is not thread-safe.

### Event Loop Internals

A container may have multiple [SystemConsumers](../api/javadocs/org/apache/samza/system/SystemConsumer.html) for consuming messages from different input systems. Each SystemConsumer reads messages on its own thread, but writes messages into a shared in-process message queue. The container uses this queue to funnel all of the messages into the event loop.

The event loop works as follows:

1. Take a message from the incoming message queue;
2. Give the message to the appropriate [task instance](samza-container.html) by calling process() on it;
3. Call window() on the task instance if it implements [WindowableTask](../api/javadocs/org/apache/samza/task/WindowableTask.html), and the window time has expired;
4. Send any output from the process() and window() calls to the appropriate [SystemProducers](../api/javadocs/org/apache/samza/system/SystemProducer.html);
5. Write checkpoints for any tasks whose [commit interval](checkpointing.html) has elapsed.

The container does this, in a loop, until it is shut down. Note that although there can be multiple task instances within a container (depending on the number of input stream partitions), their process() and window() methods are all called on the same thread, never concurrently on different threads.

### Lifecycle

The only way in which a developer can hook into a SamzaContainer's lifecycle is through the standard InitableTask, ClosableTask, StreamTask, and WindowableTask. In cases where pluggable logic needs to be added to wrap a StreamTask, the StreamTask can be wrapped by another StreamTask implementation that handles the custom logic before calling into the wrapped StreamTask.

A concrete example is a set of StreamTasks that all want to share the same try/catch logic in their process() method. A StreamTask can be implemented that wraps the original StreamTasks, and surrounds the original process() call with the appropriate try/catch logic. For more details, see [this discussion](https://issues.apache.org/jira/browse/SAMZA-437).

## [JMX &raquo;](jmx.html)
