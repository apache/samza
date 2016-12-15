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

The event loop orchestrates [reading and processing messages](streams.html), [checkpointing](checkpointing.html), [windowing](windowing.html) and [flushing metrics](metrics.html) among tasks. 

By default Samza uses a single thread in each [container](samza-container.html) to run the tasks. This fits CPU-bound jobs well; to get more CPU processors, simply add more containers. The single thread execution also simplifies sharing task state and resource management.

For IO-bound jobs, Samza supports finer-grained parallelism for both synchronous and asynchronous tasks. For synchronous tasks ([StreamTask](../api/javadocs/org/apache/samza/task/StreamTask.html) and [WindowableTask](../api/javadocs/org/apache/samza/task/WindowableTask.html)), you can schedule them to run in parallel by configuring the build-in thread pool [job.container.thread.pool.size](../jobs/configuration-table.html). This fits the blocking-IO task scenario. For asynchronous tasks ([AsyncStreamTask](../api/javadocs/org/apache/samza/task/AsyncStreamTask.html)), you can make async IO calls and trigger callbacks upon completion. The finest degree of parallelism Samza provides is within a task, and is configured by [task.max.concurrency](../jobs/configuration-table.html).

The latest version of Samza is thread-safe. You can safely access your job’s state in [key-value store](state-management.html), write messages and checkpoint offset in the task threads. If you have other data shared among tasks, such as global variables or static data, it is not thread safe if the data can be accessed concurrently by multiple threads, e.g. StreamTask running in the configured thread pool with more than one threads. For states within a task, such as member variables, Samza guarantees the mutual exclusiveness of process, window and commit so there will be no concurrent modifications among these operations and any state change from one operation will be fully visible to the others.     

### Event Loop Internals

A container may have multiple [SystemConsumers](../api/javadocs/org/apache/samza/system/SystemConsumer.html) for consuming messages from different input systems. Each SystemConsumer reads messages on its own thread, but writes messages into a shared in-process message queue. The container uses this queue to funnel all of the messages into the event loop.

The event loop works as follows:

1. Choose a message from the incoming message queue;
2. Schedule the appropriate [task instance](samza-container.html) to process the message;
3. Schedule window() on the task instance to run if it implements WindowableTask, and the window timer has been triggered;
4. Send any output from the process() and window() calls to the appropriate [SystemProducers](../api/javadocs/org/apache/samza/system/SystemProducer.html);
5. Write checkpoints and flush the state stores for any tasks whose [commit interval](checkpointing.html) has elapsed.
6. Block if all task instances are busy with processing outstanding messages, windowing or checkpointing.

The container does this, in a loop, until it is shut down.

### Semantics for Synchronous Tasks v.s. Asynchronous Tasks

The semantics of the event loop differs when running synchronous tasks and asynchronous tasks:

* For synchronous tasks (StreamTask and WindowableTask), process() and window() will run on the single main thread by default. You can configure job.container.thread.pool.size to be greater than 1, and event loop will schedule the process() and window() to run in the thread pool.  
* For Asynchronous tasks (AsyncStreamTask), processAsync() will always be invoked in a single thread, while callbacks can be triggered from a different user thread. 

In both cases, the default concurrency within a task is 1, meaning at most one outstanding message in processing per task. This guarantees in-order message processing in a topic partition. You can further increase it by configuring task.max.concurrency to be greater than 1. This allows multiple outstanding messages to be processed in parallel by a task. This option increases the parallelism within a task, but may result in out-of-order processing and completion.

The following semantics are guaranteed in any of the above cases (for happens-before semantics, see [here](https://docs.oracle.com/javase/tutorial/essential/concurrency/memconsist.html)):

* If task.max.concurrency = 1, each message process completion in a task is guaranteed to happen-before the next invocation of process()/processAsync() of the same task. If task.max.concurrency > 1, there is no such happens-before constraint and user should synchronize access to any shared/global variables in the Task..
* WindowableTask.window() is called when no invocations to process()/processAsync() are pending and no new process()/processAsync() invocations can be scheduled until it completes. Therefore, a guarantee that all previous process()/processAsync() invocations happen before an invocation of WindowableTask.window(). An invocation to WindowableTask.window() is guaranteed to happen-before any subsequent process()/processAsync() invocations. The Samza engine is responsible for ensuring that window is invoked in a timely manner.
* Checkpointing is guaranteed to only cover events that are fully processed. It happens only when there are no pending process()/processAsync() or WindowableTask.window() invocations. All preceding invocations happen-before checkpointing and checkpointing happens-before all subsequent invocations.

More details and examples can be found in [Samza Async API and Multithreading User Guide](../../../tutorials/{{site.version}}/samza-async-user-guide.html).

### Lifecycle

The only way in which a developer can hook into a SamzaContainer's lifecycle is through the standard InitableTask, ClosableTask, StreamTask/AsyncStreamTask, and WindowableTask. In cases where pluggable logic needs to be added to wrap a StreamTask, the StreamTask can be wrapped by another StreamTask implementation that handles the custom logic before calling into the wrapped StreamTask.

A concrete example is a set of StreamTasks that all want to share the same try/catch logic in their process() method. A StreamTask can be implemented that wraps the original StreamTasks, and surrounds the original process() call with the appropriate try/catch logic. For more details, see [this discussion](https://issues.apache.org/jira/browse/SAMZA-437).

## [Metrics &raquo;](metrics.html)
