---
layout: page
title: Samza Async API and Multithreading User Guide
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

This tutorial provides examples and guide to use Samza asynchronous API and multithreading.

### Synchronous Process with Multithreading

If your job process involves synchronous IO, or blocking IO, you can simply configure the Samza build-in thread pool to run your tasks in parallel. In the following example, SyncRestTask uses Jersey client to makes rest calls in each process().

{% highlight java %}
public class SyncRestTask implements StreamTask, InitableTask, ClosableTask {
  private Client client;
  private WebTarget target;

  @Override
  public void init(Config config, TaskContext taskContext) throws Exception {
    client = ClientBuilder.newClient();
    target = client.target("http://example.com/resource/").path("hello");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Response response = target.request().get();
    System.out.println("Response status code " + response.getStatus() + " received.");
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
{% endhighlight %}

By default Samza will run this task sequentially in a single thread. In below we configure the thread pool of size 16 to run the tasks in parallel:

{% highlight jproperties %}
# Thread pool to run synchronous tasks in parallel.
job.container.thread.pool.size=16
{% endhighlight %}

**NOTE:** The thread pool will be used to run all the synchronous operations of a task, including StreamTask.process(), WindowableTask.window(), and internally Task.commit(). This is for maximizing the parallelism between tasks as well as reducing the blocking time. When running tasks in multithreading, Samza still guarantees the in-order processing of the messages within a task by default.

### Asynchronous Process with AsyncStreamTask API

If your job process is asynchronous, e.g. making non-blocking remote IO calls, [AsyncStreamTask](javadocs/org/apache/samza/task/AsyncStreamTask.html) interface provides the support for it. In the following example AsyncRestTask makes asynchronous rest call and triggers callback once it's complete. 

{% highlight java %}
public class AsyncRestTask implements AsyncStreamTask, InitableTask, ClosableTask {
  private Client client;
  private WebTarget target;

  @Override
  public void init(Config config, TaskContext taskContext) throws Exception {
    client = ClientBuilder.newClient();
    target = client.target("http://example.com/resource/").path("hello");
  }

  @Override
  public void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector,
      TaskCoordinator coordinator, final TaskCallback callback) {
    target.request().async().get(new InvocationCallback<Response>() {
      @Override
      public void completed(Response response) {
        System.out.println("Response status code " + response.getStatus() + " received.");
        callback.complete();
      }

      @Override
      public void failed(Throwable throwable) {
        System.out.println("Invocation failed.");
        callback.failure(throwable);
      }
    });
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
{% endhighlight %}

In the above example, the process is not complete when processAsync() returns. In the callback thread from Jersey client, we trigger [TaskCallback](javadocs/org/apache/samza/task/TaskCallback.html) to indicate the process is done. In order to make sure the callback will be triggered within certain time interval, e.g. 5 seconds, you can config the following property:

{% highlight jproperties %}
# Timeout for processAsync() callback. When the timeout happens, it will throw a TaskCallbackTimeoutException and shut down the container.
task.callback.timeout.ms=5000
{% endhighlight %}

**NOTE:** Samza also guarantees the in-order process of the messages within an AsyncStreamTask by default, meaning the next processAsync() of a task won't be called until the previous processAsync() callback has been triggered.

### Out-of-order Process

In both cases above, Samza supports in-order process by default. Further parallelism is also supported by allowing a task to process multiple outstanding messages in parallel. The following config allows one task to process at most 4 outstanding messages in parallel at a time: 

{% highlight jproperties %}
# Max number of outstanding messages being processed per task at a time, applicable to both StreamTask and AsyncStreamTask.
task.max.concurrency=4
{% endhighlight %}

**NOTE:** In case of AsyncStreamTask, processAsync() is still invoked in the order of the message arrivals, but the completion can go out of order. In case of StreamTask with multithreading, process() can run out-of-order since they are dispatched to a thread pool. This option should **NOT** be used when strict ordering of the output is required.

### Guaranteed Semantics

In any of the scenarios, Samza guarantees the following semantics:

* Samza is thead-safe. You can safely access your job’s state in key-value store, write messages and checkpoint offset in the task threads. If you have other data shared among tasks, such as global variables or static data, it is not thread safe if the data can be accessed concurrently by multiple threads, e.g. StreamTask running in the configured thread pool with more than one threads. For states within a task, such as member variables, Samza guarantees the mutual exclusiveness of process, window and commit so there will be no concurrent modifications among these operations and any state change from one operation will be fully visible to the others.
* WindowableTask.window is called when no outstanding process/processAsync and no new process/processAsync invocations can be scheduled until it completes. The Samza engine is responsible for ensuring that window is invoked in a timely manner.
* Checkpointing is guaranteed to only cover events that are fully processed. It is persisted in commit() method.