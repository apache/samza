---
layout: page
title: Event Loop
---

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

### Lifecycle Listeners

Sometimes, you need to run your own code at specific points in a task's lifecycle. For example, you might want to set up some context in the container whenever a new message arrives, or perform some operations on startup or shutdown.

To receive notifications when such events happen, you can implement the [TaskLifecycleListenerFactory](../api/javadocs/org/apache/samza/task/TaskLifecycleListenerFactory.html) interface. It returns a [TaskLifecycleListener](../api/javadocs/org/apache/samza/task/TaskLifecycleListener.html), whose methods are called by Samza at the appropriate times.

You can then tell Samza to use your lifecycle listener with the following properties in your job configuration:

    # Define a listener called "my-listener" by giving the factory class name
    task.lifecycle.listener.my-listener.class=com.example.foo.MyListenerFactory

    # Enable it in this job (multiple listeners can be separated by commas)
    task.lifecycle.listeners=my-listener

The Samza container creates one instance of your [TaskLifecycleListener](../api/javadocs/org/apache/samza/task/TaskLifecycleListener.html). If the container has multiple task instances (processing different input stream partitions), the beforeInit, afterInit, beforeClose and afterClose methods are called for each task instance. The [TaskContext](../api/javadocs/org/apache/samza/task/TaskContext.html) argument of those methods gives you more information about the partitions.

## [JMX &raquo;](jmx.html)
