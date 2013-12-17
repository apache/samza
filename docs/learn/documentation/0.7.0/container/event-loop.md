---
layout: page
title: Event Loop
---

The event loop is the [TaskRunner](task-runner.html)'s single thread that is in charge of [reading](streams.html), [writing](streams.html), [metrics flushing](metrics.html), [checkpointing](checkpointing.html), and [windowing](windowing.html). It's the code that puts all of this stuff together. Each SystemConsumer reads messages on its own thread, but writes messages into a centralized message queue. The TaskRunner uses this queue to funnel all of the messages into the event loop. Here's how the event loop works:

1. Take a message from the incoming message queue (the queue that the SystemConsumers are putting their messages)
2. Give the message to the appropriate StreamTask by calling process() on it
3. Call window() on the StreamTask if it implements WindowableTask, and the window time has expired
4. Send any StreamTask output from the process() and window() call to the appropriate SystemProducers
5. Write checkpoints for any partitions that are past the defined checkpoint commit interval

The TaskRunner does this, in a loop, until it is shutdown.

### Lifecycle Listeners

Sometimes, it's useful to receive notifications when a specific event happens in the TaskRunner. For example, you might want to reset some context in the container whenever a new message arrives. To accomplish this, Samza provides a TaskLifecycleListener interface, that can be wired into the TaskRunner through configuration.

```
/**
 * Used to get before/after notifications before initializing/closing all tasks
 * in a given container (JVM/process).
 */
public interface TaskLifecycleListener {
  /**
   * Called before all tasks in TaskRunner are initialized.
   */
  void beforeInit(Config config, TaskContext context);

  /**
   * Called after all tasks in TaskRunner are initialized.
   */
  void afterInit(Config config, TaskContext context);

  /**
   * Called before a message is processed by a task.
   */
  void beforeProcess(IncomingMessageEnvelope envelope, Config config, TaskContext context);

  /**
   * Called after a message is processed by a task.
   */
  void afterProcess(IncomingMessageEnvelope envelope, Config config, TaskContext context);

  /**
   * Called before all tasks in TaskRunner are closed.
   */
  void beforeClose(Config config, TaskContext context);

  /**
   * Called after all tasks in TaskRunner are closed.
   */
  void afterClose(Config config, TaskContext context);
}
```

To use a TaskLifecycleListener, you must also create a factory for the listener.

```
public interface TaskLifecycleListenerFactory {
  TaskLifecycleListener getLifecyleListener(String name, Config config);
}
```

#### Configuring Lifecycle Listeners

Once you have written a TaskLifecycleListener, and its factory, you can use the listener by configuring your Samza job with the following keys:

* task.lifecycle.listeners: A CSV list of all defined listeners that should be used for the Samza job.
* task.lifecycle.listener.&lt;listener name&gt;.class: A Java package and class name for a single listener factory.

For example, you might define a listener called "my-listener":

```
task.lifecycle.listener.my-listener.class=com.foo.bar.MyListenerFactory
```

And then enable it for your Samza job:

```
task.lifecycle.listeners=my-listener
```

Samza's container will create one instance of TaskLifecycleListener, and notify it whenever any of the events (shown in the API above) occur.

Borrowing from the example above, if we have a single Samza container processing partitions 0 and 2, and have defined a lifecycle listener called my-listener, then the Samza container will have a single instance of MyListener. MyListener's beforeInit, afterInit, beforeClose, and afterClose methods will all be called twice: one for each of the two partitions (e.g. beforeInit partition 0, before init partition 1, etc). The beforeProcess and afterProcess methods will simply be called once for each incoming message. The TaskContext is how the TaskLifecycleListener is able to tell which partition the event is for.

## [JMX &raquo;](jmx.html)
