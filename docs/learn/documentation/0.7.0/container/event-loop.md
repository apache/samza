---
layout: page
title: Event Loop
---

The event loop is the [TaskRunner](task-runner.html)'s single thread that is in charge of [reading](streams.html), [writing](streams.html), [metrics flushing](metrics.html), [checkpointing](checkpointing.html), and [windowing](windowing.html). It's the code that puts all of this stuff together. Each StreamConsumer reads messages on its own thread, but writes messages into a centralized message queue. The TaskRunner uses this queue to funnel all of the messages into the event loop. Here's how the event loop works:

1. Take a message from the incoming message queue (the queue that the StreamConsumers are putting their messages)
2. Give the message to the appropriate StreamTask by calling process() on it
3. Send any StreamTask output from the process() call to the appropriate StreamProducers
4. Call window() on the StreamTask if it implements WindowableTask, and the window time has expired
5. Send any StreamTask output from the window() call to the appropriate StreamProducers
6. Write checkpoints for any partitions that are past the defined checkpoint commit interval

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
  void beforeProcess(MessageEnvelope envelope, Config config, TaskContext context);

  /**
   * Called after a message is processed by a task.
   */
  void afterProcess(MessageEnvelope envelope, Config config, TaskContext context);

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

The TaskRunner will notify any lifecycle listeners whenever one of these events occurs. Usually, you don't really need to worry about lifecycle, but it's there if you need it.

## [JMX &raquo;](jmx.html)
