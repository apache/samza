---
layout: page
title: Windowing
---

Sometimes a stream processing job needs to do something in regular time intervals, regardless of how many incoming messages the job is processing. For example, say you want to report the number of page views per minute. To do this, you increment a counter every time you see a page view event. Once per minute, you send the current counter value to an output stream and reset the counter to zero.

Samza's *windowing* feature provides a way for tasks to do something in regular time intervals, for example once per minute. To enable windowing, you just need to set one property in your job configuration:

    # Call the window() method every 60 seconds
    task.window.ms=60000

Next, your stream task needs to implement the [WindowableTask](../api/javadocs/org/apache/samza/task/WindowableTask.html) interface. This interface defines a window() method which is called by Samza in the regular interval that you configured.

For example, this is how you would implement a basic per-minute event counter:

    public class EventCounterTask implements StreamTask, WindowableTask {

      public static final SystemStream OUTPUT_STREAM =
        new SystemStream("kafka", "events-per-minute");

      private int eventsSeen = 0;

      public void process(IncomingMessageEnvelope envelope,
                          MessageCollector collector,
                          TaskCoordinator coordinator) {
        eventsSeen++;
      }

      public void window(MessageCollector collector,
                         TaskCoordinator coordinator) {
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, eventsSeen));
        eventsSeen = 0;
      }
    }

If you need to send messages to output streams, you can use the [MessageCollector](../api/javadocs/org/apache/samza/task/MessageCollector.html) object passed to the window() method. Please only use that MessageCollector object for sending messages, and don't use it outside of the call to window().

Note that Samza uses [single-threaded execution](event-loop.html), so the window() call can never happen concurrently with a process() call. This has the advantage that you don't need to worry about thread safety in your code (no need to synchronize anything), but the downside that the window() call may be delayed if your process() method takes a long time to return.

## [Event Loop &raquo;](event-loop.html)
