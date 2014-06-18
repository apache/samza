---
layout: page
title: Windowing
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

Sometimes a stream processing job needs to do something in regular time intervals, regardless of how many incoming messages the job is processing. For example, say you want to report the number of page views per minute. To do this, you increment a counter every time you see a page view event. Once per minute, you send the current counter value to an output stream and reset the counter to zero.

Samza's *windowing* feature provides a way for tasks to do something in regular time intervals, for example once per minute. To enable windowing, you just need to set one property in your job configuration:

{% highlight jproperties %}
# Call the window() method every 60 seconds
task.window.ms=60000
{% endhighlight %}

Next, your stream task needs to implement the [WindowableTask](../api/javadocs/org/apache/samza/task/WindowableTask.html) interface. This interface defines a window() method which is called by Samza in the regular interval that you configured.

For example, this is how you would implement a basic per-minute event counter:

{% highlight java %}
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
{% endhighlight %}

If you need to send messages to output streams, you can use the [MessageCollector](../api/javadocs/org/apache/samza/task/MessageCollector.html) object passed to the window() method. Please only use that MessageCollector object for sending messages, and don't use it outside of the call to window().

Note that Samza uses [single-threaded execution](event-loop.html), so the window() call can never happen concurrently with a process() call. This has the advantage that you don't need to worry about thread safety in your code (no need to synchronize anything), but the downside that the window() call may be delayed if your process() method takes a long time to return.

## [Event Loop &raquo;](event-loop.html)
