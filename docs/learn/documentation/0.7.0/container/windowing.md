---
layout: page
title: Windowing
---

Referring back to the "count PageViewEvent by user ID" example in the [Architecture](../introduction/architecture.html) section, one thing that we left out was what we do with the counts. Let's say that the Samza job wants to update the user ID counts in a database once every minute. Here's how it would work. The Samza job that does the counting would keep a Map&lt;Integer, Integer&gt; in memory, which maps user IDs to page view counts. Every time a message arrives, the job would take the user ID in the PageViewEvent, and use it to increment the user ID's count in the in-memory map. Then, once a minute, the StreamTask would update the database (total_count += current_count) for every user ID in the map, and then reset the count map.

Windowing is how we achieve this. If a StreamTask implements the WindowableTask interface, the TaskRunner will call the window() method on the task over a configured interval.

```
public interface WindowableTask {
  void window(MessageCollector collector, TaskCoordinator coordinator);
}
```

If you choose to implement the WindowableTask interface, you can use the Samza job's configuration to define how often the TaskRunner should call your window() method. In the PageViewEvent example (above), you would define it to flush every 60000 milliseconds (60 seconds).

## [Event Loop &raquo;](event-loop.html)
