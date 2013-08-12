---
layout: page
title: Metrics
---

Samza also provides a metrics library that the TaskRunner uses. It allows a StreamTask to create counters and gauges. The TaskRunner then writes those metrics to metrics infrastructure through a MetricsReporter implementation.

```
public class MyJavaStreamerTask implements StreamTask, InitableTask {
  private static final Counter messageCount;

  public void init(Config config, TaskContextPartition context) {
    this.messageCount = context.getMetricsRegistry().newCounter(MyJavaStreamerTask.class.toString(), "MessageCount");
  }

  @Override
  public void process(MessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    System.out.println(envelope.getMessage().toString());
    messageCount.inc();
  }
}
```

Samza's metrics design is very similar to Coda Hale's [metrics](https://github.com/codahale/metrics) library. It has two important interfaces:

```
public interface MetricsRegistry {
  Counter newCounter(String group, String name);

  <T> Gauge<T> newGauge(String group, String name, T value);
}

public interface MetricsReporter {
  void report(MessageCollector collector, ReadableMetricsRegistry registry, long currentTimeMillis, Partition partition);
}
```

### MetricsRegistry

When the TaskRunner starts up, as with StreamTask instantiation, it creates a MetricsRegistry for every partition in the Samza job.

![diagram](/img/0.7.0/learn/documentation/container/metrics.png)

The TaskRunner, itself, also gets a MetricsRegistry that it can use to create counters and gauges. It uses this registry to measure a lot of relevant metrics for itself.

### MetricsReporter

The other important interface is the MetricsReporter. The TaskRunner uses MetricsReporter implementations to send its MetricsRegistry counters and gauges to whatever metrics infrastructure the reporter uses. A Samza job's configuration determines which MetricsReporters the TaskRunner will use. Out of the box, Samza comes with a MetricsSnapshotReporter that sends JSON metrics messages to a Kafka topic.

## [Windowing &raquo;](windowing.html)
