---
layout: page
title: Metrics
---

When you're running a stream process in production, it's important that you have good metrics to track the health of your job. In order to make this easy, Samza includes a metrics library. It is used by Samza itself to generate some standard metrics such as message throughput, but you can also use it in your task code to emit custom metrics.

Metrics can be reported in various ways. You can expose them via [JMX](jmx.html), which is useful in development. In production, a common setup is for each Samza container to periodically publish its metrics to a "metrics" Kafka topic, in which the metrics from all Samza jobs are aggregated. You can then consume this stream in another Samza job, and send the metrics to your favorite graphing system such as [Graphite](http://graphite.wikidot.com/).

To set up your job to publish metrics to Kafka, you can use the following configuration:

    # Define a metrics reporter called "snapshot", which publishes metrics
    # every 60 seconds.
    metrics.reporters=snapshot
    metrics.reporter.snapshot.class=org.apache.samza.metrics.reporter.MetricsSnapshotReporterFactory

    # Tell the snapshot reporter to publish to a topic called "metrics"
    # in the "kafka" system.
    metrics.reporter.snapshot.stream=kafka.metrics

    # Encode metrics data as JSON.
    serializers.registry.metrics.class=org.apache.samza.serializers.MetricsSnapshotSerdeFactory
    systems.kafka.streams.metrics.samza.msg.serde=metrics

With this configuration, the job automatically sends several JSON-encoded messages to the "metrics" topic in Kafka every 60 seconds. The messages look something like this:

    {
      "header": {
        "container-name": "samza-container-0",
        "host": "samza-grid-1234.example.com",
        "job-id": "1",
        "job-name": "my-samza-job",
        "reset-time": 1401729000347,
        "samza-version": "0.0.1",
        "source": "Partition-2",
        "time": 1401729420566,
        "version": "0.0.1"
      },
      "metrics": {
        "org.apache.samza.container.TaskInstanceMetrics": {
          "commit-calls": 7,
          "commit-skipped": 77948,
          "kafka-input-topic-offset": "1606",
          "messages-sent": 985,
          "process-calls": 1093,
          "send-calls": 985,
          "send-skipped": 76970,
          "window-calls": 0,
          "window-skipped": 77955
        }
      }
    }

There is a separate message for each task instance, and the header tells you the job name, job ID and partition of the task. The metrics allow you to see how many messages have been processed and sent, the current offset in the input stream partition, and other details. There are additional messages which give you metrics about the JVM (heap size, garbage collection information, threads etc.), internal metrics of the Kafka producers and consumers, and more.

It's easy to generate custom metrics in your job, if there's some value you want to keep an eye on. You can use Samza's built-in metrics framework, which is similar in design to Coda Hale's [metrics](http://metrics.codahale.com/) library. 

You can register your custom metrics through a [MetricsRegistry](../api/javadocs/org/apache/samza/metrics/MetricsRegistry.html). Your stream task needs to implement [InitableTask](../api/javadocs/org/apache/samza/task/InitableTask.html), so that you can get the metrics registry from the [TaskContext](../api/javadocs/org/apache/samza/task/TaskContext.html). This simple example shows how to count the number of messages processed by your task:

    public class MyJavaStreamTask implements StreamTask, InitableTask {
      private Counter messageCount;

      public void init(Config config, TaskContext context) {
        this.messageCount = context
          .getMetricsRegistry()
          .newCounter(getClass().getName(), "message-count");
      }

      public void process(IncomingMessageEnvelope envelope,
                          MessageCollector collector,
                          TaskCoordinator coordinator) {
        messageCount.inc();
      }
    }

Samza currently supports two kind of metrics: [counters](../api/javadocs/org/apache/samza/metrics/Counter.html) and [gauges](../api/javadocs/org/apache/samza/metrics/Gauge.html). Use a counter when you want to track how often something occurs, and a gauge when you want to report the level of something, such as the size of a buffer. Each task instance (for each input stream partition) gets its own set of metrics.

If you want to report metrics in some other way, e.g. directly to a graphing system (without going via Kafka), you can implement a [MetricsReporterFactory](../api/javadocs/org/apache/samza/metrics/MetricsReporterFactory.html) and reference it in your job configuration.

## [Windowing &raquo;](windowing.html)
