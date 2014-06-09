---
layout: page
title: Streams
---

The [samza container](samza-container.html) reads and writes messages using the [SystemConsumer](../api/javadocs/org/apache/samza/system/SystemConsumer.html) and [SystemProducer](../api/javadocs/org/apache/samza/system/SystemProducer.html) interfaces. You can integrate any message broker with Samza by implementing these two interfaces.

    public interface SystemConsumer {
      void start();

      void stop();

      void register(
          SystemStreamPartition systemStreamPartition,
          String lastReadOffset);

      List<IncomingMessageEnvelope> poll(
          Map<SystemStreamPartition, Integer> systemStreamPartitions,
          long timeout)
        throws InterruptedException;
    }

    public class IncomingMessageEnvelope {
      public Object getMessage() { ... }

      public Object getKey() { ... }

      public SystemStreamPartition getSystemStreamPartition() { ... }
    }

    public interface SystemProducer {
      void start();

      void stop();

      void register(String source);

      void send(String source, OutgoingMessageEnvelope envelope);

      void flush(String source);
    }

    public class OutgoingMessageEnvelope {
      ...
      public Object getKey() { ... }

      public Object getMessage() { ... }
    }

Out of the box, Samza supports Kafka (KafkaSystemConsumer and KafkaSystemProducer). However, any message bus system can be plugged in, as long as it can provide the semantics required by Samza, as described in the [javadoc](../api/javadocs/org/apache/samza/system/SystemConsumer.html).

SystemConsumers and SystemProducers may read and write messages of any data type. It's ok if they only support byte arrays &mdash; Samza has a separate [serialization layer](serialization.html) which converts to and from objects that application code can use. Samza does not prescribe any particular data model or serialization format.

The job configuration file can include properties that are specific to a particular consumer and producer implementation. For example, the configuration would typically indicate the hostname and port of the message broker to use, and perhaps connection options.

### How streams are processed

If a job is consuming messages from more than one input stream, and all input streams have messages available, messages are processed in a round robin fashion by default. For example, if a job is consuming AdImpressionEvent and AdClickEvent, the task instance's process() method is called with a message from AdImpressionEvent, then a message from AdClickEvent, then another message from AdImpressionEvent, ... and continues to alternate between the two.

If one of the input streams has no new messages available (the most recent message has already been consumed), that stream is skipped, and the job continues to consume from the other inputs. It continues to check for new messages becoming available.

#### MessageChooser

When a Samza container has several incoming messages on different stream partitions, how does it decide which to process first? The behavior is determined by a [MessageChooser](../api/javadocs/org/apache/samza/system/chooser/MessageChooser.html). The default chooser is RoundRobinChooser, but you can override it by implementing a custom chooser.

To plug in your own message chooser, you need to implement the [MessageChooserFactory](../api/javadocs/org/apache/samza/system/chooser/MessageChooserFactory.html) interface, and set the "task.chooser.class" configuration to the fully-qualified class name of your implementation:

    task.chooser.class=com.example.samza.YourMessageChooserFactory

#### Prioritizing input streams

There are certain times when messages from one stream should be processed with higher priority than messages from another stream. For example, some Samza jobs consume two streams: one stream is fed by a real-time system and the other stream is fed by a batch system. In this case, it's useful to prioritize the real-time stream over the batch stream, so that the real-time processing doesn't slow down if there is a sudden burst of data on the batch stream.

Samza provides a mechanism to prioritize one stream over another by setting this configuration parameter: systems.&lt;system&gt;.streams.&lt;stream&gt;.samza.priority=&lt;number&gt;. For example:

    systems.kafka.streams.my-real-time-stream.samza.priority=2
    systems.kafka.streams.my-batch-stream.samza.priority=1

This declares that my-real-time-stream's messages should be processed with higher priority than my-batch-stream's messages. If my-real-time-stream has any messages available, they are processed first. Only if there are no messages currently waiting on my-real-time-stream, the Samza job continues processing my-batch-stream.

Each priority level gets its own MessageChooser. It is valid to define two streams with the same priority. If messages are available from two streams at the same priority level, it's up to the MessageChooser for that priority level to decide which message should be processed first.

It's also valid to only define priorities for some streams. All non-prioritized streams are treated as the lowest priority, and share a MessageChooser.

#### Bootstrapping

Sometimes, a Samza job needs to fully consume a stream (from offset 0 up to the most recent message) before it processes messages from any other stream. This is useful in situations where the stream contains some prerequisite data that the job needs, and it doesn't make sense to process messages from other streams until the job has loaded that prerequisite data. Samza supports this use case with *bootstrap streams*.

A bootstrap stream seems similar to a stream with a high priority, but is subtly different. Before allowing any other stream to be processed, a bootstrap stream waits for the consumer to explicitly confirm that the stream has been fully consumed. Until then, the bootstrap stream is the exclusive input to the job: even if a network issue or some other factor causes the bootstrap stream consumer to slow down, other inputs can't sneak their messages in.

Another difference between a bootstrap stream and a high-priority stream is that the bootstrap stream's special treatment is temporary: when it has been fully consumed (we say it has "caught up"), its priority drops to be the same as all the other input streams.

To configure a stream called "my-bootstrap-stream" to be a fully-consumed bootstrap stream, use the following settings:

    systems.kafka.streams.my-bootstrap-stream.samza.bootstrap=true
    systems.kafka.streams.my-bootstrap-stream.samza.reset.offset=true
    systems.kafka.streams.my-bootstrap-stream.samza.offset.default=oldest

The bootstrap=true parameter enables the bootstrap behavior (prioritization over other streams). The combination of reset.offset=true and offset.default=oldest tells Samza to always start reading the stream from the oldest offset, every time a container starts up (rather than starting to read from the most recent checkpoint).

It is valid to define multiple bootstrap streams. In this case, the order in which they are bootstrapped is determined by the priority.

#### Batching

In some cases, you can improve performance by consuming several messages from the same stream partition in sequence. Samza supports this mode of operation, called *batching*.

For example, if you want to read 100 messages in a row from each stream partition (regardless of the MessageChooser), you can use this configuration parameter:

    task.consumer.batch.size=100

With this setting, Samza tries to read a message from the most recently used [SystemStreamPartition](../api/javadocs/org/apache/samza/system/SystemStreamPartition.html). This behavior continues either until no more messages are available for that SystemStreamPartition, or until the batch size has been reached. When that happens, Samza defers to the MessageChooser to determine the next message to process. It then again tries to continue consume from the chosen message's SystemStreamPartition until the batch size is reached.

## [Serialization &raquo;](serialization.html)
