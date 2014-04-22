---
layout: page
title: Streams
---

The [TaskRunner](task-runner.html) reads and writes messages using the SystemConsumer and SystemProducer interfaces.

```
public interface SystemConsumer {

  void start();

  void stop();

  void register(SystemStreamPartition systemStreamPartition, String lastReadOffset);

  List<IncomingMessageEnvelope> poll(Map<SystemStreamPartition, Integer> systemStreamPartitions, long timeout) throws InterruptedException;
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
```

Out of the box, Samza supports reads and writes to Kafka (i.e. it has a KafkaSystemConsumer/KafkaSystemProducer), but the interfaces are pluggable, and most message bus systems can be plugged in, with some degree of support.

A number of stream-related properties should be defined in your Samza job's configuration file. These properties define systems that Samza can read from, the streams on these systems, and how to serialize and deserialize the messages from the streams. For example, you might wish to read PageViewEvent from a specific Kafka cluster. The system properties in the configuration file would define how to connect to the Kafka cluster. The stream section would define PageViewEvent as an input stream. The serializer in the configuration would define the serde to use to decode PageViewEvent messages.

When the TaskRunner starts up, it will use the stream-related properties in your configuration to instantiate consumers for each stream partition. For example, if your input stream is PageViewEvent, which has 12 partitions, then the TaskRunner would create 12 KafkaSystemConsumers. Each consumer will read ByteBuffers from one partition, deserialize the ByteBuffer to an object, and put them into a queue. This queue is what the [event loop](event-loop.html) will use to feed messages to your StreamTask instances.

In the process method in StreamTask, there is a MessageCollector parameter given to use. When the TaskRunner calls process() on one of your StreamTask instances, it provides the collector. After the process() method completes, the TaskRunner takes any output messages that your StreamTask wrote to the collector, serializes the messages, and calls the send() method on the appropriate SystemProducer.

### Message Ordering

If a job is consuming messages from more than one system/stream/partition combination, by default, messages will be processed in a round robin fashion. For example, if a job is reading partitions 1 and 2 of page-view-events from a Kafka system, and there are messages available to be processed from both partitions, your StreamTask will get messages in round robin order (partition 1, partition 2, partition 1, partition 2, etc). If a message is not available for a given partition, it will be skipped, until a message becomes available.

#### MessageChooser

The default round robin behavior can be overridden by implementing a custom MessageChooser. A MessageChooser's job is to answer the question, "Given a set of incoming messages, which one should a Samza container process next?".  To write a custom MessageChooser, take a look at the [Javadocs](../api/javadocs/org/apache/samza/system/MessageChooser.html), and then configure your task with the "task.chooser.class" configuration, which should point to your MessageChooserFactory.

Out of the box, Samza ships with a RoundRobinChooser, which is the default. You can use the StreamChooser by adding the following configuration to your job.

```
task.chooser.class=org.apache.samza.system.YourStreamChooserFactory
```

#### Prioritizing

There are certain times when messages from a stream should be favored over messages from any other stream. For example, some Samza jobs consume two streams: one stream is fed by a real-time system and the other stream is fed by a batch system. A typical pattern is to have a Samza processor with a statistical model that is ranking a real-time feed of data. Periodically, this model needs to be retrained and updated. The Samza processor can be re-deployed with the new model, but how do you re-process all of the old data that the processor has already seen? This can be accomplished by having a batch system send messages to the Samza processor for any data that needs to be re-processed. In this example, you'd like to favor the real-time system over the batch system, when messages are available for the real-time system. This prevents latency from being introduced into the real-time feed even when the batch system is sending messages by always processing the real-time messages first.

Samza provides a mechanism to prioritize one stream over another by setting this value: systems.&lt;system&gt;.streams.&lt;stream&gt;.samza.priority=2. A config snippet illustrates the settings:

```
systems.kafka.streams.my-stream.samza.priority=2
systems.kafka.streams.my-other-stream.samza.priority=1
```

This declares that my-stream's messages will be processed before my-other-stream's. If my-stream has no messages available at the moment (because more are still being read in, for instance), then my-other-stream's messages will get processed.

Each priority level gets its own MessageChooser. In the example above, one MessageChooser is used for my-stream, and another is used for my-other-stream. The MessageChooser for my-other-stream will only be used when my-stream's MessageChooser doesn't return a message to process. 

It is also valid to define two streams with the same priority. If messages are available from two streams at the same priority level, it's up to the MessageChooser for that priority level to decide which message should be processed first.

It's also valid to only define priorities for some streams. All non-prioritized streams will be treated as the lowest priority, and will share a single MessageChooser. If you had my-third-stream, as a third input stream in the example above, it would be prioritized as the lowest stream, and also get its own MessageChooser.

#### Bootstrapping

Some Samza jobs wish to fully consume a stream from offset 0 all the way through to the last message in the stream before they process messages from any other stream. This is useful for streams that have some key-value data that a Samza job wishes to use when processing messages from another stream. This is 

Consider a case where you want to read a currency-code stream, which has mappings of country code (e.g. USD) to symbols (e.g. $), and is partitioned by country code. You might want to join these symbols to a stream called transactions which is also partitioned by currency, and has a schema like {"country": "USD", "amount": 1234}. You could then have your StreamTask join the currency symbol to each transaction message, and emit messages like {"amount": "$1234"}.

To bootstrap the currency-code stream, you need to read it from offset 0 all the way to the last message in the stream (what I'm calling head). It is not desirable to read any message from the transactions stream until the currency-code stream has been fully read, or else you might try to join a transaction message to a country code that hasn't yet been read.

Samza supports this style of processing with the systems.&lt;system&gt;.streams.&lt;stream&gt;.samza.bootstrap property.

```
systems.kafka.streams.currency-code.samza.bootstrap=true
```

This configuration tells Samza that currency-code's messages should be read from the last checkpointed offset all the way until the stream is caught up to "head", before any other message is processed. If you wish to process all messages in currency-code from offset 0 to head, you can define:

```
systems.kafka.streams.currency-code.samza.bootstrap=true
systems.kafka.streams.currency-code.samza.reset.offset=true
```

This tells Samza to start from beginning of the currency-code stream, and read all the way to head.

The difference between prioritizing a stream and bootstrapping a stream, is a high priority stream will still allow lower priority stream messages to be processed when no messages are available for the high priority stream. In the case of bootstrapping, no streams will be allowed to be processed until all messages in the bootstrap stream have been read up to the last message.

Once a bootstrap stream has been fully consumed ("caught up"), it is treated like a normal stream, and no bootstrapping logic happens.

It is valid to define multiple bootstrap streams.

```
systems.kafka.streams.currency-code.samza.bootstrap=true
systems.kafka.streams.other-bootstrap-stream.samza.bootstrap=true
```

In this case, currency-code and other-bootstrap-stream will both be processed before any other stream is processed. The order of message processing (the bootstrap order) between currency-code and other-bootstrap-stream is up to the MessageChooser. If you want to fully process one bootstrap stream before another, you can use priorities:

```
systems.kafka.streams.currency-code.samza.bootstrap=true
systems.kafka.streams.currency-code.samza.priority=2
systems.kafka.streams.other-bootstrap-stream.samza.bootstrap=true
systems.kafka.streams.other-bootstrap-stream.samza.priority=1
```

This defines a specific bootstrap ordering: fully bootstrap currency-code before bootstrapping other-bootstrap-stream.

Lastly, bootstrap and non-bootstrap prioritized streams can be mixed:

```
systems.kafka.streams.currency-code.samza.bootstrap=true
systems.kafka.streams.non-bootstrap-stream.samza.priority=2
systems.kafka.streams.other-non-bootstrap-stream.samza.priority=1
```

Bootstrap streams are assigned a priority of Int.MaxInt by default, so they will always be prioritized over any other prioritized stream. In this case, currency-code will be fully bootstrapped, and then treated as the highest priority stream (Int.IntMax). The next highest priority stream will be non-bootstrap-stream (priority 2), followed by other-non-bootstrap-stream (priority 1), and then any non-bootstrap/non-prioritized streams.

#### Batching

There are cases where consuming from the same SystemStreamPartition repeatedly leads to better performance. Samza allows for consumer batching to satisfy this use case. For example, if you had two SystemStreamPartitions, SSP1 and SSP2, you might wish to read 100 messages from SSP1 and then one from SSP2, regardless of the MessageChooser that's used. This can be accomplished with:

```
task.consumer.batch.size=100
```

With this setting, Samza will always try and read a message from the last SystemStreamPartition that was read. This behavior will continue until no message is available for the SystemStreamPartition, or the batch size has been reached. In either of these cases, Samza will defer to the MessageChooser to determine the next message to process. It will then try and stick to the new message's SystemStreamPartition again.

## [Checkpointing &raquo;](checkpointing.html)
