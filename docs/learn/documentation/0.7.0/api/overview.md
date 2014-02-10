---
layout: page
title: API Overview
---

When writing a stream processor for Samza, you must implement the StreamTask interface:

```
/** User processing tasks implement this. */
public interface StreamTask {
  void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception;
}
```

When Samza runs your task, the process method will be called once for each message that Samza receives from your task's input streams. The envelope contains three things of importance: the message, the key, and the stream that the message came from:

```
/** This class is given to a StreamTask once for each message that it receives. */
public class IncomingMessageEnvelope {
  /** A deserialized message. */
  Object getMessage() { ... }

  /** A deserialized key. */
  Object getKey() { ... }

  /** The stream and partition that this message came from. */
  SystemStreamPartition getSystemStreamPartition() { ... }
}
```
<!-- TODO This description and example needs to be updated to match SystemStreamPartition. -->
Note that the getSystemStreamPartition() method returns a SystemStreamPartition object, not a String, as you might expect. This is because a Samza Stream actually consists of a name, a system, and a stream. The name is what you call the stream in your Samza configuration file. The system is the name of the cluster that the stream came from (e.g. kafka-aggreate-tracking, databus, etc). The system name is also defined in your Samza configuration file. Lastly, the actual stream is available. For Kafka, this would be the Kafka topic's name.

```
/** A name/system/stream tuple that represents a Samza stream. */
public class SystemStreamPartition extends SystemStream {

  /** The system name that this stream is associated with. This is also
      defined in a Samza job's configuration. */
  public String getSystem() { ... }

  /** The stream name for the system. */
  public String getStream() { ... }

  /** The partition within the stream. */
    public Partition getPartition() { ... }
}
```

To make this a bit clearer, let me show you an example. A Samza job's configuration might have:

```
# the stream
streams.page-view-event.stream=PageViewEvent
streams.page-view-event.system=kafka
streams.page-view-event.serde=json

# the system
systems.kafka.samza.partition.manager=samza.stream.kafka.KafkaPartitionManager
systems.kafka.samza.consumer.factory=samza.stream.kafka.KafkaConsumerFactory
systems.kafka.samza.producer.factory=samza.stream.kafka.KafkaProducerFactory
...
```

In this example, getName would return page-view-event, getSystem would return kafka, and getStream would return PageViewEvent. If you've got more than one input stream feeding into your StreamTask, you can use the getStream() object to determine what kind of message you've received.

What about sending messages? If you take a look at the process() method in StreamTask, you'll see that you get a MessageCollector.

```
/** When a task wishes to send a message, it uses this class. */
public interface MessageCollector {
  void send(OutgoingMessageEnvelope envelope);
}
```

The collector takes OutgoingMessageEnvelope, which allows tasks to supply a partition key when sending the message. The partition key, if supplied, is used to determine which partition of a stream a message is destined for.

```
/** A message envelope that has a key. */
public class OutgoingMessageEnvelope {
  ...
  Object getKey() { ... }
}
```

And, putting it all together:

<!-- TODO Verify that this example actually works. -->

```
class MyStreamerTask extends StreamTask {
  def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    val msg = envelope.getMessage.asInstanceOf[GenericRecord]
    collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "SomeTopicPartitionedByUserId"), msg.get("user_id"), msg))
  }
}
```

This is a simplistic example that just reads from a stream, and sends the messages to SomeTopicPartitionedByUserId, partitioned by the message's user ID.

## [TaskRunner &raquo;](../container/task-runner.html)
