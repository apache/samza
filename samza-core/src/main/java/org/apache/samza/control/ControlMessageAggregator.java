package org.apache.samza.control;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.message.MessageType;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;


public class ControlMessageAggregator {

  interface ControlMessageManager {
    IncomingMessageEnvelope update(IncomingMessageEnvelope envelope);
  }

  private final Map<MessageType, ControlMessageManager> managers;

  public ControlMessageAggregator(Set<SystemStreamPartition> ssps, MessageCollector collector) {
    Map<MessageType, ControlMessageManager> managerMap = new HashMap<>();
    managerMap.put(MessageType.END_OF_STREAM, new EndOfStreamManager(ssps, collector));
    this.managers = Collections.unmodifiableMap(managerMap);
  }

  public IncomingMessageEnvelope aggregate(IncomingMessageEnvelope controlMessage) {
    MessageType type = MessageType.of(controlMessage.getMessage());
    return managers.get(type).update(controlMessage);
  }
}
