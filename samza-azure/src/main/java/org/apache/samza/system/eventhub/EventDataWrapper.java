package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventData;

/**
 * Simpler wrapper of {@link EventData} events with the decrypted payload
 */
public class EventDataWrapper {
  private final EventData _eventData;
  private final byte[] _body;

  public EventDataWrapper(EventData eventData, byte[] body) {
    _eventData = eventData;
    _body = body;
  }

  public EventData getEventData() {
    return _eventData;
  }

  /**
   * @return the body of decrypted body of the message. In case not encryption is setup for this topic
   * just returns the body of the message.
   */
  public byte[] getDecryptedBody() {
    return _body;
  }

  @Override
  public String toString() {
    return "EventDataWrapper: body: " + (new String(_body)) + ",  EventData " + _eventData;
  }

}
