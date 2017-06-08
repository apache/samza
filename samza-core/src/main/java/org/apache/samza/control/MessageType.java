package org.apache.samza.control;

import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.message.WatermarkMessage;


/**
 * The type of the message inside {@link org.apache.samza.system.IncomingMessageEnvelope}
 */
public enum MessageType {
  DATA,
  WATERMARK,
  END_OF_STREAM;

  /**
   * Returns the type of the message inside {@link org.apache.samza.system.IncomingMessageEnvelope}
   * @param message a message object
   * @return type of the message
   */
  public static MessageType of(Object message) {
    if (message instanceof WatermarkMessage) {
      return WATERMARK;
    } else if (message instanceof EndOfStreamMessage) {
      return END_OF_STREAM;
    } else {
      return DATA;
    }
  }
}