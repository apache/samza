/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.serializers;

import java.util.Arrays;
import org.apache.samza.SamzaException;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.MessageType;
import org.apache.samza.system.WatermarkMessage;
import org.codehaus.jackson.type.TypeReference;


/**
 * This class provides serialization/deserialization of the intermediate messages.
 *
 * The message format of an intermediate stream is below:
 *
 * IntermediateStreamMessage: {
 *   MessageType : int8
 *   MessageData : byte[]
 * }
 *
 * MessageType: [0(UserMessage), 1(Watermark), 2(EndOfStream)]
 * MessageData: [UserMessage/ControlMessage]
 * ControlMessage:
 *   Version   : int
 *   TaskName  : string
 *   TaskCount : int
 *   Other Message Data (based on different types of control message)
 *
 * For user message, we use the user message serde.
 * For control message, we use json serde.
 */
public class IntermediateMessageSerde implements Serde<Object> {

  private static final class WatermarkSerde extends JsonSerde<WatermarkMessage> {
    @Override
    public WatermarkMessage fromBytes(byte[] bytes) {
      try {
        return mapper().readValue(new String(bytes, "UTF-8"), new TypeReference<WatermarkMessage>() { });
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }
  }

  private static final class EndOfStreamSerde extends JsonSerde<EndOfStreamMessage> {
    @Override
    public EndOfStreamMessage fromBytes(byte[] bytes) {
      try {
        return mapper().readValue(new String(bytes, "UTF-8"), new TypeReference<EndOfStreamMessage>() { });
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }
  }

  private final Serde userMessageSerde;
  private final Serde<WatermarkMessage> watermarkSerde;
  private final Serde<EndOfStreamMessage> eosSerde;

  public IntermediateMessageSerde(Serde userMessageSerde) {
    this.userMessageSerde = userMessageSerde;
    this.watermarkSerde = new WatermarkSerde();
    this.eosSerde = new EndOfStreamSerde();
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    try {
      final Object object;
      final MessageType type = MessageType.values()[bytes[0]];
      final byte [] data = Arrays.copyOfRange(bytes, 1, bytes.length);
      switch (type) {
        case USER_MESSAGE:
          object = userMessageSerde.fromBytes(data);
          break;
        case WATERMARK:
          object = watermarkSerde.fromBytes(data);
          break;
        case END_OF_STREAM:
          object = eosSerde.fromBytes(data);
          break;
        default:
          throw new UnsupportedOperationException(String.format("Message type %s is not supported", type.name()));
      }
      return object;

    } catch (UnsupportedOperationException ue) {
      throw new SamzaException(ue);
    } catch (Exception e) {
      // This will catch the following exceptions:
      // 1) the first byte is not a valid type so it will cause ArrayOutOfBound exception
      // 2) the first byte happens to be a valid type, but the deserialization fails with certain exception
      // For these cases, we fall back to user-provided serde
      return userMessageSerde.fromBytes(bytes);
    }
  }

  @Override
  public byte[] toBytes(Object object) {
    final byte [] data;
    final MessageType type = MessageType.of(object);
    switch (type) {
      case USER_MESSAGE:
        data = userMessageSerde.toBytes(object);
        break;
      case WATERMARK:
        data = watermarkSerde.toBytes((WatermarkMessage) object);
        break;
      case END_OF_STREAM:
        data = eosSerde.toBytes((EndOfStreamMessage) object);
        break;
      default:
        throw new SamzaException("Unknown message type: " + type.name());
    }

    final byte [] bytes = new byte[data.length + 1];
    bytes[0] = (byte) type.ordinal();
    System.arraycopy(data, 0, bytes, 1, data.length);

    return bytes;
  }
}
