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

package org.apache.samza.serializers.model.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.message.WatermarkMessage;
import org.apache.samza.message.MessageType;
import org.apache.samza.serializers.IntermediateMessageSerde;
import org.apache.samza.serializers.Serde;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestIntermediateMessageSerde {

  static final class ObjectSerde implements Serde<Object> {

    @Override
    public Object fromBytes(byte[] bytes) {
      try {
        final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
        Object object = ois.readObject();
        ois.close();
        return object;
      } catch (Exception e) {
        return null;
      }
    }

    @Override
    public byte[] toBytes(Object object) {
      try {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.close();
        return baos.toByteArray();
      } catch (Exception e) {
        return null;
      }
    }
  }

  static final class TestUserMessage implements Serializable {
    private final String message;
    private final long timestamp;
    private final int offset;

    public TestUserMessage(String message, int offset, long timestamp) {
      this.message = message;
      this.offset = offset;
      this.timestamp = timestamp;
    }

    public String getMessage() {
      return message;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public int getOffset() {
      return offset;
    }
  }

  @Test
  public void testUserMessageSerde() {
    IntermediateMessageSerde imserde= new IntermediateMessageSerde(new ObjectSerde());
    String msg = "this is a test message";
    TestUserMessage userMessage = new TestUserMessage(msg, 0, System.currentTimeMillis());
    byte[] bytes = imserde.toBytes(userMessage);
    TestUserMessage de = (TestUserMessage) imserde.fromBytes(bytes);
    assertEquals(MessageType.of(de), MessageType.USER_MESSAGE);
    assertEquals(de.getMessage(), msg);
    assertEquals(de.getOffset(), 0);
    assertTrue(de.getTimestamp() > 0);
  }

  @Test
  public void testWatermarkMessageSerde() {
    IntermediateMessageSerde imserde= new IntermediateMessageSerde(new ObjectSerde());
    String taskName = "task-1";
    WatermarkMessage watermark = new WatermarkMessage(System.currentTimeMillis(),taskName, 8);
    byte[] bytes = imserde.toBytes(watermark);
    WatermarkMessage de = (WatermarkMessage) imserde.fromBytes(bytes);
    assertEquals(MessageType.of(de), MessageType.WATERMARK);
    assertEquals(de.getTaskName(), taskName);
    assertEquals(de.getTaskCount(), 8);
    assertTrue(de.getTimestamp() > 0);
  }

  @Test
  public void testEndOfStreamMessageSerde() {
    IntermediateMessageSerde imserde= new IntermediateMessageSerde(new ObjectSerde());
    String streamId = "test-stream";
    String taskName = "task-1";
    EndOfStreamMessage eos = new EndOfStreamMessage(streamId, taskName, 8);
    byte[] bytes = imserde.toBytes(eos);
    EndOfStreamMessage de = (EndOfStreamMessage) imserde.fromBytes(bytes);
    assertEquals(MessageType.of(de), MessageType.END_OF_STREAM);
    assertEquals(de.getStreamId(), streamId);
    assertEquals(de.getTaskName(), taskName);
    assertEquals(de.getTaskCount(), 8);
    assertEquals(de.getVersion(), 1);
  }
}
