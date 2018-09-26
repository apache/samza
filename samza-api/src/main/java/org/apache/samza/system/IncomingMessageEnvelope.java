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

package org.apache.samza.system;

import java.nio.charset.Charset;

/**
 * This class represents a message envelope that is received by a StreamTask for each message that is received from a
 * partition of a specific input stream.
 */
public class IncomingMessageEnvelope {

  //The offset starting with a NUL byte encoded are reserved for end-of-stream.
  private static final byte[] END_OF_STREAM_BYTES = "\0END_OF_STREAM".getBytes();
  public static final String END_OF_STREAM_OFFSET = new String(END_OF_STREAM_BYTES, Charset.defaultCharset());

  private final SystemStreamPartition systemStreamPartition;
  private final String offset;
  private final Object key;
  private final Object message;
  private final int size;
  private long timestamp = 0L;

  /**
   * Constructs a new IncomingMessageEnvelope from specified components.
   * @param systemStreamPartition The aggregate object representing the incoming stream name, the name of the cluster
   * from which the stream came, and the partition of the stream from which the message was received.
   * @param offset The offset in the partition that the message was received from.
   * @param key A deserialized key received from the partition offset.
   * @param message A deserialized message received from the partition offset.
   */
  public IncomingMessageEnvelope(SystemStreamPartition systemStreamPartition, String offset, Object key, Object message) {
    this(systemStreamPartition, offset, key, message, 0);
  }

  /**
   * Constructs a new IncomingMessageEnvelope from specified components.
   * @param systemStreamPartition The aggregate object representing the incoming stream name, the name of the cluster
   * from which the stream came, and the partition of the stream from which the message was received.
   * @param offset The offset in the partition that the message was received from.
   * @param key A deserialized key received from the partition offset.
   * @param message A deserialized message received from the partition offset.
   * @param size size of the message and key in bytes.
   */
  public IncomingMessageEnvelope(SystemStreamPartition systemStreamPartition, String offset,
      Object key, Object message, int size) {
    this.systemStreamPartition = systemStreamPartition;
    this.offset = offset;
    this.key = key;
    this.message = message;
    this.size = size;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public SystemStreamPartition getSystemStreamPartition() {
    return systemStreamPartition;
  }

  public String getOffset() {
    return offset;
  }

  public Object getKey() {
    return key;
  }

  public Object getMessage() {
    return message;
  }

  public int getSize() {
    return size;
  }

  public boolean isEndOfStream() {
    return END_OF_STREAM_OFFSET.equals(offset);
  }

  /**
   * This method is deprecated in favor of WatermarkManager.buildEndOfStreamEnvelope(SystemStreamPartition ssp).
   *
   * @param ssp The SSP that is at end-of-stream.
   * @return an IncomingMessageEnvelope corresponding to end-of-stream for that SSP.
   */
  public static IncomingMessageEnvelope buildEndOfStreamEnvelope(SystemStreamPartition ssp) {
    return new IncomingMessageEnvelope(ssp, END_OF_STREAM_OFFSET, null, new EndOfStreamMessage(null));
  }

  public static IncomingMessageEnvelope buildWatermarkEnvelope(SystemStreamPartition ssp, long watermark) {
    return new IncomingMessageEnvelope(ssp, null, null, new WatermarkMessage(watermark, null));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((message == null) ? 0 : message.hashCode());
    result = prime * result + ((offset == null) ? 0 : offset.hashCode());
    result = prime * result + ((systemStreamPartition == null) ? 0 : systemStreamPartition.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IncomingMessageEnvelope other = (IncomingMessageEnvelope) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (message == null) {
      if (other.message != null)
        return false;
    } else if (!message.equals(other.message))
      return false;
    if (offset == null) {
      if (other.offset != null)
        return false;
    } else if (!offset.equals(other.offset))
      return false;
    if (systemStreamPartition == null) {
      if (other.systemStreamPartition != null)
        return false;
    } else if (!systemStreamPartition.equals(other.systemStreamPartition))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "IncomingMessageEnvelope [systemStreamPartition=" + systemStreamPartition + ", offset=" + offset + ", key=" + key + ", message=" + message + "]";
  }
}
