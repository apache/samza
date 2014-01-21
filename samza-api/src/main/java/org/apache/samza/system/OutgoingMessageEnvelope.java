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

/**
 * This class represents a message envelope that is sent by a StreamTask. It can be thought of as a complement to the
 * IncomingMessageEnvelope class.
 */
public class OutgoingMessageEnvelope {
  private final SystemStream systemStream;
  private final String keySerializerName;
  private final String messageSerializerName;
  private final Object partitionKey;
  private final Object key;
  private final Object message;

  /**
   * Constructs a new OutgoingMessageEnvelope from specified components.
   * @param systemStream Object representing the appropriate stream of which this envelope will be sent on.
   * @param keySerializerName String representing the serializer used for serializing this envelope's key.
   * @param messageSerializerName String representing the serializer used for serializing this envelope's message.
   * @param partitionKey A key representing which partition of the systemStream to send this envelope on.
   * @param key A deserialized key to be used for the message.
   * @param message A deserialized message to be sent in this envelope.
   */
  public OutgoingMessageEnvelope(SystemStream systemStream, String keySerializerName, String messageSerializerName, Object partitionKey, Object key, Object message) {
    this.systemStream = systemStream;
    this.keySerializerName = keySerializerName;
    this.messageSerializerName = messageSerializerName;
    this.partitionKey = partitionKey;
    this.key = key;
    this.message = message;
  }

  /**
   * Constructs a new OutgoingMessageEnvelope from specified components.
   * @param systemStream Object representing the appropriate stream of which this envelope will be sent on.
   * @param partitionKey A key representing which partition of the systemStream to send this envelope on.
   * @param key A deserialized key to be used for the message.
   * @param message A deserialized message to be sent in this envelope.
   */
  public OutgoingMessageEnvelope(SystemStream systemStream, Object partitionKey, Object key, Object message) {
    this(systemStream, null, null, partitionKey, key, message);
  }

  /**
   * Constructs a new OutgoingMessageEnvelope from specified components.
   * @param systemStream Object representing the appropriate stream of which this envelope will be sent on.
   * @param key A deserialized key to be used for the message.
   * @param message A deserialized message to be sent in this envelope.
   */
  public OutgoingMessageEnvelope(SystemStream systemStream, Object key, Object message) {
    this(systemStream, null, null, key, key, message);
  }

  /**
   * Constructs a new OutgoingMessageEnvelope from specified components.
   * @param systemStream Object representing the appropriate stream of which this envelope will be sent on.
   * @param message A deserialized message to be sent in this envelope.
   */
  public OutgoingMessageEnvelope(SystemStream systemStream, Object message) {
    this(systemStream, null, null, null, null, message);
  }

  public SystemStream getSystemStream() {
    return systemStream;
  }

  public String getKeySerializerName() {
    return keySerializerName;
  }

  public String getMessageSerializerName() {
    return messageSerializerName;
  }

  public Object getPartitionKey() {
    return partitionKey;
  }

  public Object getKey() {
    return key;
  }

  public Object getMessage() {
    return message;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((keySerializerName == null) ? 0 : keySerializerName.hashCode());
    result = prime * result + ((message == null) ? 0 : message.hashCode());
    result = prime * result + ((messageSerializerName == null) ? 0 : messageSerializerName.hashCode());
    result = prime * result + ((partitionKey == null) ? 0 : partitionKey.hashCode());
    result = prime * result + ((systemStream == null) ? 0 : systemStream.hashCode());
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
    OutgoingMessageEnvelope other = (OutgoingMessageEnvelope) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (keySerializerName == null) {
      if (other.keySerializerName != null)
        return false;
    } else if (!keySerializerName.equals(other.keySerializerName))
      return false;
    if (message == null) {
      if (other.message != null)
        return false;
    } else if (!message.equals(other.message))
      return false;
    if (messageSerializerName == null) {
      if (other.messageSerializerName != null)
        return false;
    } else if (!messageSerializerName.equals(other.messageSerializerName))
      return false;
    if (partitionKey == null) {
      if (other.partitionKey != null)
        return false;
    } else if (!partitionKey.equals(other.partitionKey))
      return false;
    if (systemStream == null) {
      if (other.systemStream != null)
        return false;
    } else if (!systemStream.equals(other.systemStream))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "OutgoingMessageEnvelope [systemStream=" + systemStream + ", keySerializerName=" + keySerializerName + ", messageSerializerName=" + messageSerializerName + ", partitionKey=" + partitionKey + ", key=" + key + ", message=" + message + "]";
  }
}
