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

package org.apache.samza.serializers

import org.apache.samza.SamzaException
import org.apache.samza.config.SerializerConfig
import org.apache.samza.system.SystemStream
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.IncomingMessageEnvelope

class SerdeManager(
  serdes: Map[String, Serde[Object]] = Map(),
  systemKeySerdes: Map[String, Serde[Object]] = Map(),
  systemMessageSerdes: Map[String, Serde[Object]] = Map(),
  systemStreamKeySerdes: Map[SystemStream, Serde[Object]] = Map(),
  systemStreamMessageSerdes: Map[SystemStream, Serde[Object]] = Map(),
  changeLogSystemStreams: Set[SystemStream] = Set(),
  accessLogSystemStreams: Set[SystemStream] = Set()) {

  def toBytes(obj: Object, serializerName: String) = serdes
    .getOrElse(serializerName, throw new SamzaException("No serde defined for %s" format serializerName))
    .toBytes(obj)

  def toBytes(envelope: OutgoingMessageEnvelope): OutgoingMessageEnvelope = {
    val key = if (changeLogSystemStreams.contains(envelope.getSystemStream)
      || accessLogSystemStreams.contains(envelope.getSystemStream)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      envelope.getKey
    } else if (envelope.getKeySerializerName != null) {
      // If a serde is defined, use it.
      toBytes(envelope.getKey, envelope.getKeySerializerName)
    } else if (systemStreamKeySerdes.contains(envelope.getSystemStream)) {
      // If the stream has a serde defined, use it.
      systemStreamKeySerdes(envelope.getSystemStream).toBytes(envelope.getKey)
    } else if (systemKeySerdes.contains(envelope.getSystemStream.getSystem)) {
      // If the system has a serde defined, use it.
      systemKeySerdes(envelope.getSystemStream.getSystem).toBytes(envelope.getKey)
    } else {
      // Just use the object.
      envelope.getKey
    }

    val message = if (changeLogSystemStreams.contains(envelope.getSystemStream)
      || accessLogSystemStreams.contains(envelope.getSystemStream)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      envelope.getMessage
    } else if (envelope.getMessageSerializerName != null) {
      // If a serde is defined, use it.
      toBytes(envelope.getMessage, envelope.getMessageSerializerName)
    } else if (systemStreamMessageSerdes.contains(envelope.getSystemStream)) {
      // If the stream has a serde defined, use it.
      systemStreamMessageSerdes(envelope.getSystemStream).toBytes(envelope.getMessage)
    } else if (systemMessageSerdes.contains(envelope.getSystemStream.getSystem)) {
      // If the system has a serde defined, use it.
      systemMessageSerdes(envelope.getSystemStream.getSystem).toBytes(envelope.getMessage)
    } else {
      // Just use the object.
      envelope.getMessage
    }

    if ((key eq envelope.getKey) && (message eq envelope.getMessage)) {
      envelope
    } else {
      new OutgoingMessageEnvelope(
        envelope.getSystemStream,
        null,
        null,
        envelope.getPartitionKey,
        key,
        message)
    }
  }

  def fromBytes(bytes: Array[Byte], deserializerName: String) = serdes
    .getOrElse(deserializerName, throw new SamzaException("No serde defined for %s" format deserializerName))
    .fromBytes(bytes)

  def fromBytes(envelope: IncomingMessageEnvelope) = {
    val key = if (changeLogSystemStreams.contains(envelope.getSystemStreamPartition.getSystemStream)
      || accessLogSystemStreams.contains(envelope.getSystemStreamPartition.getSystemStream)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      envelope.getKey
    } else if (systemStreamKeySerdes.contains(envelope.getSystemStreamPartition)) {
      // If the stream has a serde defined, use it.
      systemStreamKeySerdes(envelope.getSystemStreamPartition).fromBytes(envelope.getKey.asInstanceOf[Array[Byte]])
    } else if (systemKeySerdes.contains(envelope.getSystemStreamPartition.getSystem)) {
      // If the system has a serde defined, use it.
      systemKeySerdes(envelope.getSystemStreamPartition.getSystem).fromBytes(envelope.getKey.asInstanceOf[Array[Byte]])
    } else {
      // Just use the object.
      envelope.getKey
    }

    val message = if (changeLogSystemStreams.contains(envelope.getSystemStreamPartition.getSystemStream)
      || accessLogSystemStreams.contains(envelope.getSystemStreamPartition.getSystemStream)) {
      // If the stream is a change log stream, don't do any serde. It is up to storage engines to handle serde.
      envelope.getMessage
    } else if (systemStreamMessageSerdes.contains(envelope.getSystemStreamPartition)) {
      // If the stream has a serde defined, use it.
      systemStreamMessageSerdes(envelope.getSystemStreamPartition).fromBytes(envelope.getMessage.asInstanceOf[Array[Byte]])
    } else if (systemMessageSerdes.contains(envelope.getSystemStreamPartition.getSystem)) {
      // If the system has a serde defined, use it.
      systemMessageSerdes(envelope.getSystemStreamPartition.getSystem).fromBytes(envelope.getMessage.asInstanceOf[Array[Byte]])
    } else {
      // Just use the object.
      envelope.getMessage
    }

    if ((key eq envelope.getKey) && (message eq envelope.getMessage)) {
      envelope
    } else {
      new IncomingMessageEnvelope(
        envelope.getSystemStreamPartition,
        envelope.getOffset,
        key,
        message)
    }
  }
}
