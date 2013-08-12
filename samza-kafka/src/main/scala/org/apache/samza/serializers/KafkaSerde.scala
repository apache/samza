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
import java.nio.ByteBuffer
import org.apache.samza.util.Util
import kafka.serializer.Encoder
import kafka.serializer.Decoder
import org.apache.samza.config.Config
import org.apache.samza.config.KafkaSerdeConfig.Config2KafkaSerde
import org.apache.samza.SamzaException

class KafkaSerde[T](encoder: Encoder[T], decoder: Decoder[T]) extends Serde[T] {
  def toBytes(obj: T): Array[Byte] = encoder.toBytes(obj)
  def fromBytes(bytes: Array[Byte]): T = decoder.fromBytes(bytes)
}

class KafkaSerdeFactory[T] extends SerdeFactory[T] {
  def getSerde(name: String, config: Config): Serde[T] = {
    val encoderClassName = config
      .getKafkaEncoder(name)
      .getOrElse(throw new SamzaException("No kafka encoder defined for %s" format name))
    val decoderClassName = config
      .getKafkaDecoder(name)
      .getOrElse(throw new SamzaException("No kafka decoder defined for %s" format name))
    val encoder = Util.getObj[Encoder[T]](encoderClassName)
    val decoder = Util.getObj[Decoder[T]](decoderClassName)
    new KafkaSerde(encoder, decoder)
  }
}
