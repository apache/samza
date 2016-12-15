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
import org.apache.samza.config.Config

/**
 * A serializer for doubles
 */
class DoubleSerdeFactory extends SerdeFactory[java.lang.Double] {
  def getSerde(name: String, config: Config): Serde[java.lang.Double] = new DoubleSerde
}

class DoubleSerde extends Serde[java.lang.Double] {
  def toBytes(obj: java.lang.Double): Array[Byte] = if (obj != null) {
    ByteBuffer.allocate(8).putDouble(obj.doubleValue()).array
  } else {
    null
  }

  // big-endian by default
  def fromBytes(bytes: Array[Byte]): java.lang.Double = if (bytes != null) {
    ByteBuffer.wrap(bytes).getDouble
  } else {
    null
  }
}
