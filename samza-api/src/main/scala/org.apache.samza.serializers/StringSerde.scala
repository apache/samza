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

import org.apache.samza.config.Config

/**
 * A serializer for strings
 */
class StringSerdeFactory extends SerdeFactory[String] {
  def getSerde(name: String, config: Config): Serde[String] =
    new StringSerde(config.get("encoding", "UTF-8"))
}

class StringSerde(val encoding: String) extends Serde[String] {
  // constructor (for Java) that defaults to UTF-8 encoding
  def this() {
    this("UTF-8")
  }

  def toBytes(obj: String): Array[Byte] = if (obj != null) {
    obj.toString.getBytes(encoding)
  } else {
    null
  }

  def fromBytes(bytes: Array[Byte]): String = if (bytes != null) {
    new String(bytes, 0, bytes.size, encoding)
  } else {
    null
  }
}
