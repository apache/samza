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
import org.apache.samza.config.Config
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.LoggerFactory


/**
  * A serializer for JSON strings. JsonSerdeV2 differs from JsonSerde in that:
  * <ol>
  *   <li>
  *     It allows specifying the specific POJO type to deserialize to (using JsonSerdeV2(Class[T])
  *     or JsonSerdeV2#of(Class[T]). JsonSerde always returns a LinkedHashMap<String, Object> upon deserialization.
  *   <li>
  *     It uses Jackson's default 'camelCase' property naming convention, which simplifies defining
  *     the POJO to bind to. JsonSerde enforces the 'dash-separated' property naming convention.
  * </ol>
  * This JsonSerdeV2 should be preferred over JsonSerde for High Level API applications, unless
  * backwards compatibility with the older data format (with dasherized names) is required.
  *
  * @param clazzOption the class of the POJO being (de)serialized. If this is None,
  *                    a LinkedHashMap<String, Object> is returned upon deserialization.
  * @tparam T the type of the POJO being (de)serialized.
  */
class JsonSerdeV2[T] private(clazzOption: Option[Class[T]]) extends Serde[T] {
  private val LOG = LoggerFactory.getLogger(classOf[JsonSerdeV2[T]])
  @transient lazy private val mapper = new ObjectMapper()

  def this() {
    this(None)
  }

  def this(clazz: Class[T]) {
    this(Option(clazz))
  }

  def toBytes(obj: T): Array[Byte] = {
    try {
      val str = mapper.writeValueAsString(obj)
      str.getBytes("UTF-8")
    } catch {
      case e: Exception => throw new SamzaException(e);
    }
  }

  def fromBytes(bytes: Array[Byte]): T = {
    val str = new String(bytes, "UTF-8")
     try {
       clazzOption match {
         case Some(clazz) => mapper.readValue(str, clazz)
         case None => mapper.readValue(str, new TypeReference[T]() {})
       }
     } catch {
       case e: Exception =>
         LOG.debug(s"Error deserializing message: $str", e)
         throw new SamzaException(e)
     }
  }

}

object JsonSerdeV2 {
  def of[T](clazz: Class[T]): JsonSerdeV2[T] = {
    new JsonSerdeV2[T](clazz)
  }
}

class JsonSerdeV2Factory extends SerdeFactory[Object] {
  def getSerde(name: String, config: Config) = new JsonSerdeV2
}
