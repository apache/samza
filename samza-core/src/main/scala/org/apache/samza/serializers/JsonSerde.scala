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
import org.apache.samza.serializers.model.SamzaObjectMapper
import org.codehaus.jackson.`type`.TypeReference
import org.apache.samza.config.Config

class JsonSerde[T] extends Serde[T] {
  @transient lazy val mapper = SamzaObjectMapper.getObjectMapper()
  var clazzOption: Option[Class[T]] = None

  def this(clazz: Class[T]) {
    this()
    if (clazz == null) throw new SamzaException("clazz must not be null.")
    this.clazzOption = Option(clazz)
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
       case e: Exception => throw new SamzaException(e)
     }
  }

}

class JsonSerdeFactory extends SerdeFactory[Object] {
  def getSerde(name: String, config: Config) = new JsonSerde
}
