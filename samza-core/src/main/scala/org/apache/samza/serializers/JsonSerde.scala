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
import org.codehaus.jackson.map.ObjectMapper
import org.apache.samza.config.Config

class JsonSerde[T] extends Serde[T] {
  val mapper = SamzaObjectMapper.getObjectMapper()

  def toBytes(obj: T): Array[Byte] = {
    try {
      mapper.writeValueAsString(obj).getBytes("UTF-8")
    }
    catch {
      case e: Exception => throw new SamzaException(e);
    }
  }

  def fromBytes(bytes: Array[Byte]): T = {
     try {
         mapper.readValue(new String(bytes, "UTF-8"), new TypeReference[T]() {})}
     catch {
       case e: Exception => throw new SamzaException(e);
     }
  }

}

class JsonSerdeFactory extends SerdeFactory[Object] {
  def getSerde(name: String, config: Config) = new JsonSerde
}
