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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.apache.samza.config.Config

/**
 * A serializer for Serializable
 */
class SerializableSerdeFactory[T <: java.io.Serializable] extends SerdeFactory[T] {
  def getSerde(name: String, config: Config): Serde[T] =
    new SerializableSerde[T]
}

class SerializableSerde[T <: java.io.Serializable] extends Serde[T] {
  def toBytes(obj: T): Array[Byte] = if (obj != null) {
    val bos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(bos)

    try {
      oos.writeObject(obj)
    }
    finally {
      oos.close()
    }

    bos.toByteArray
  } else {
    null
  }

  def fromBytes(bytes: Array[Byte]): T = if (bytes != null) {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)

    try {
      ois.readObject.asInstanceOf[T]
    }
    finally{
      ois.close()
    }
  } else {
    null.asInstanceOf[T]
  }
}
