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

package org.apache.samza.storage.kv

import java.io.Serializable
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.util.ArrayList

class AccessLogMessage(val DBOperation: Int,
    val duration: Long,
    val keys: ArrayList[Array[Byte]],
    val timestamp: Long
   ) extends Serializable {


  def serialize() : Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val outputStream = new ObjectOutputStream(byteStream)
    outputStream.writeObject(this)
    outputStream.close
    val obj: Array[Byte] = byteStream.toByteArray
    byteStream.close
    return obj
  }

}
