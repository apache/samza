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

/**
  * A marker serde class to indicate that messages should not be serialized or deserialized.
  * This is the same behavior as when no serde is provided, and is intended for use cases where
  * a Serde parameter or configuration is required.
  * This is different than [[ByteSerde]] which is a pass-through serde for byte arrays.
  *
  * @tparam T type of messages which should not be serialized or deserialized
  */
class NoOpSerde[T] extends Serde[T] {

  override def fromBytes(bytes: Array[Byte]): T =
    throw new NotImplementedError("NoOpSerde fromBytes should not be invoked by the framework.")

  override def toBytes(obj: T): Array[Byte] =
    throw new NotImplementedError("NoOpSerde toBytes should not be invoked by the framework.")
  
}
