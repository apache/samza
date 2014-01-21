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

package org.apache.samza.serializers;

/**
 * A standard interface for Samza compatible serializers, used for serializing objects to bytes.
 * @param <T> The type of object this serializer should be implemented to serialize.
 */
public interface Serializer<T> {
  /**
   * Serializes given object to an array of bytes.
   * @param object Object of specific type to serialize.
   * @return An array of bytes representing the object in serialized form.
   */
  byte[] toBytes(T object);
}
