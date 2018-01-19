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

import java.io.Serializable;

/**
 * A Serde is a convenience type that implements both the {@link org.apache.samza.serializers.Serializer} and
 * {@link org.apache.samza.serializers.Deserializer} interfaces, allowing it to both read and write data
 * in its value type, T.
 *
 * A serde instance itself must be {@link Serializable} using Java serialization.
 *
 * @param <T> The type of serialized object implementations can both read and write
 */
public interface Serde<T> extends Serializer<T>, Deserializer<T>, Serializable {
}
