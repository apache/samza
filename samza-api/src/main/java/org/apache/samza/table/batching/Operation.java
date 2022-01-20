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

package org.apache.samza.table.batching;

/**
 * Interface for table operations that can be batched.
 *
 * @param <K> The key type associated with the operation.
 * @param <V> The value type associated with the operation.
 * @param <U> The update type associated with the operation.
 */
public interface Operation<K, V, U> {
  /**
   * @return The key associated with the operation.
   */
  K getKey();

  /**
   * @return The value associated with the operation.
   */
  V getValue();

  /**
   * @return The update associated with the operation.
   */
  U getUpdate();

  /**
   * @return The extra arguments associated with the operation.
   */
  Object[] getArgs();
}
