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

import java.util.concurrent.CompletableFuture;


/**
 * Define how the batch operations will be handled.
 *
 * @param <K> The key type of the operations
 * @param <V> The value type of the operations.
 * @param <U> The update type of the operations.
 */
public interface BatchHandler<K, V, U> {
  /**
   *
   * @param batch The batch to be handled
   * @return A {@link CompletableFuture} that indicates the status of the handle process.
   */
  CompletableFuture<Void> handle(Batch<K, V, U> batch);
}
