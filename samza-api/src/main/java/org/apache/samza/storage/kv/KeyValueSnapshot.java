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

package org.apache.samza.storage.kv;

/**
 * An immutable view of the {@link KeyValueStore} at a point-in-time.
 * The snapshot MUST be closed after use.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KeyValueSnapshot<K, V> {
  /**
   * Creates a new iterator for this snapshot. The iterator MUST be
   * closed after its execution by invoking {@link KeyValueIterator#close}.
   * @return an iterator
   */
  KeyValueIterator<K, V> iterator();

  /**
   * Closes this snapshot releasing any associated resources. Once a
   * snapshot is closed, no new iterators can be created for it.
   */
  void close();
}
