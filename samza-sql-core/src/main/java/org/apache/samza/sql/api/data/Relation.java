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

package org.apache.samza.sql.api.data;

import org.apache.samza.storage.kv.KeyValueStore;


/**
 * This class defines the general interface of {@code Relation}, which is defined as a map of {@link org.apache.samza.sql.api.data.Tuple}.
 *
 * <p>The interface is defined as an extension to {@link org.apache.samza.storage.kv.KeyValueStore}.
 *
 */

public interface Relation<K> extends KeyValueStore<K, Tuple> {

  /**
   * Get the name of the relation
   *
   * @return The relation name
   */
  EntityName getName();
}
