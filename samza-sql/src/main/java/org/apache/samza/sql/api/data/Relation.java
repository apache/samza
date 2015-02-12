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
 * This class defines the general interface of <code>Relation</code>, which is defined as a map of <code>Tuple</code>.
 *
 * <p>The interface is defined as an extension to <code>KeyValueStore&lt;Object, Tuple&gt;</code>.
 *
 */

public interface Relation extends KeyValueStore<Object, Tuple> {

  /**
   * Get the primary key field name for this table
   *
   * @return The name of the primary key field
   */
  String getPrimaryKey();

  /**
   * Get the name of the relation created by CREATE TABLE
   *
   * @return The relation name
   */
  EntityName getName();
}
