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

/**
 * This interface defines a non-ordered {@link org.apache.samza.sql.api.data.Relation}, which has a unique primary key
 *
 * <p> This is to define a table created by CREATE TABLE statement
 *
 * @param <K> The primary key for the {@code Table} class
 */
public interface Table<K> extends Relation<K> {

  /**
   * Get the primary key field name for this table
   *
   * @return The name of the primary key field
   */
  String getPrimaryKeyName();

}
