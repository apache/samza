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

import java.util.HashMap;
import java.util.Map;


/**
 * This class defines the name scheme for the collective data entities in Samza Stream SQL, i.e. tables and streams.
 */
public class EntityName {
  /**
   * {@code EntityType} defines the types of the entity names
   *
   */
  private enum EntityType {
    TABLE,
    STREAM
  };

  /**
   * Type of the entity name
   */
  private final EntityType type;

  /**
   * Formatted name of the entity.
   *
   * <p>This formatted name of the entity should be unique identifier for the corresponding table/stream in the system.
   * e.g. for a Kafka system stream named "mystream", the formatted name should be "kafka:mystream".
   */
  private final String name;

  private final boolean isSystemEntity;

  /**
   * Static map of already allocated table names
   */
  private static Map<String, EntityName> tables = new HashMap<String, EntityName>();

  /**
   * Static map of already allocated stream names
   */
  private static Map<String, EntityName> streams = new HashMap<String, EntityName>();

  private static final String ANONYMOUS = "anonymous";

  /**
   * Private ctor to create entity names
   *
   * @param type Type of the entity name
   * @param name Formatted name of the entity
   * @param isSystemEntity whether the entity is a system input/output
   */
  private EntityName(EntityType type, String name, boolean isSystemEntity) {
    this.type = type;
    this.name = name;
    this.isSystemEntity = isSystemEntity;
  }

  @Override
  public String toString() {
    return String.format("%s:%s", this.type, this.name);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof EntityName) {
      EntityName otherEntity = (EntityName) other;
      return this.type.equals(otherEntity.type) && this.name.equals(otherEntity.name);
    }
    return false;
  }

  /**
   * Check to see whether this entity name is for a table
   *
   * @return true if the entity type is {@code EntityType.TABLE}; false otherwise
   */
  public boolean isTable() {
    return this.type.equals(EntityType.TABLE);
  }

  /**
   * Check to see whether this entity name is for a stream
   *
   * @return true if the entity type is {@code EntityType.STREAM}; false otherwise
   */
  public boolean isStream() {
    return this.type.equals(EntityType.STREAM);
  }

  public boolean isSystemEntity() {
    return this.isSystemEntity;
  }

  /**
   * Get the formatted entity name
   *
   * @return The formatted entity name
   */
  public String getName() {
    return this.name;
  }

  public static EntityName getTableName(String name) {
    return getTableName(name, false);
  }

  public static EntityName getStreamName(String name) {
    return getStreamName(name, false);
  }

  /**
   * Static method to get the instance of {@code EntityName} with type {@code EntityType.TABLE}
   *
   * @param name The formatted entity name of the relation
   * @param isSystem The boolean flag indicating whether this is a system input/output
   * @return A <code>EntityName</code> for a relation
   */
  public static EntityName getTableName(String name, boolean isSystem) {
    if (tables.get(name) == null) {
      tables.put(name, new EntityName(EntityType.TABLE, name, isSystem));
    }
    return tables.get(name);
  }

  /**
   * Static method to get the instance of <code>EntityName</code> with type <code>EntityType.STREAM</code>
   *
   * @param name The formatted entity name of the stream
   * @param isSystem The boolean flag indicating whether this is a system input/output
   * @return A <code>EntityName</code> for a stream
   */
  public static EntityName getStreamName(String name, boolean isSystem) {
    if (streams.get(name) == null) {
      streams.put(name, new EntityName(EntityType.STREAM, name, isSystem));
    }
    return streams.get(name);
  }

  public static EntityName getAnonymousStream() {
    return getStreamName(ANONYMOUS);
  }

  public static EntityName getAnonymousTable() {
    return getTableName(ANONYMOUS);
  }

  public boolean isAnonymous() {
    return this.name.equals(ANONYMOUS);
  }
}
