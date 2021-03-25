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
package org.apache.samza.storage;

/**
 * Immutable class that defines the properties of a Store
 */
public class StoreProperties {
  private final boolean persistedToDisk;
  private final boolean loggedStore;
  private final boolean durable;

  private StoreProperties(
      final boolean persistedToDisk,
      final boolean loggedStore,
      final boolean durable) {
    this.persistedToDisk = persistedToDisk;
    this.loggedStore = loggedStore;
    this.durable = durable;
  }

  /**
   * Flag to indicate whether a store can be persisted to disk or not
   *
   * @return True, if store can be flushed to disk. False, by default.
   */
  public boolean isPersistedToDisk() {
    return persistedToDisk;
  }

  /**
   * Flag to indicate whether a store is associated with a changelog (used for recovery) or not
   *
   * @return True, if changelog is enabled. False, by default.
   */
  public boolean isLoggedStore() {
    return loggedStore;
  }

  /**
   * Flag to indicate whether a store is durable, that is, it's contents are available cross container restarts
   * or host reallocation.
   *
   * @return True, if the store is durable. False by default.
   */
  public boolean isDurableStore() {
    return durable;
  }

  public static class StorePropertiesBuilder {
    private boolean persistedToDisk = false;
    private boolean loggedStore = false;
    private boolean durable = false;

    public StorePropertiesBuilder setPersistedToDisk(boolean persistedToDisk) {
      this.persistedToDisk = persistedToDisk;
      return this;
    }

    public StorePropertiesBuilder setLoggedStore(boolean loggedStore) {
      this.loggedStore = loggedStore;
      return this;
    }

    public StorePropertiesBuilder setIsDurable(boolean durable) {
      this.durable = durable;
      return this;
    }

    public StoreProperties build() {
      return new StoreProperties(persistedToDisk, loggedStore, durable);
    }
  }
}
