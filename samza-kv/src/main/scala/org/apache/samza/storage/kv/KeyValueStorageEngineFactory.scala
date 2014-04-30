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

package org.apache.samza.storage.kv

import java.io.File
import org.apache.samza.config.Config
import org.apache.samza.container.SamzaContainerContext
import org.apache.samza.serializers._
import org.apache.samza.SamzaException
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.task.MessageCollector
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.storage.StorageEngineFactory
import org.apache.samza.storage.StorageEngine

class KeyValueStorageEngineFactory[K, V] extends StorageEngineFactory[K, V] {
  def getStorageEngine(
    storeName: String,
    storeDir: File,
    keySerde: Serde[K],
    msgSerde: Serde[V],
    collector: MessageCollector,
    registry: MetricsRegistry,
    changeLogSystemStreamPartition: SystemStreamPartition,
    containerContext: SamzaContainerContext): StorageEngine = {

    val storageConfig = containerContext.config.subset("stores." + storeName + ".", true)
    val batchSize = storageConfig.getInt("write.batch.size", 500)
    val cacheSize = storageConfig.getInt("object.cache.size", math.max(batchSize, 1000))
    val deleteCompactionThreshold = storageConfig.getInt("compaction.delete.threshold", -1)
    val enableCache = cacheSize > 0

    if (cacheSize > 0 && cacheSize < batchSize) {
      throw new SamzaException("A stores cache.size cannot be less than batch.size as batched values reside in cache.")
    }

    if (keySerde == null) {
      throw new SamzaException("Must define a key serde when using key value storage.")
    }

    if (msgSerde == null) {
      throw new SamzaException("Must define a message serde when using key value storage.")
    }

    val levelDbMetrics = new LevelDbKeyValueStoreMetrics(storeName, registry)
    val levelDbOptions = LevelDbKeyValueStore.options(storageConfig, containerContext)
    val levelDb = new LevelDbKeyValueStore(storeDir, levelDbOptions, deleteCompactionThreshold, levelDbMetrics)
    val maybeLoggedStore = if (changeLogSystemStreamPartition == null) {
      levelDb
    } else {
      val loggedStoreMetrics = new LoggedStoreMetrics(storeName, registry)
      new LoggedStore(levelDb, changeLogSystemStreamPartition, collector, loggedStoreMetrics)
    }
    val serializedMetrics = new SerializedKeyValueStoreMetrics(storeName, registry)
    val serialized = new SerializedKeyValueStore[K, V](maybeLoggedStore, keySerde, msgSerde, serializedMetrics)
    val maybeCachedStore = if (enableCache) {
      val cachedStoreMetrics = new CachedStoreMetrics(storeName, registry)
      new CachedStore(serialized, cacheSize, batchSize, cachedStoreMetrics)
    } else {
      serialized
    }
    val db = new NullSafeKeyValueStore(maybeCachedStore)
    val keyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics(storeName, registry)

    // Decide if we should use raw bytes when restoring

    new KeyValueStorageEngine(db, levelDb, keyValueStorageEngineMetrics, batchSize)
  }
}
