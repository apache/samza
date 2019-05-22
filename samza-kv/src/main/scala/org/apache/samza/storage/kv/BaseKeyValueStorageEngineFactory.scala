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

import org.apache.samza.SamzaException
import org.apache.samza.config.MetricsConfig.Config2Metrics
import org.apache.samza.context.{ContainerContext, JobContext}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.Serde
import org.apache.samza.storage.StorageEngineFactory.StoreMode
import org.apache.samza.storage.{StorageEngine, StorageEngineFactory, StoreProperties}
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.MessageCollector
import org.apache.samza.util.HighResolutionClock

/**
  * A key value storage engine factory implementation
  *
  * This trait encapsulates all the steps needed to create a key value storage engine. It is meant to be extended
  * by the specific key value store factory implementations which will in turn override the getKVStore method.
  */
trait BaseKeyValueStorageEngineFactory[K, V] extends StorageEngineFactory[K, V] {

  private val INMEMORY_KV_STORAGE_ENGINE_FACTORY =
    "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory"
  private val CHANGELOG_MAX_MSG_SIZE_BYTES = "changelog.max.message.size.bytes"
  private val DEFAULT_CHANGELOG_MAX_MSG_SIZE_BYTES = 1000000 // slightly less than 1 MB
  private val EXPECT_LARGE_MESSAGES = "expect.large.message"
  private val DEFAULT_EXPECT_LARGE_MESSAGES = false
  private val DROP_LARGE_MESSAGES = "drop.large.messages"
  private val DEFAULT_DROP_LARGE_MESSAGES = false

  /**
   * Return a KeyValueStore instance for the given store name,
   * which will be used as the underlying raw store
   *
   * @param storeName Name of the store
   * @param storeDir The directory of the store
   * @param registry MetricsRegistry to which to publish store specific metrics.
   * @param changeLogSystemStreamPartition Samza stream partition from which to receive the changelog.
   * @param containerContext Information about the container in which the task is executing.
   * @return A valid KeyValueStore instance
   */
  def getKVStore(storeName: String,
    storeDir: File,
    registry: MetricsRegistry,
    changeLogSystemStreamPartition: SystemStreamPartition,
    jobContext: JobContext,
    containerContext: ContainerContext, storeMode: StoreMode): KeyValueStore[Array[Byte], Array[Byte]]

  /**
   * Constructs a key-value StorageEngine and returns it to the caller
   *
   * @param storeName The name of the storage engine.
   * @param storeDir The directory of the storage engine.
   * @param keySerde The serializer to use for serializing keys when reading or writing to the store.
   * @param msgSerde The serializer to use for serializing messages when reading or writing to the store.
   * @param collector MessageCollector the storage engine uses to persist changes.
   * @param registry MetricsRegistry to which to publish storage-engine specific metrics.
   * @param changeLogSystemStreamPartition Samza stream partition from which to receive the changelog.
   * @param containerContext Information about the container in which the task is executing.
   **/
  def getStorageEngine(storeName: String,
    storeDir: File,
    keySerde: Serde[K],
    msgSerde: Serde[V],
    collector: MessageCollector,
    registry: MetricsRegistry,
    changeLogSystemStreamPartition: SystemStreamPartition,
    jobContext: JobContext,
    containerContext: ContainerContext, storeMode : StoreMode): StorageEngine = {
    val storageConfig = jobContext.getConfig.subset("stores." + storeName + ".", true)
    val storeFactory = storageConfig.get("factory")
    var storePropertiesBuilder = new StoreProperties.StorePropertiesBuilder()
    val accessLog = storageConfig.getBoolean("accesslog.enabled", false)
    val maxMessageSize = storageConfig.getInt(CHANGELOG_MAX_MSG_SIZE_BYTES, DEFAULT_CHANGELOG_MAX_MSG_SIZE_BYTES)
    val largeMessagesExpected = storageConfig.getBoolean(EXPECT_LARGE_MESSAGES, DEFAULT_EXPECT_LARGE_MESSAGES)
    val dropLargeMessage = storageConfig.getBoolean(DROP_LARGE_MESSAGES, DEFAULT_DROP_LARGE_MESSAGES)

    if (storeFactory == null) {
      throw new SamzaException("Store factory not defined. Cannot proceed with KV store creation!")
    }
    if (!storeFactory.equals(INMEMORY_KV_STORAGE_ENGINE_FACTORY)) {
      storePropertiesBuilder = storePropertiesBuilder.setPersistedToDisk(true)
    }

    val batchSize = storageConfig.getInt("write.batch.size", 500)
    val cacheSize = storageConfig.getInt("object.cache.size", math.max(batchSize, 1000))
    val enableCache = cacheSize > 0

    if (cacheSize > 0 && cacheSize < batchSize) {
      throw new SamzaException("A store's cache.size cannot be less than batch.size as batched values reside in cache.")
    }

    if (keySerde == null) {
      throw new SamzaException("Must define a key serde when using key value storage.")
    }

    if (msgSerde == null) {
      throw new SamzaException("Must define a message serde when using key value storage.")
    }

    val rawStore =
      getKVStore(storeName, storeDir, registry, changeLogSystemStreamPartition, jobContext, containerContext, storeMode)

    // maybe wrap with logging
    val maybeLoggedStore = if (changeLogSystemStreamPartition == null) {
      rawStore
    } else {
      val loggedStoreMetrics = new LoggedStoreMetrics(storeName, registry)
      storePropertiesBuilder = storePropertiesBuilder.setLoggedStore(true)
      new LoggedStore(rawStore, changeLogSystemStreamPartition, collector, loggedStoreMetrics)
    }

    var toBeAccessLoggedStore: KeyValueStore[K, V] = null

    if (largeMessagesExpected) {
      // Execute "if" when we expect to get large messages and user defined config -> EXPECT_LARGE_MESSAGES = true
      // This code path will throw a SamzaException for large message size.

      // maybe wrap with caching
      val maybeCachedStore = if (enableCache) {
        val cachedStoreMetrics = new CachedStoreMetrics(storeName, registry)
        new CachedStore(maybeLoggedStore, cacheSize, batchSize, cachedStoreMetrics)
      } else {
        maybeLoggedStore
      }

      // wrap with large message checking
      val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(maybeCachedStore, storeName, false, maxMessageSize)

      // wrap with serialization
      val serializedMetrics = new SerializedKeyValueStoreMetrics(storeName, registry)
      val serialized = new SerializedKeyValueStore[K, V](largeMessageSafeKeyValueStore, keySerde, msgSerde, serializedMetrics)
      toBeAccessLoggedStore = serialized

    } else {

      var toBeSerializedStore = maybeLoggedStore
      if (dropLargeMessage) {
        // Call LargeMessageSafeKeyValueStore when EXPECT_LARGE_MESSAGES = false but DROP_LARGE_MESSAGES = true
        // This if statement code path will ignore all large messages and continue to add all other messages to the desired stores.

        // wrap with large message checking
        toBeSerializedStore = new LargeMessageSafeStore(maybeLoggedStore, storeName, dropLargeMessage, maxMessageSize)
      }
      // wrap with serialization
      val serializedMetrics = new SerializedKeyValueStoreMetrics(storeName, registry)
      val serialized = new SerializedKeyValueStore[K, V](toBeSerializedStore, keySerde, msgSerde, serializedMetrics)

      // maybe wrap with caching
      val maybeCachedStore = if (enableCache) {
        val cachedStoreMetrics = new CachedStoreMetrics(storeName, registry)
        new CachedStore(serialized, cacheSize, batchSize, cachedStoreMetrics)
      } else {
        serialized
      }
      toBeAccessLoggedStore = maybeCachedStore
    }

    val maybeAccessLoggedStore = if (accessLog) {
      new AccessLoggedStore(toBeAccessLoggedStore, collector, changeLogSystemStreamPartition, storageConfig, storeName, keySerde)
    } else {
      toBeAccessLoggedStore
    }

    // wrap with null value checking
    val nullSafeStore = new NullSafeKeyValueStore(maybeAccessLoggedStore)

    // create the storage engine and return
    // TODO: Decide if we should use raw bytes when restoring
    val keyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics(storeName, registry)
    val clock = if (jobContext.getConfig.getMetricsTimerEnabled) {
      new HighResolutionClock {
        override def nanoTime(): Long = System.nanoTime()
      }
    } else {
      new HighResolutionClock {
        override def nanoTime(): Long = 0L
      }
    }

    new KeyValueStorageEngine(storeName, storeDir, storePropertiesBuilder.build(), nullSafeStore, rawStore,
      keyValueStorageEngineMetrics, batchSize, () => clock.nanoTime())
  }
}
