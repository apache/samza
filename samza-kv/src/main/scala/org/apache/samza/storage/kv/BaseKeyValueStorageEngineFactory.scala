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
import org.apache.samza.config.{MetricsConfig, StorageConfig}
import org.apache.samza.context.{ContainerContext, JobContext}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.Serde
import org.apache.samza.storage.StorageEngineFactory.StoreMode
import org.apache.samza.storage.{StorageEngine, StorageEngineFactory, StoreProperties}
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.MessageCollector
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util.{HighResolutionClock, Logging}

/**
  * A key value storage engine factory implementation
  *
  * This trait encapsulates all the steps needed to create a key value storage engine. It is meant to be extended
  * by the specific key value store factory implementations which will in turn override the getKVStore method.
  */
trait BaseKeyValueStorageEngineFactory[K, V] extends StorageEngineFactory[K, V] {

  private val INMEMORY_KV_STORAGE_ENGINE_FACTORY =
    "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory"

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
   * @param changelogCollector MessageCollector the storage engine uses to persist changes.
   * @param registry MetricsRegistry to which to publish storage-engine specific metrics.
   * @param changelogSSP Samza system stream partition from which to receive the changelog.
   * @param containerContext Information about the container in which the task is executing.
   **/
  def getStorageEngine(storeName: String,
    storeDir: File,
    keySerde: Serde[K],
    msgSerde: Serde[V],
    changelogCollector: MessageCollector,
    registry: MetricsRegistry,
    changelogSSP: SystemStreamPartition,
    jobContext: JobContext,
    containerContext: ContainerContext, storeMode : StoreMode): StorageEngine = {
    val storageConfigSubset = jobContext.getConfig.subset("stores." + storeName + ".", true)
    val storageConfig = new StorageConfig(jobContext.getConfig)
    val storeFactory = JavaOptionals.toRichOptional(storageConfig.getStorageFactoryClassName(storeName)).toOption
    var storePropertiesBuilder = new StoreProperties.StorePropertiesBuilder()
    val accessLog = storageConfig.getAccessLogEnabled(storeName)

    var maxMessageSize = storageConfig.getChangelogMaxMsgSizeBytes(storeName)
    val disallowLargeMessages = storageConfig.getDisallowLargeMessages(storeName)
    val dropLargeMessage = storageConfig.getDropLargeMessages(storeName)

    if (storeFactory.isEmpty) {
      throw new SamzaException("Store factory not defined. Cannot proceed with KV store creation!")
    }
    if (!storeFactory.get.equals(INMEMORY_KV_STORAGE_ENGINE_FACTORY)) {
      storePropertiesBuilder = storePropertiesBuilder.setPersistedToDisk(true)
    }

    val batchSize = storageConfigSubset.getInt("write.batch.size", 500)
    val cacheSize = storageConfigSubset.getInt("object.cache.size", math.max(batchSize, 1000))
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
      getKVStore(storeName, storeDir, registry, changelogSSP, jobContext, containerContext, storeMode)

    // maybe wrap with logging
    val maybeLoggedStore = if (changelogSSP == null) {
      rawStore
    } else {
      val loggedStoreMetrics = new LoggedStoreMetrics(storeName, registry)
      storePropertiesBuilder = storePropertiesBuilder.setLoggedStore(true)
      new LoggedStore(rawStore, changelogSSP, changelogCollector, loggedStoreMetrics)
    }

    var toBeAccessLoggedStore: KeyValueStore[K, V] = null

    // If large messages are disallowed in config, then this creates a LargeMessageSafeKeyValueStore that throws a
    // RecordTooLargeException when a large message is encountered.
    if (disallowLargeMessages) {
      // maybe wrap with caching
      val maybeCachedStore = if (enableCache) {
        createCachedStore(storeName, registry, maybeLoggedStore, cacheSize, batchSize)
      } else {
        maybeLoggedStore
      }

      // wrap with large message checking
      val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(maybeCachedStore, storeName, false, maxMessageSize)
      // wrap with serialization
      val serializedMetrics = new SerializedKeyValueStoreMetrics(storeName, registry)
      toBeAccessLoggedStore = new SerializedKeyValueStore[K, V](largeMessageSafeKeyValueStore, keySerde, msgSerde, serializedMetrics)

    }
    else {
      val toBeSerializedStore = if (dropLargeMessage) {
        // wrap with large message checking
        new LargeMessageSafeStore(maybeLoggedStore, storeName, dropLargeMessage, maxMessageSize)
      } else {
        maybeLoggedStore
      }
      // wrap with serialization
      val serializedMetrics = new SerializedKeyValueStoreMetrics(storeName, registry)
      val serializedStore = new SerializedKeyValueStore[K, V](toBeSerializedStore, keySerde, msgSerde, serializedMetrics)
      // maybe wrap with caching
      toBeAccessLoggedStore = if (enableCache) {
        createCachedStore(storeName, registry, serializedStore, cacheSize, batchSize)
      } else {
        serializedStore
      }
    }

    val maybeAccessLoggedStore = if (accessLog) {
      new AccessLoggedStore(toBeAccessLoggedStore, changelogCollector, changelogSSP, storageConfig, storeName, keySerde)
    } else {
      toBeAccessLoggedStore
    }

    // wrap with null value checking
    val nullSafeStore = new NullSafeKeyValueStore(maybeAccessLoggedStore)

    // create the storage engine and return
    val keyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics(storeName, registry)
    val metricsConfig = new MetricsConfig(jobContext.getConfig)
    val clock = if (metricsConfig.getMetricsTimerEnabled) {
      new HighResolutionClock {
        override def nanoTime(): Long = System.nanoTime()
      }
    } else {
      new HighResolutionClock {
        override def nanoTime(): Long = 0L
      }
    }

    new KeyValueStorageEngine(storeName, storeDir, storePropertiesBuilder.build(), nullSafeStore, rawStore,
      changelogSSP, changelogCollector, keyValueStorageEngineMetrics, batchSize, () => clock.nanoTime())
  }

  def createCachedStore[K, V](storeName: String, registry: MetricsRegistry,
    underlyingStore: KeyValueStore[K, V], cacheSize: Int, batchSize: Int): KeyValueStore[K, V] = {
    // wrap with caching
    val cachedStoreMetrics = new CachedStoreMetrics(storeName, registry)
    new CachedStore(underlyingStore, cacheSize, batchSize, cachedStoreMetrics)
  }
}
