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

import java.io.File;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.storage.StorageEngineFactory;
import org.apache.samza.storage.StoreProperties;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.util.HighResolutionClock;
import org.apache.samza.util.ScalaJavaUtil;


/**
 * This encapsulates all the steps needed to create a key value storage engine.
 * This is meant to be extended by the specific key value store factory implementations which will in turn override the
 * getKVStore method to return a raw key-value store.
 *
 * BaseKeyValueStorageEngineFactory assumes non null keySerde and msgSerde 
 */
public abstract class BaseKeyValueStorageEngineFactory<K, V> implements StorageEngineFactory<K, V> {
  private static final String INMEMORY_KV_STORAGE_ENGINE_FACTORY =
      "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory";
  private static final String WRITE_BATCH_SIZE = "write.batch.size";
  private static final int DEFAULT_WRITE_BATCH_SIZE = 500;
  private static final String OBJECT_CACHE_SIZE = "object.cache.size";
  private static final int DEFAULT_OBJECT_CACHE_SIZE = 1000;

  /**
   * Implement this to return a KeyValueStore instance for the given store name, which will be used as the underlying
   * raw store.
   *
   * @param storeName Name of the store
   * @param storeDir The directory of the store
   * @param registry MetricsRegistry to which to publish store specific metrics.
   * @param jobContext Information about the job in which the task is executing.
   * @param containerContext Information about the container in which the task is executing.
   * @return A raw KeyValueStore instance
   */
  protected abstract KeyValueStore<byte[], byte[]> getKVStore(String storeName,
      File storeDir,
      MetricsRegistry registry,
      JobContext jobContext,
      ContainerContext containerContext,
      StoreMode storeMode);

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
  public StorageEngine getStorageEngine(String storeName,
      File storeDir,
      Serde<K> keySerde,
      Serde<V> msgSerde,
      MessageCollector changelogCollector,
      MetricsRegistry registry,
      SystemStreamPartition changelogSSP,
      JobContext jobContext,
      ContainerContext containerContext,
      StoreMode storeMode) {
    Config storageConfigSubset = jobContext.getConfig().subset("stores." + storeName + ".", true);
    StorageConfig storageConfig = new StorageConfig(jobContext.getConfig());
    Optional<String> storeFactory = storageConfig.getStorageFactoryClassName(storeName);
    StoreProperties.StorePropertiesBuilder storePropertiesBuilder = new StoreProperties.StorePropertiesBuilder();
    if (!storeFactory.isPresent() || StringUtils.isBlank(storeFactory.get())) {
      throw new SamzaException(
          String.format("Store factory not defined for store %s. Cannot proceed with KV store creation!", storeName));
    }
    if (!storeFactory.get().equals(INMEMORY_KV_STORAGE_ENGINE_FACTORY)) {
      storePropertiesBuilder.setPersistedToDisk(true);
    }
    // The store is durable iff it is backed by the task backup manager
    List<String> storeBackupManager = storageConfig.getStoreBackupManagerClassName(storeName);
    storePropertiesBuilder.setIsDurable(!storeBackupManager.isEmpty());

    int batchSize = storageConfigSubset.getInt(WRITE_BATCH_SIZE, DEFAULT_WRITE_BATCH_SIZE);
    int cacheSize = storageConfigSubset.getInt(OBJECT_CACHE_SIZE, Math.max(batchSize, DEFAULT_OBJECT_CACHE_SIZE));
    if (cacheSize > 0 && cacheSize < batchSize) {
      throw new SamzaException(
          String.format("cache.size for store %s cannot be less than batch.size as batched values reside in cache.",
              storeName));
    }
    if (keySerde == null) {
      throw new SamzaException(
          String.format("Must define a key serde when using key value storage for store %s.", storeName));
    }
    if (msgSerde == null) {
      throw new SamzaException(
          String.format("Must define a message serde when using key value storage for store %s.", storeName));
    }

    KeyValueStore<byte[], byte[]> rawStore =
        getKVStore(storeName, storeDir, registry, jobContext, containerContext, storeMode);
    KeyValueStore<byte[], byte[]> maybeLoggedStore = buildMaybeLoggedStore(changelogSSP,
        storeName, registry, storePropertiesBuilder, rawStore, changelogCollector);
    // this also applies serialization and caching layers
    KeyValueStore<K, V> toBeAccessLoggedStore = buildStoreWithLargeMessageHandling(storeName, registry,
        maybeLoggedStore, storageConfig, cacheSize, batchSize, keySerde, msgSerde);
    KeyValueStore<K, V> maybeAccessLoggedStore =
        buildMaybeAccessLoggedStore(storeName, toBeAccessLoggedStore, changelogCollector, changelogSSP, storageConfig,
            keySerde);
    KeyValueStore<K, V> nullSafeStore = new NullSafeKeyValueStore<>(maybeAccessLoggedStore);

    KeyValueStorageEngineMetrics keyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics(storeName, registry);
    HighResolutionClock clock = buildClock(jobContext.getConfig());
    return new KeyValueStorageEngine<>(storeName, storeDir, storePropertiesBuilder.build(), nullSafeStore, rawStore,
        changelogSSP, changelogCollector, keyValueStorageEngineMetrics, batchSize,
        ScalaJavaUtil.toScalaFunction(clock::nanoTime));
  }

  /**
   * Wraps {@code storeToWrap} into a {@link LoggedStore} if {@code changelogSSP} is defined.
   * Otherwise, returns the original {@code storeToWrap}.
   */
  private static KeyValueStore<byte[], byte[]> buildMaybeLoggedStore(SystemStreamPartition changelogSSP,
      String storeName,
      MetricsRegistry registry,
      StoreProperties.StorePropertiesBuilder storePropertiesBuilder,
      KeyValueStore<byte[], byte[]> storeToWrap,
      MessageCollector changelogCollector) {
    if (changelogSSP == null) {
      return storeToWrap;
    } else {
      LoggedStoreMetrics loggedStoreMetrics = new LoggedStoreMetrics(storeName, registry);
      storePropertiesBuilder.setLoggedStore(true);
      return new LoggedStore<>(storeToWrap, changelogSSP, changelogCollector, loggedStoreMetrics);
    }
  }

  /**
   * Wraps {@code storeToWrap} with the proper layers to handle large messages.
   * If "disallow.large.messages" is enabled, then the message will be serialized and the size will be checked before
   * storing in the serialized message in the cache.
   * If "disallow.large.messages" is disabled, then the deserialized message will be stored in the cache. If
   * "drop.large.messages" is enabled, then large messages will not be sent to the logged store.
   */
  private static <T, U> KeyValueStore<T, U> buildStoreWithLargeMessageHandling(String storeName,
      MetricsRegistry registry,
      KeyValueStore<byte[], byte[]> storeToWrap,
      StorageConfig storageConfig,
      int cacheSize,
      int batchSize,
      Serde<T> keySerde,
      Serde<U> msgSerde) {
    int maxMessageSize = storageConfig.getChangelogMaxMsgSizeBytes(storeName);
    if (storageConfig.getDisallowLargeMessages(storeName)) {
      /*
       * The store wrapping ordering is done this way so that a large message cannot end up in the cache. However, it
       * also means that serialized data is in the cache, so performance will be worse since the data needs to be
       * deserialized even when cached.
       */
      KeyValueStore<byte[], byte[]> maybeCachedStore =
          buildMaybeCachedStore(storeName, registry, storeToWrap, cacheSize, batchSize);
      // this will throw a RecordTooLargeException when a large message is encountered
      LargeMessageSafeStore largeMessageSafeKeyValueStore =
          new LargeMessageSafeStore(maybeCachedStore, storeName, false, maxMessageSize);
      return buildSerializedStore(storeName, registry, largeMessageSafeKeyValueStore, keySerde, msgSerde);
    } else {
      KeyValueStore<byte[], byte[]> toBeSerializedStore;
      if (storageConfig.getDropLargeMessages(storeName)) {
        toBeSerializedStore = new LargeMessageSafeStore(storeToWrap, storeName, true, maxMessageSize);
      } else {
        toBeSerializedStore = storeToWrap;
      }
      KeyValueStore<T, U> serializedStore =
          buildSerializedStore(storeName, registry, toBeSerializedStore, keySerde, msgSerde);
      /*
       * Allows deserialized entries to be stored in the cache, but it means that a large message may end up in the
       * cache even though it was not persisted to the logged store.
       */
      return buildMaybeCachedStore(storeName, registry, serializedStore, cacheSize, batchSize);
    }
  }

  /**
   * Wraps {@code storeToWrap} with a {@link CachedStore} if caching is enabled.
   * Otherwise, returns the {@code storeToWrap}.
   */
  private static <T, U> KeyValueStore<T, U> buildMaybeCachedStore(String storeName, MetricsRegistry registry,
      KeyValueStore<T, U> storeToWrap, int cacheSize, int batchSize) {
    if (cacheSize > 0) {
      CachedStoreMetrics cachedStoreMetrics = new CachedStoreMetrics(storeName, registry);
      return new CachedStore<>(storeToWrap, cacheSize, batchSize, cachedStoreMetrics);
    } else {
      return storeToWrap;
    }
  }

  /**
   * Wraps {@code storeToWrap} with a {@link SerializedKeyValueStore}.
   */
  private static <T, U> KeyValueStore<T, U> buildSerializedStore(String storeName,
      MetricsRegistry registry,
      KeyValueStore<byte[], byte[]> storeToWrap,
      Serde<T> keySerde,
      Serde<U> msgSerde) {
    SerializedKeyValueStoreMetrics serializedMetrics = new SerializedKeyValueStoreMetrics(storeName, registry);
    return new SerializedKeyValueStore<>(storeToWrap, keySerde, msgSerde, serializedMetrics);
  }

  /**
   * Wraps {@code storeToWrap} with an {@link AccessLoggedStore} if enabled.
   * Otherwise, returns the {@code storeToWrap}.
   */
  private static <T, U> KeyValueStore<T, U> buildMaybeAccessLoggedStore(String storeName,
      KeyValueStore<T, U> storeToWrap,
      MessageCollector changelogCollector,
      SystemStreamPartition changelogSSP,
      StorageConfig storageConfig,
      Serde<T> keySerde) {
    if (storageConfig.getAccessLogEnabled(storeName)) {
      return new AccessLoggedStore<>(storeToWrap, changelogCollector, changelogSSP, storageConfig, storeName, keySerde);
    } else {
      return storeToWrap;
    }
  }

  /**
   * If "metrics.timer.enabled" is enabled, then returns a {@link HighResolutionClock} that uses
   * {@link System#nanoTime}.
   * Otherwise, returns a clock which always returns 0.
   */
  private static HighResolutionClock buildClock(Config config) {
    MetricsConfig metricsConfig = new MetricsConfig(config);
    if (metricsConfig.getMetricsTimerEnabled()) {
      return System::nanoTime;
    } else {
      return () -> 0;
    }
  }
}
