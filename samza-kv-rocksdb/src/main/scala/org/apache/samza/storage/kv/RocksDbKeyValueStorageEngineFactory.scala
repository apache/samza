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

import org.apache.samza.config.StorageConfig._
import org.apache.samza.context.{ContainerContext, JobContext}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemStreamPartition
import org.rocksdb.{FlushOptions, WriteOptions}

class RocksDbKeyValueStorageEngineFactory [K, V] extends BaseKeyValueStorageEngineFactory[K, V] {
  /**
   * Return a KeyValueStore instance for the given store name
   * @param storeName Name of the store
   * @param storeDir The directory of the store
   * @param registry MetricsRegistry to which to publish store specific metrics.
   * @param changeLogSystemStreamPartition Samza stream partition from which to receive the changelog.
   * @param containerContext Information about the container in which the task is executing.
   * @return A valid KeyValueStore instance
   */
  override def getKVStore(storeName: String,
    storeDir: File,
    registry: MetricsRegistry,
    changeLogSystemStreamPartition: SystemStreamPartition,
    jobContext: JobContext,
    containerContext: ContainerContext): KeyValueStore[Array[Byte], Array[Byte]] = {
    val storageConfig = jobContext.getConfig.subset("stores." + storeName + ".", true)
    val isLoggedStore = jobContext.getConfig.getChangelogStream(storeName).isDefined
    val rocksDbMetrics = new KeyValueStoreMetrics(storeName, registry)
    val numTasksForContainer = containerContext.getContainerModel.getTasks.keySet().size()
    rocksDbMetrics.newGauge("rocksdb.block-cache-size",
      () => RocksDbOptionsHelper.getBlockCacheSize(storageConfig, numTasksForContainer))

    val rocksDbOptions = RocksDbOptionsHelper.options(storageConfig, numTasksForContainer)
    val rocksDbWriteOptions = new WriteOptions().setDisableWAL(true)
    val rocksDbFlushOptions = new FlushOptions().setWaitForFlush(true)
    val rocksDb = new RocksDbKeyValueStore(
      storeDir,
      rocksDbOptions,
      storageConfig,
      isLoggedStore,
      storeName,
      rocksDbWriteOptions,
      rocksDbFlushOptions,
      rocksDbMetrics)
    rocksDb
  }
}
