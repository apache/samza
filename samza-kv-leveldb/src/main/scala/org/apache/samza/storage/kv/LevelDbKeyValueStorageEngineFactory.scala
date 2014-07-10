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

object LevelDbKeyValueStorageEngineFactory {
  def getKeyValueStore(storeName: String,
                          storeDir: File,
                          registry: MetricsRegistry,
                          changeLogSystemStreamPartition: SystemStreamPartition,
                          containerContext: SamzaContainerContext): KeyValueStore[Array[Byte], Array[Byte]] = {
    val storageConfig = containerContext.config.subset("stores." + storeName + ".", true)
    val deleteCompactionThreshold = storageConfig.getInt("compaction.delete.threshold", -1)

    val levelDbMetrics = new KeyValueStoreMetrics(storeName, registry)
    val levelDbOptions = LevelDbKeyValueStore.options(storageConfig, containerContext)
    val levelDb = new LevelDbKeyValueStore(storeDir, levelDbOptions, deleteCompactionThreshold, levelDbMetrics)

    levelDb
  }

}

class LevelDbKeyValueStorageEngineFactory[K, V] extends BaseKeyValueStorageEngineFactory[K, V] {

  override def getKVStore(storeName: String,
                          storeDir: File,
                          registry: MetricsRegistry,
                          changeLogSystemStreamPartition: SystemStreamPartition,
                          containerContext: SamzaContainerContext): KeyValueStore[Array[Byte], Array[Byte]] = {
    LevelDbKeyValueStorageEngineFactory.getKeyValueStore(storeName, storeDir, registry, changeLogSystemStreamPartition, containerContext)
  }

}
