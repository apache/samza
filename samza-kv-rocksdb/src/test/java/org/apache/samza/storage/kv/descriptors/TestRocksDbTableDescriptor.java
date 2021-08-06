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
package org.apache.samza.storage.kv.descriptors;

import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.LocalTableProviderFactory;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;

import org.junit.Test;
import org.junit.Assert;

public class TestRocksDbTableDescriptor {

  private static final String TABLE_ID = "t1";

  @Test
  public void testMinimal() {
    Map tableConfig = createTableDescriptor()
        .toConfig(createJobConfig());
    Assert.assertNotNull(tableConfig);
    Assert.assertEquals(2, tableConfig.size());
  }

  @Test
  public void testTableProviderFactoryConfig() {
    Map tableConfig = createTableDescriptor()
        .toConfig(createJobConfig());
    Assert.assertEquals(2, tableConfig.size());
    Assert.assertEquals(LocalTableProviderFactory.class.getName(),
        tableConfig.get(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, TABLE_ID)));
    Assert.assertEquals(RocksDbKeyValueStorageEngineFactory.class.getName(),
        tableConfig.get(String.format(StorageConfig.FACTORY, TABLE_ID)));
  }

  @Test
  public void testRocksDbConfig() {

    Map tableConfig = new RocksDbTableDescriptor<Integer, String>(
            TABLE_ID, KVSerde.of(new IntegerSerde(), new StringSerde()))
        .withBlockSize(1)
        .withCacheSize(2)
        .withCompactionStyle("fifo")
        .withCompressionType("snappy")
        .withMaxLogFileSize(3)
        .withNumLogFilesToKeep(4)
        .withNumWriteBuffers(5)
        .withObjectCacheSize(6)
        .withTtl(7)
        .withWriteBatchSize(8)
        .withWriteBufferSize(9)
        .withMaxOpenFiles(10)
        .withMaxFileOpeningThreads(11)
        .withConfig("abc", "xyz")
        .toConfig(createJobConfig());

    Assert.assertEquals(16, tableConfig.size());
    assertEquals("1", RocksDbTableDescriptor.ROCKSDB_BLOCK_SIZE_BYTES, tableConfig);
    assertEquals("2", RocksDbTableDescriptor.CONTAINER_CACHE_SIZE_BYTES, tableConfig);
    assertEquals("3", RocksDbTableDescriptor.ROCKSDB_MAX_LOG_FILE_SIZE_BYTES, tableConfig);
    assertEquals("4", RocksDbTableDescriptor.ROCKSDB_KEEP_LOG_FILE_NUM, tableConfig);
    assertEquals("5", RocksDbTableDescriptor.ROCKSDB_NUM_WRITE_BUFFERS, tableConfig);
    assertEquals("6", RocksDbTableDescriptor.OBJECT_CACHE_SIZE, tableConfig);
    assertEquals("7", RocksDbTableDescriptor.ROCKSDB_TTL_MS, tableConfig);
    assertEquals("8", RocksDbTableDescriptor.WRITE_BATCH_SIZE, tableConfig);
    assertEquals("9", RocksDbTableDescriptor.CONTAINER_WRITE_BUFFER_SIZE_BYTES, tableConfig);
    assertEquals("10", RocksDbTableDescriptor.ROCKSDB_MAX_OPEN_FILES, tableConfig);
    assertEquals("11", RocksDbTableDescriptor.ROCKSDB_MAX_FILE_OPENING_THREADS, tableConfig);
    assertEquals("snappy", RocksDbTableDescriptor.ROCKSDB_COMPRESSION, tableConfig);
    assertEquals("fifo", RocksDbTableDescriptor.ROCKSDB_COMPACTION_STYLE, tableConfig);
    Assert.assertFalse(tableConfig.containsKey(String.format(StorageConfig.CHANGELOG_STREAM, TABLE_ID)));
    Assert.assertFalse(tableConfig.containsKey(String.format(StorageConfig.CHANGELOG_REPLICATION_FACTOR, TABLE_ID)));
    Assert.assertEquals("xyz", tableConfig.get("abc"));
  }

  private void assertEquals(String expectedValue, String key, Map config) {
    String realKey = String.format("stores.%s.%s", TABLE_ID, key);
    Assert.assertEquals(expectedValue, config.get(realKey));
  }

  private Config createJobConfig() {
    return new MapConfig();
  }

  private RocksDbTableDescriptor createTableDescriptor() {
    return new RocksDbTableDescriptor<>(TABLE_ID,
        KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
  }
}
