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

import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.table.TableSpec;
import org.junit.Test;

import junit.framework.Assert;


public class TestRocksDbTableDescriptor {

  @Test
  public void testMinimal() {
    new RocksDbTableDescriptor<Integer, String>("1")
        .validate();
  }

  @Test
  public void testSerde() {
    TableSpec tableSpec = new RocksDbTableDescriptor<Integer, String>("1")
        .withSerde(KVSerde.of(new IntegerSerde(), new StringSerde()))
        .getTableSpec();
    Assert.assertNotNull(tableSpec.getSerde());
    Assert.assertEquals(tableSpec.getSerde().getKeySerde().getClass(), IntegerSerde.class);
    Assert.assertEquals(tableSpec.getSerde().getValueSerde().getClass(), StringSerde.class);
  }

  @Test
  public void testTableSpec() {

    TableSpec tableSpec = new RocksDbTableDescriptor<Integer, String>("1")
        .withSerde(KVSerde.of(new IntegerSerde(), new StringSerde()))
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
        .withConfig("rocksdb.abc", "xyz")
        .getTableSpec();

    Assert.assertNotNull(tableSpec.getSerde());
    Assert.assertNotNull(tableSpec.getSerde().getKeySerde());
    Assert.assertNotNull(tableSpec.getSerde().getValueSerde());
    Assert.assertEquals("1", getConfig(tableSpec, RocksDbTableDescriptor.ROCKSDB_BLOCK_SIZE_BYTES));
    Assert.assertEquals("2", getConfig(tableSpec, RocksDbTableDescriptor.CONTAINER_CACHE_SIZE_BYTES));
    Assert.assertEquals("3", getConfig(tableSpec, RocksDbTableDescriptor.ROCKSDB_MAX_LOG_FILE_SIZE_BYTES));
    Assert.assertEquals("4", getConfig(tableSpec, RocksDbTableDescriptor.ROCKSDB_KEEP_LOG_FILE_NUM));
    Assert.assertEquals("5", getConfig(tableSpec, RocksDbTableDescriptor.ROCKSDB_NUM_WRITE_BUFFERS));
    Assert.assertEquals("6", getConfig(tableSpec, RocksDbTableDescriptor.OBJECT_CACHE_SIZE));
    Assert.assertEquals("7", getConfig(tableSpec, RocksDbTableDescriptor.ROCKSDB_TTL_MS));
    Assert.assertEquals("8", getConfig(tableSpec, RocksDbTableDescriptor.WRITE_BATCH_SIZE));
    Assert.assertEquals("9", getConfig(tableSpec, RocksDbTableDescriptor.CONTAINER_WRITE_BUFFER_SIZE_BYTES));
    Assert.assertEquals("snappy", getConfig(tableSpec, RocksDbTableDescriptor.ROCKSDB_COMPRESSION));
    Assert.assertEquals("fifo", getConfig(tableSpec, RocksDbTableDescriptor.ROCKSDB_COMPACTION_STYLE));
    Assert.assertEquals("xyz", getConfig(tableSpec, "abc"));
  }

  private String getConfig(TableSpec tableSpec, String key) {
    return tableSpec.getConfig().get("rocksdb." + key);
  }
}
