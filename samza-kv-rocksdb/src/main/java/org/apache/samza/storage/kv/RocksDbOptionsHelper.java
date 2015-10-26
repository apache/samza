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

import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerContext;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class to help construct the <code>Options</code> for RocksDb.
 */
public class RocksDbOptionsHelper {
  private static final Logger log = LoggerFactory.getLogger(RocksDbOptionsHelper.class);

  public static Options options(Config storeConfig, SamzaContainerContext containerContext) {
    Options options = new Options();
    Long writeBufSize = storeConfig.getLong("container.write.buffer.size.bytes", 32 * 1024 * 1024);
    // Cache size and write buffer size are specified on a per-container basis.
    int numTasks = containerContext.taskNames.size();
    options.setWriteBufferSize((int) (writeBufSize / numTasks));

    CompressionType compressionType = CompressionType.SNAPPY_COMPRESSION;
    String compressionInConfig = storeConfig.get("rocksdb.compression", "snappy");
    switch (compressionInConfig) {
    case "snappy":
      compressionType = CompressionType.SNAPPY_COMPRESSION;
      break;
    case "bzip2":
      compressionType = CompressionType.BZLIB2_COMPRESSION;
      break;
    case "zlib":
      compressionType = CompressionType.ZLIB_COMPRESSION;
      break;
    case "lz4":
      compressionType = CompressionType.LZ4_COMPRESSION;
      break;
    case "lz4hc":
      compressionType = CompressionType.LZ4HC_COMPRESSION;
      break;
    case "none":
      compressionType = CompressionType.NO_COMPRESSION;
    default:
      log.warn("Unknown rocksdb.compression codec " + compressionInConfig + ", overwriting to Snappy");
    }
    options.setCompressionType(compressionType);

    Long cacheSize = storeConfig.getLong("container.cache.size.bytes", 100 * 1024 * 1024L);
    Long cacheSizePerContainer = cacheSize / numTasks;
    int blockSize = storeConfig.getInt("rocksdb.block.size.bytes", 4096);
    BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
    tableOptions.setBlockCacheSize(cacheSizePerContainer).setBlockSize(blockSize);
    options.setTableFormatConfig(tableOptions);

    CompactionStyle compactionStyle = CompactionStyle.UNIVERSAL;
    String compactionStyleInConfig = storeConfig.get("rocksdb.compaction.style", "universal");
    switch (compactionStyleInConfig) {
    case "universal":
      compactionStyle = CompactionStyle.UNIVERSAL;
      break;
    case "fifo":
      compactionStyle = CompactionStyle.FIFO;
      break;
    case "level":
      compactionStyle = CompactionStyle.LEVEL;
      break;
    default:
      log.warn("Unknown rocksdb.compactionStyle " + compactionStyleInConfig + ", overwriting to universal");
    }
    options.setCompactionStyle(compactionStyle);

    options.setMaxWriteBufferNumber(storeConfig.getInt("rocksdb.num.write.buffers", 3));
    options.setCreateIfMissing(true);
    options.setErrorIfExists(false);

    return options;
  }
}