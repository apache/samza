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

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.table.TableSpec;


/**
 * Table descriptor for RocksDb backed tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class RocksDbTableDescriptor<K, V> extends BaseLocalStoreBackedTableDescriptor<K, V, RocksDbTableDescriptor<K, V>> {

  static final public String WRITE_BATCH_SIZE = "write.batch.size";
  static final public String OBJECT_CACHE_SIZE = "object.cache.size";
  static final public String CONTAINER_CACHE_SIZE_BYTES = "container.cache.size.bytes";
  static final public String CONTAINER_WRITE_BUFFER_SIZE_BYTES = "container.write.buffer.size.bytes";
  static final public String ROCKSDB_COMPRESSION = "rocksdb.compression";
  static final public String ROCKSDB_BLOCK_SIZE_BYTES = "rocksdb.block.size.bytes";
  static final public String ROCKSDB_TTL_MS = "rocksdb.ttl.ms";
  static final public String ROCKSDB_COMPACTION_STYLE = "rocksdb.compaction.style";
  static final public String ROCKSDB_NUM_WRITE_BUFFERS = "rocksdb.num.write.buffers";
  static final public String ROCKSDB_MAX_LOG_FILE_SIZE_BYTES = "rocksdb.max.log.file.size.bytes";
  static final public String ROCKSDB_KEEP_LOG_FILE_NUM = "rocksdb.keep.log.file.num";

  protected Integer writeBatchSize;
  protected Integer objectCacheSize;
  private Integer cacheSize;
  private Integer writeBufferSize;
  private Integer blockSize;
  private Integer ttl;
  private Integer numWriteBuffers;
  private Integer maxLogFileSize;
  private Integer numLogFilesToKeep;
  private String compressionType;
  private String compactionStyle;

  public RocksDbTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Refer to <code>stores.store-name.write.batch.size</code> in Samza configuration guide
   * @param writeBatchSize write batch size
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withWriteBatchSize(int writeBatchSize) {
    this.writeBatchSize = writeBatchSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.object.cache.size</code> in Samza configuration guide
   * @param objectCacheSize the object cache size
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withObjectCacheSize(int objectCacheSize) {
    this.objectCacheSize = objectCacheSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.container.cache.size.bytes</code> in Samza configuration guide
   * @param cacheSize the cache size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.container.write.buffer.size.bytes</code> in Samza configuration guide
   * @param writeBufferSize the write buffer size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withWriteBufferSize(int writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.compression</code> in Samza configuration guide
   * @param compressionType the compression type
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withCompressionType(String compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.block.size.bytes</code> in Samza configuration guide
   * @param blockSize the block size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withBlockSize(int blockSize) {
    this.blockSize = blockSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.ttl.ms</code> in Samza configuration guide
   * @param ttl the time to live in milliseconds
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withTtl(int ttl) {
    this.ttl = ttl;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.compaction.style</code> in Samza configuration guide
   * @param compactionStyle the compaction style
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withCompactionStyle(String compactionStyle) {
    this.compactionStyle = compactionStyle;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.num.write.buffers</code> in Samza configuration guide
   * @param numWriteBuffers the number of write buffers
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withNumWriteBuffers(int numWriteBuffers) {
    this.numWriteBuffers = numWriteBuffers;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.max.log.file.size.bytes</code> in Samza configuration guide
   * @param maxLogFileSize the maximal log file size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withMaxLogFileSize(int maxLogFileSize) {
    this.maxLogFileSize = maxLogFileSize;
    return this;
  }

  /**
   * Refer to <code>stores.store-name.rocksdb.num.write.buffers</code> in Samza configuration guide
   * @param numLogFilesToKeep the number of log files to keep
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withNumLogFilesToKeep(int numLogFilesToKeep) {
    this.numLogFilesToKeep = numLogFilesToKeep;
    return this;
  }

  /**
   * Create a table spec based on this table description
   * @return the table spec
   */
  @Override
  public TableSpec getTableSpec() {

    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    return new TableSpec(tableId, serde, RocksDbTableProviderFactory.class.getName(), tableSpecConfig,
        sideInputs, sideInputProcessor);
  }

  @Override
  protected void generateTableSpecConfig(Map<String, String> tableSpecConfig) {

    super.generateTableSpecConfig(tableSpecConfig);

    if (writeBatchSize != null) {
      addRocksDbConfig(tableSpecConfig, WRITE_BATCH_SIZE, writeBatchSize.toString());
    }
    if (objectCacheSize != null) {
      addRocksDbConfig(tableSpecConfig, OBJECT_CACHE_SIZE, objectCacheSize.toString());
    }
    if (cacheSize != null) {
      addRocksDbConfig(tableSpecConfig, CONTAINER_CACHE_SIZE_BYTES, cacheSize.toString());
    }
    if (writeBufferSize != null) {
      addRocksDbConfig(tableSpecConfig, CONTAINER_WRITE_BUFFER_SIZE_BYTES, writeBufferSize.toString());
    }
    if (compressionType != null) {
      addRocksDbConfig(tableSpecConfig, ROCKSDB_COMPRESSION, compressionType);
    }
    if (blockSize != null) {
      addRocksDbConfig(tableSpecConfig, ROCKSDB_BLOCK_SIZE_BYTES, blockSize.toString());
    }
    if (ttl != null) {
      addRocksDbConfig(tableSpecConfig, ROCKSDB_TTL_MS, ttl.toString());
    }
    if (compactionStyle != null) {
      addRocksDbConfig(tableSpecConfig, ROCKSDB_COMPACTION_STYLE, compactionStyle);
    }
    if (numWriteBuffers != null) {
      addRocksDbConfig(tableSpecConfig, ROCKSDB_NUM_WRITE_BUFFERS, numWriteBuffers.toString());
    }
    if (maxLogFileSize != null) {
      addRocksDbConfig(tableSpecConfig, ROCKSDB_MAX_LOG_FILE_SIZE_BYTES, maxLogFileSize.toString());
    }
    if (numLogFilesToKeep != null) {
      addRocksDbConfig(tableSpecConfig, ROCKSDB_KEEP_LOG_FILE_NUM, numLogFilesToKeep.toString());
    }
  }

  private void addRocksDbConfig(Map<String, String> map, String key, String value) {
    map.put("rocksdb." + key, value);
  }

}
