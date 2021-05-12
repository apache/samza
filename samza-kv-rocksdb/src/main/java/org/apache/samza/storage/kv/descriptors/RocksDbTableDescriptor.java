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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.storage.kv.LocalTableProviderFactory;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.apache.samza.table.descriptors.LocalTableDescriptor;


/**
 * Table descriptor for RocksDb backed tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class RocksDbTableDescriptor<K, V> extends LocalTableDescriptor<K, V, RocksDbTableDescriptor<K, V>> {

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
  static final public String ROCKSDB_MAX_OPEN_FILES = "rocksdb.max.open.files";
  static final public String ROCKSDB_MAX_FILE_OPENING_THREADS = "rocksdb.max.file.opening.threads";


  private Integer writeBatchSize;
  private Integer objectCacheSize;
  private Integer cacheSize;
  private Integer writeBufferSize;
  private Integer blockSize;
  private Long ttl;
  private Integer numWriteBuffers;
  private Integer maxLogFileSize;
  private Integer numLogFilesToKeep;
  private String compressionType;
  private String compactionStyle;
  private Integer maxOpenFiles;
  private Integer maxFileOpeningThreads;

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must conform to pattern {@literal [\\d\\w-_]+}
   * @param serde the serde for key and value
   */
  public RocksDbTableDescriptor(String tableId, KVSerde<K, V> serde) {
    super(tableId, serde);
  }

  /**
   * For better write performance, the storage engine buffers writes and applies them to the
   * underlying store in a batch. If the same key is written multiple times in quick succession,
   * this buffer also deduplicates writes to the same key. This property is set to the number
   * of key/value pairs that should be kept in this in-memory buffer, per task instance.
   * The number cannot be greater than {@link #withObjectCacheSize}.
   * <p>
   * Default value is 500.
   * <p>
   * Refer to <code>stores.store-name.write.batch.size</code> in Samza configuration guide
   *
   * @param writeBatchSize write batch size
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withWriteBatchSize(int writeBatchSize) {
    this.writeBatchSize = writeBatchSize;
    return this;
  }

  /**
   * Samza maintains an additional cache in front of RocksDB for frequently-accessed objects.
   * This cache contains deserialized objects (avoiding the deserialization overhead on cache
   * hits), in contrast to the RocksDB block cache ({@link #withCacheSize}), which caches
   * serialized objects. This property determines the number of objects to keep in Samza's
   * cache, per task instance. This same cache is also used for write buffering
   * (see {@link #withWriteBatchSize}). A value of 0 disables all caching and batching.
   * <p>
   * Default value is 1,000.
   * <p>
   * Refer to <code>stores.store-name.object.cache.size</code> in Samza configuration guide
   *
   * @param objectCacheSize the object cache size
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor withObjectCacheSize(int objectCacheSize) {
    this.objectCacheSize = objectCacheSize;
    return this;
  }

  /**
   * The size of RocksDB's block cache in bytes, per container. If there are several task
   * instances within one container, each is given a proportional share of this cache.
   * Note that this is an off-heap memory allocation, so the container's total memory
   * use is the maximum JVM heap size plus the size of this cache.
   * <p>
   * Default value is 104,857,600.
   * <p>
   * Refer to <code>stores.store-name.container.cache.size.bytes</code> in Samza configuration guide
   *
   * @param cacheSize the cache size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  /**
   * The amount of memory (in bytes) that RocksDB uses for buffering writes before they are
   * written to disk, per container. If there are several task instances within one container,
   * each is given a proportional share of this buffer. This setting also determines the
   * size of RocksDB's segment files.
   * <p>
   * Default value is 33,554,432.
   * <p>
   * Refer to <code>stores.store-name.container.write.buffer.size.bytes</code> in Samza configuration guide
   *
   * @param writeBufferSize the write buffer size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withWriteBufferSize(int writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  /**
   * Controls whether RocksDB should compress data on disk and in the block cache.
   * The following values are valid:
   * <ul>
   *   <li><b>snappy</b> Compress data using the <a href="https://code.google.com/p/snappy/">Snappy</a> codec.
   *   <li><b>bzip2</b> Compress data using the <a href="http://en.wikipedia.org/wiki/Bzip2">bzip2</a> codec.
   *   <li><b>zlib</b> Compress data using the <a href="http://en.wikipedia.org/wiki/Zlib">zlib</a> codec.
   *   <li><b>lz4</b> Compress data using the <a href="https://code.google.com/p/lz4/">lz4</a> codec.
   *   <li><b>lz4hc</b> Compress data using the <a href="https://code.google.com/p/lz4/">lz4hc</a> (high compression) codec.
   *   <li><b>none</b> Do not compress data.
   * </ul>
   * <p>
   * Default value is snappy.
   * <p>
   * Refer to <code>stores.store-name.rocksdb.compression</code> in Samza configuration guide
   *
   * @param compressionType the compression type
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withCompressionType(String compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  /**
   * If compression is enabled, RocksDB groups approximately this many uncompressed
   * bytes into one compressed block. You probably don't need to change this property.
   * <p>
   * Default value is 4,096.
   * <p>
   * Refer to <code>stores.store-name.rocksdb.block.size.bytes</code> in Samza configuration guide
   *
   * @param blockSize the block size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withBlockSize(int blockSize) {
    this.blockSize = blockSize;
    return this;
  }

  /**
   * The time-to-live of the store. Please note it's not a strict TTL limit (removed
   * only after compaction). Please use caution opening a database with and without
   * TTL, as it might corrupt the database. Please make sure to read the
   * <a href="https://github.com/facebook/rocksdb/wiki/Time-to-Live">constraints</a>
   * before using.
   * <p>
   * Refer to <code>stores.store-name.rocksdb.ttl.ms</code> in Samza configuration guide
   *
   * @param ttl the time to live in milliseconds
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withTtl(long ttl) {
    this.ttl = ttl;
    return this;
  }

  /**
   * This property controls the compaction style that RocksDB will employ when compacting
   * its levels. The following values are valid:
   * <ul>
   *   <li><b>universal</b> Use <a href="https://github.com/facebook/rocksdb/wiki/Universal-Compaction">universal</a> compaction.
   *   <li><b>fifo</b> Use <a href="https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style">FIFO</a> compaction.
   *   <li><b>level</b> Use RocksDB's standard leveled compaction.
   * </ul>
   * <p>
   * Default value is universal.
   * <p>
   * Refer to <code>stores.store-name.rocksdb.compaction.style</code> in Samza configuration guide
   *
   * @param compactionStyle the compaction style
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withCompactionStyle(String compactionStyle) {
    this.compactionStyle = compactionStyle;
    return this;
  }

  /**
   * Configures the
   * <a href="https://github.com/facebook/rocksdb/wiki/Basic-Operations#write-buffer">
   * number of write buffers</a> that a RocksDB store uses. This allows RocksDB
   * to continue taking writes to other buffers even while a given write buffer is being
   * flushed to disk.
   * <p>
   * Default value is 3.
   * <p>
   * Refer to <code>stores.store-name.rocksdb.num.write.buffers</code> in Samza configuration guide
   *
   * @param numWriteBuffers the number of write buffers
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withNumWriteBuffers(int numWriteBuffers) {
    this.numWriteBuffers = numWriteBuffers;
    return this;
  }

  /**
   * The maximum size in bytes of the RocksDB LOG file before it is rotated.
   * <p>
   * Default value is 67,108,864.
   * <p>
   * Refer to <code>stores.store-name.rocksdb.max.log.file.size.bytes</code> in Samza configuration guide
   *
   * @param maxLogFileSize the maximal log file size in bytes
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withMaxLogFileSize(int maxLogFileSize) {
    this.maxLogFileSize = maxLogFileSize;
    return this;
  }

  /**
   * The number of RocksDB LOG files (including rotated LOG.old.* files) to keep.
   * <p>
   * Default value is 2.
   * <p>
   * Refer to <code>stores.store-name.rocksdb.keep.log.file.num</code> in Samza configuration guide
   *
   * @param numLogFilesToKeep the number of log files to keep
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withNumLogFilesToKeep(int numLogFilesToKeep) {
    this.numLogFilesToKeep = numLogFilesToKeep;
    return this;
  }

  /**
   * Limits the number of open files that can be used by the DB.  You may need to increase this if your database
   * has a large working set. Value -1 means files opened are always kept open.
   * <p>
   *  Default value is -1.
   * <p>
   * @param maxOpenFiles the number of open files that can be used by the DB.
   * @return this table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withMaxOpenFiles(int maxOpenFiles) {
    this.maxOpenFiles = maxOpenFiles;
    return this;
  }

  /**
   * Sets the number of threads used to open DB files.
   * If max_open_files is -1, DB will open all files on DB::Open(). You can use this option to increase the number of
   * threads used to open files.
   * <p>
   * Default is 16.
   * <p>
   * @param maxFileOpeningThreads The number of threads to use when opening DB files.
   * @return the table descriptor instance
   */
  public RocksDbTableDescriptor<K, V> withMaxFileOpeningThreads(int maxFileOpeningThreads) {
    this.maxFileOpeningThreads = maxFileOpeningThreads;
    return this;
  }

  @Override
  public String getProviderFactoryClassName() {
    return LocalTableProviderFactory.class.getName();
  }

  @Override
  public Map<String, String> toConfig(Config jobConfig) {

    Map<String, String> tableConfig = new HashMap<>(super.toConfig(jobConfig));

    // Store factory configuration
    tableConfig.put(String.format(StorageConfig.FACTORY, tableId),
        RocksDbKeyValueStorageEngineFactory.class.getName());

    if (writeBatchSize != null) {
      addStoreConfig(WRITE_BATCH_SIZE, writeBatchSize.toString(), tableConfig);
    }
    if (objectCacheSize != null) {
      addStoreConfig(OBJECT_CACHE_SIZE, objectCacheSize.toString(), tableConfig);
    }
    if (cacheSize != null) {
      addStoreConfig(CONTAINER_CACHE_SIZE_BYTES, cacheSize.toString(), tableConfig);
    }
    if (writeBufferSize != null) {
      addStoreConfig(CONTAINER_WRITE_BUFFER_SIZE_BYTES, writeBufferSize.toString(), tableConfig);
    }
    if (compressionType != null) {
      addStoreConfig(ROCKSDB_COMPRESSION, compressionType, tableConfig);
    }
    if (blockSize != null) {
      addStoreConfig(ROCKSDB_BLOCK_SIZE_BYTES, blockSize.toString(), tableConfig);
    }
    if (ttl != null) {
      addStoreConfig(ROCKSDB_TTL_MS, ttl.toString(), tableConfig);
    }
    if (compactionStyle != null) {
      addStoreConfig(ROCKSDB_COMPACTION_STYLE, compactionStyle, tableConfig);
    }
    if (numWriteBuffers != null) {
      addStoreConfig(ROCKSDB_NUM_WRITE_BUFFERS, numWriteBuffers.toString(), tableConfig);
    }
    if (maxLogFileSize != null) {
      addStoreConfig(ROCKSDB_MAX_LOG_FILE_SIZE_BYTES, maxLogFileSize.toString(), tableConfig);
    }
    if (numLogFilesToKeep != null) {
      addStoreConfig(ROCKSDB_KEEP_LOG_FILE_NUM, numLogFilesToKeep.toString(), tableConfig);
    }
    if (maxOpenFiles != null) {
      addStoreConfig(ROCKSDB_MAX_OPEN_FILES, maxOpenFiles.toString(), tableConfig);
    }
    if (maxFileOpeningThreads != null) {
      addStoreConfig(ROCKSDB_MAX_FILE_OPENING_THREADS, maxFileOpeningThreads.toString(), tableConfig);
    }

    return Collections.unmodifiableMap(tableConfig);
  }
}
