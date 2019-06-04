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
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSerializerConfig;
import org.apache.samza.config.SerializerConfig$;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.storage.StorageEngineFactory;
import org.apache.samza.util.Util;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to read the RocksDb according to the provided directory
 * position
 */
public class RocksDbKeyValueReader {
  private static final Logger log = LoggerFactory.getLogger(RocksDbKeyValueReader.class);
  private RocksDB db;
  private Serde<Object> keySerde;
  private Serde<Object> valueSerde;

  /**
   * Construct the <code>RocksDbKeyValueReader</code> with store's name,
   * database's path and Samza's config
   *
   * @param storeName name of the RocksDb defined in the config file
   * @param dbPath path to the db directory
   * @param config Samza's config
   */
  public RocksDbKeyValueReader(String storeName, String dbPath, Config config) {
    // get the key serde and value serde from the config
    StorageConfig storageConfig = new StorageConfig(config);
    JavaSerializerConfig serializerConfig = new JavaSerializerConfig(config);

    keySerde = getSerdeFromName(storageConfig.getStorageKeySerde(storeName).orElse(null), serializerConfig);
    valueSerde = getSerdeFromName(storageConfig.getStorageMsgSerde(storeName).orElse(null), serializerConfig);

    // get db options
    Options options = RocksDbOptionsHelper.options(config, 1, new File(dbPath), StorageEngineFactory.StoreMode.ReadWrite);

    // open the db
    RocksDB.loadLibrary();
    try {
      db = RocksDB.openReadOnly(options, dbPath);
    } catch (RocksDBException e) {
      throw new SamzaException("can not open the rocksDb in " + dbPath, e);
    }
  }

  /**
   * get the value from the key. This key will be serialized to bytes using the
   * serde defined in <i>systems.system-name.samza.key.serde</i>. The result
   * will be deserialized back to the object using the serde in
   * <i>systems.system-name.samza.msg.serde</i>. If the value does not exist in
   * the db, it return null.
   *
   * @param key the key of the value you want to get
   * @return deserialized value for the key
   *         Returns null, if the value doesn't exist
   */
  public Object get(Object key) {
    byte[] byteKey = keySerde.toBytes(key);
    byte[] result = null;
    try {
      result = db.get(byteKey);
    } catch (RocksDBException e) {
      log.error("can not get the value for key: " + key);
    }

    if (result == null) {
      log.info(key + " does not exist in the rocksDb");
      return null;
    } else {
      return valueSerde.fromBytes(result);
    }
  }

  public void stop() {
    log.debug("closing the db");
    if (db != null) {
      db.close();
    }
    log.info("db is closed.");
  }

  /**
   * A helper method to get the Serde from the serdeName
   *
   * @param name serde name
   * @param serializerConfig serializer config
   * @return a Serde of this serde name
   */
  private Serde<Object> getSerdeFromName(String name, JavaSerializerConfig serializerConfig) {
    String serdeClassName = serializerConfig.getSerdeClass(name);
    if (serdeClassName == null) {
      serdeClassName = SerializerConfig$.MODULE$.getSerdeFactoryName(name);
    }
    return Util.getObj(serdeClassName, SerdeFactory.class).getSerde(name, serializerConfig);
  }
}
