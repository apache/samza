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

package org.apache.samza.test.framework;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.storage.kv.RocksDbKeyValueReader;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;

import static org.junit.Assert.*;


public class StateAssert {

  public static <M> void contains(String storeName, Config config, M key) {
    config.put(String.format(StorageConfig.FACTORY(), storeName), RocksDbKeyValueStorageEngineFactory.class.getName());
    boolean contains = false;
    for(File partition: getPartitionDirectories(storeName)) {
      RocksDbKeyValueReader
          reader = new RocksDbKeyValueReader(storeName, partition.getAbsolutePath(), config);
      if(reader.get(key) != null) {
        contains = true;
        reader.stop();
        break;
      }
      reader.stop();
    }
    assertTrue(contains);
  }

  public static <M> List get(String storeName, Config config, M key) {
    config.put(String.format(StorageConfig.FACTORY(), storeName), RocksDbKeyValueStorageEngineFactory.class.getName());
    List values = new ArrayList<>();
    for(File partition: getPartitionDirectories(storeName)) {
      RocksDbKeyValueReader
          reader = new RocksDbKeyValueReader(storeName, partition.getAbsolutePath(), config);
      if(reader.get(key) != null)
        values.add(reader.get(key));
      reader.stop();
    }
    return values;
  }

  private static File[] getPartitionDirectories(String store) {
    File currentDirFile = new File(".");
    File storeDir = new File(currentDirFile.getAbsolutePath().replace(".", "state/"+ store +"/"));
    File[] files = storeDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File f) {
        return f.isDirectory();
      }
    });
    return files;
  }
}
