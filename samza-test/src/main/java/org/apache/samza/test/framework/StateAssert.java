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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.storage.kv.RocksDbKeyValueReader;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.table.TableConfigGenerator;
import org.apache.samza.table.TableSpec;

import static org.junit.Assert.*;


/**
 *  Assertion utils on the content of a Table described by
 *  {@link org.apache.samza.operators.TableDescriptor}.
 */
public class StateAssert {

  /**
   * Verifies that the {@code key} is present in the table described by {@code RocksDbTableDescriptor}
   * @param key expected key in the table
   * @param tableDescriptor describes the RocksDb table
   */
  public static void contains(Object key, RocksDbTableDescriptor tableDescriptor) {
    boolean contains = false;
    for(File partition: getPartitionDirectories(tableDescriptor.getTableId())) {
      RocksDbKeyValueReader
          reader = new RocksDbKeyValueReader(tableDescriptor, partition.getAbsolutePath());
      if(reader.get(key) != null) {
        contains = true;
        reader.stop();
        break;
      }
      reader.stop();
    }
    assertTrue(contains);
  }

  /**
   * Fetches the list of values associated with a {@code key} in all the partitions of the table
   * described by {@code tableDescriptor}
   * @param tableDescriptor describes the RocksDb table
   * @param key expected key in the table
   * @return list of values associated with the {@code key}
   */
  public static List get(RocksDbTableDescriptor tableDescriptor, Object key) {
    List values = new ArrayList<>();
    for(File partition: getPartitionDirectories(tableDescriptor.getTableId())) {
      RocksDbKeyValueReader
          reader = new RocksDbKeyValueReader(tableDescriptor, partition.getAbsolutePath());
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
