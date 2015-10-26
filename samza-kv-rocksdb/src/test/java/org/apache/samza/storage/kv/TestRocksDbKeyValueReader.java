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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.apache.samza.serializers.IntegerSerdeFactory;

public class TestRocksDbKeyValueReader {
  private static final String DB_NAME = "testKvStore";
  private static Path dirPath = Paths.get(DB_NAME);
  private static RocksDB db = null;

  @BeforeClass
  static public void createRocksDb() throws IOException, RocksDBException {
    if (Files.exists(dirPath)) {
      removeRecursiveDirectory(dirPath);
    }
    Files.createDirectories(dirPath);
    Options options = new Options().setCreateIfMissing(true);
    db = RocksDB.open(options, dirPath.toString());
    db.put("testString".getBytes(), "this is string".getBytes());
    db.put(ByteBuffer.allocate(4).putInt(123).array(), ByteBuffer.allocate(4).putInt(456).array());
  }

  @AfterClass
  static public void tearDownRocksDb() {
    if (db != null) {
      db.close();
    }
    if (Files.exists(dirPath)) {
      removeRecursiveDirectory(dirPath);
    }
  }

  @Test
  public void testReadCorrectDbValue() throws RocksDBException {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("stores." + DB_NAME + ".factory", "mockFactory");
    map.put("stores." + DB_NAME + ".key.serde", "string");
    map.put("stores." + DB_NAME + ".msg.serde", "string");
    Config config = new MapConfig(map);

    RocksDbKeyValueReader reader = new RocksDbKeyValueReader(DB_NAME, dirPath.toString(), config);
    assertEquals("this is string", reader.get("testString"));

    // should throw exception if the input is in other type
    boolean throwClassCastException = false;
    try {
      reader.get(123);
    } catch (Exception e) {
      if (e instanceof ClassCastException) {
        throwClassCastException = true;
      }
    }
    assertTrue(throwClassCastException);
    reader.stop();

    // test with customized serde
    map.put("serializers.registry.mock.class", IntegerSerdeFactory.class.getCanonicalName());
    map.put("stores." + DB_NAME + ".key.serde", "mock");
    map.put("stores." + DB_NAME + ".msg.serde", "mock");
    config = new MapConfig(map);
    reader = new RocksDbKeyValueReader(DB_NAME, dirPath.toString(), config);
    assertEquals(456, reader.get(123));

    assertNull(reader.get(789));
    reader.stop();
  }

  private static void removeRecursiveDirectory(Path path) {
    try {
      Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          if (exc == null) {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          } else {
            throw exc;
          }
        }
      });
    } catch (IOException e) {
      throw new SamzaException("can not delete " + path.toString(), e);
    }
  }
}
