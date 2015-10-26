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

import java.util.List;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;

import org.apache.samza.config.MapConfig;
import org.apache.samza.util.CommandLine;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commandline tool to get the value of a given key from the RocksDb.
 */
public class RocksDbReadingTool extends CommandLine {
  private ArgumentAcceptingOptionSpec<String> dbPathArgument = parser()
      .accepts("db-path", "path of RocksDb location")
      .withRequiredArg()
      .ofType(String.class)
      .describedAs("db-path");

  private ArgumentAcceptingOptionSpec<String> dbNameArgument = parser()
      .accepts("db-name", "name of the db")
      .withRequiredArg()
      .ofType(String.class)
      .describedAs("db-name");

  private ArgumentAcceptingOptionSpec<Long> longKeyArgu = parser()
      .accepts("long-key", "a list of long keys. Sperated by ','.")
      .withOptionalArg()
      .ofType(Long.class)
      .describedAs("long-key")
      .withValuesSeparatedBy( ',' );

  private ArgumentAcceptingOptionSpec<String> stringKeyArgu = parser()
      .accepts("string-key", "a list of string keys. Sperated by ','.")
      .withOptionalArg()
      .ofType(String.class)
      .describedAs("string-key")
      .withValuesSeparatedBy( ',' );

  private ArgumentAcceptingOptionSpec<Integer> integerKeyArgu = parser()
      .accepts("integer-key", "a list of integer keys. Sperated by ','.")
      .withOptionalArg()
      .ofType(Integer.class)
      .describedAs("integer-key")
      .withValuesSeparatedBy( ',' );

  private String dbPath = "";
  private String dbName = "";
  private Object key = null;
  private Logger log = LoggerFactory.getLogger(RocksDbReadingTool.class);

  @Override
  public MapConfig loadConfig(OptionSet options) {
    MapConfig config = super.loadConfig(options);
    // get the db name
    if (options.has(dbNameArgument)) {
      dbName = options.valueOf(dbNameArgument);
    } else {
      log.error("Please specify DB Name using --db-name");
      System.exit(-1);
    }
    // get the db location
    if (options.has(dbPathArgument)) {
      dbPath = options.valueOf(dbPathArgument);
    } else {
      log.error("Please specify DB path using --db-path");
      System.exit(-1);
    }
    log.debug("Will read the RocksDb store " + dbName + " in " + dbPath);

    // get the key value
    int keyTypeOptions = 0;
    if (options.has(integerKeyArgu)) {
      key = options.valuesOf(integerKeyArgu);
      keyTypeOptions++;
    }
    if (options.has(longKeyArgu)) {
      key = options.valuesOf(longKeyArgu);
      keyTypeOptions++;
    }
    if (options.has(stringKeyArgu)) {
      key = options.valuesOf(stringKeyArgu);
      keyTypeOptions++;
    }

    if (keyTypeOptions > 1) {
      log.error("Found more than 1 type of key. Please specify only one type of key to use.");
      System.exit(-1);
    }

    if (key == null) {
      log.error("Can not find the key. Please specify the type of key to use");
      System.exit(-1);
    }
    return config;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getDbName() {
    return dbName;
  }

  @SuppressWarnings("unchecked")
  public List<Object> getKeys() {
    return (List<Object>) key;
  }

  public void outputResult(Object key, Object value) {
    System.out.println("key=" + key + "," + "value=" + value);
  }

  public static void main(String[] args) throws RocksDBException {
    RocksDbReadingTool tool = new RocksDbReadingTool();
    OptionSet options = tool.parser().parse(args);
    MapConfig config = tool.loadConfig(options);
    String path = tool.getDbPath();
    String dbName = tool.getDbName();
    RocksDbKeyValueReader kvReader = new RocksDbKeyValueReader(dbName, path, config);

    for (Object obj : tool.getKeys()) {
      Object result = kvReader.get(obj);
      tool.outputResult(obj, result);
    }

    kvReader.stop();
  }
}
