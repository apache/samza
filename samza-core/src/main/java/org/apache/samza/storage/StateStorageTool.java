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

package org.apache.samza.storage;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;

import org.apache.samza.config.MapConfig;
import org.apache.samza.util.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commandline tool to recover the state storage to a specified directory
 */
public class StateStorageTool extends CommandLine {
  private ArgumentAcceptingOptionSpec<String> newPathArgu = parser().accepts("path", "path of the new state storage").withRequiredArg().ofType(String.class).describedAs("path");
  private String newPath = "";
  private Logger log = LoggerFactory.getLogger(StateStorageTool.class);

  @Override
  public MapConfig loadConfig(OptionSet options) {
    MapConfig config = super.loadConfig(options);
    if (options.has(newPathArgu)) {
      newPath = options.valueOf(newPathArgu);
      log.info("new state storage is " + newPath);
    }
    return config;
  }

  public String getPath() {
    return newPath;
  }

  public static void main(String[] args) {
    StateStorageTool tool = new StateStorageTool();
    OptionSet options = tool.parser().parse(args);
    MapConfig config = tool.loadConfig(options);
    String path = tool.getPath();

    StorageRecovery storageRecovery = new StorageRecovery(config, path);
    storageRecovery.run();
  }
}
