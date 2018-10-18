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

package org.apache.samza.sql.client.cli;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.sql.client.impl.SamzaExecutor;
import org.apache.samza.sql.client.interfaces.ExecutionContext;
import org.apache.samza.sql.client.interfaces.SqlExecutor;
import org.apache.samza.sql.client.util.CliException;
import org.apache.samza.sql.client.util.CliUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
      // Get configuration file path
  /*    String configFilePath = null;
      for(int i = 0; i < args.length; ++i) {
        switch(args[i]) {
          case "-conf":
            if(i + 1 < args.length) {
              configFilePath = args[i + 1];
              i++;
            }
            break;
          default:
            LOG.warn("Unknown parameter {}", args[i]);
            break;
        }
      }

      SqlExecutor executor = null;
      CliEnvironment environment = new CliEnvironment();
      Map<String, String> executorConfig = new HashMap<>();

      if(!CliUtil.isNullOrEmpty(configFilePath)) {
        LOG.info("Configuration file path is: {}", configFilePath);
        try {
          FileReader fileReader = new FileReader(configFilePath);
          BufferedReader bufferedReader = new BufferedReader(fileReader);
          String line;
          while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("#") || line.startsWith("[")) {
              continue;
            }
            String[] strs = line.split("=");
            if (strs.length != 2) {
              continue;
            }
            String key = strs[0].trim().toLowerCase();
            String value = strs[1].trim();
            if(key.startsWith(CliConstants.CONFIG_SHELL_PREFIX)) {
              if(key.equals(CliConstants.CONFIG_EXECUTOR)) {
                try {
                  Class<?> clazz = Class.forName(value);
                  Constructor<?> ctor = clazz.getConstructor();
                  executor = (SqlExecutor) ctor.newInstance();
                  LOG.info("Sql executor creation succeed. Executor class is: {}", value);
                } catch (ClassNotFoundException | NoSuchMethodException
                    | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                  throw new CliException(String.format("Failed to create executor %s.", value), e);
                }
                continue;
              }

              // Suppose a shell variable.
              int result = environment.setEnvironmentVariable(key, value);
              if(result == -1) { // CliEnvironment doesn't recognize the key.
                LOG.warn("Unknowing shell environment variable: {}", key);
              } else if(result == -2) { // Invalid value
                LOG.warn("Unknowing shell environment value: {}", value);
              }
            } else {
              executorConfig.put(key, value);
            }
          }
        } catch (IOException e) {
          LOG.error("Error in opening and reading the configuration file {}", e.toString());
        }
      }
      if(executor == null) {
        executor = new SamzaExecutor();
      }

      // CliShell shell = new CliShell(executor, environment, new ExecutionContext(executorConfig));
      // shell.open(); */

      SamzaExecutor _executor = new SamzaExecutor();
      _executor.listTables(new ExecutionContext(new HashMap<>()));
      //_executor.executeQuery(null, "insert into kafka.ProfileChangeStream_sink select * from kafka.ProfileChangeStream");
      //_executor.executeQuery(null, "insert into log.outputStream select * from kafka.ProfileChangeStream");
      // _executor.executeQuery(null, "select * from kafka.ProfileChangeStream");
    }
}

