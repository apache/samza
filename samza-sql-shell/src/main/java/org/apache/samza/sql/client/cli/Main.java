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

import org.apache.samza.sql.client.exceptions.CommandHandlerException;
import org.apache.samza.sql.client.exceptions.ExecutorException;
import org.apache.samza.sql.client.util.CliUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry of the program.
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
      // Get configuration file path
      String configFilePath = null;
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

      CliEnvironment environment = new CliEnvironment();
      StringBuilder messageBuilder = new StringBuilder();

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
            try {
              LOG.info("Configuration: setting {} = {}", key, value);
              int result = environment.setEnvironmentVariable(key, value);
              if (result == -1) { // CliEnvironment doesn't recognize the key.
                LOG.warn("Unknowing shell environment variable: {}", key);
              } else if (result == -2) { // Invalid value
                LOG.warn("Unknowing shell environment value: {}", value);
              }
            } catch(ExecutorException e) {
              messageBuilder.append("Warning: Failed to create executor: ").append(value).append('\n');
              messageBuilder.append("Warning: Using default executor " + CliConstants.DEFAULT_EXECUTOR_CLASS);
              LOG.error("Failed to create user specified executor {}", value, e);
            } catch (CommandHandlerException e) {
              messageBuilder.append("Warning: Failed to create CommandHandler: ").append(value).append('\n');
              LOG.error("Failed to create user specified CommandHandler {}", value, e);
            }
          }
        } catch (IOException e) {
          LOG.error("Error in opening and reading the configuration file {}", e.toString());
        }
      }

      environment.finishInitialization();
      CliShell shell;
      try {
        shell = new CliShell(environment);
      } catch (ExecutorException e) {
        System.out.println("Unable to initialize executor. Shell must exit. ");
        LOG.error("Unable to initialize executor.", e);
        return;
      }

      shell.open(messageBuilder.toString());
    }
}

