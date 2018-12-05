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

import org.apache.samza.sql.client.interfaces.EnvironmentVariableHandler;
import org.apache.samza.sql.client.interfaces.EnvironmentVariableSpecs;
import org.apache.samza.sql.client.interfaces.SqlExecutor;
import org.apache.samza.sql.client.util.CliException;
import org.apache.samza.sql.client.util.CliUtil;
import org.apache.samza.sql.client.util.Pair;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CliEnvironment handles "environment variables" that configures the shell behavior.
 */
class CliEnvironment {
  private EnvironmentVariableHandler shellEnvHandler;
  private EnvironmentVariableHandler executorEnvHandler;
  private SqlExecutor executor;
  private Map<String, String> delayedExecutorVars;

  // shell.executor is special and is specifically handled by CliEnvironment
  private String activeExecutorClassName;

  public CliEnvironment() {
    shellEnvHandler = new CliShellEnvironmentVariableHandler();
  }

  /**
   * @param envName Environment variable name
   * @param value   Value of the environment variable
   * @return 0 : succeed
   * -1: invalid envName
   * -2: invalid value
   */
  public int setEnvironmentVariable(String envName, String value) {
    envName = envName.toLowerCase();
    if(envName.equals(CliConstants.CONFIG_EXECUTOR)) {
      if(createShellExecutor(value)) {
        activeExecutorClassName = value;
        executorEnvHandler = executor.getEnvironmentVariableHandler();
        return 0;
      } else {
        return -2;
      }
    }

    EnvironmentVariableHandler handler = getAppropriateHandler(envName);
    if(handler == null) {
      // Shell doesn't recognize this variable. There's no executor handler yet. Save for future executor
      if(delayedExecutorVars == null) {
        delayedExecutorVars = new HashMap<>();
      }
      delayedExecutorVars.put(envName, value);
      return 0;
    }

    return handler.setEnvironmentVariable(envName, value);
  }

  public String getEnvironmentVariable(String envName) {
    if(envName.equalsIgnoreCase(CliConstants.CONFIG_EXECUTOR)) {
      return activeExecutorClassName;
    }

    EnvironmentVariableHandler handler = getAppropriateHandler(envName);
    if(handler == null)
      return null;

    return handler.getEnvironmentVariable(envName);
  }

  public String[] getPossibleValues(String envName)
  {
    EnvironmentVariableHandler handler = getAppropriateHandler(envName);
    if(handler == null)
      return null;

    EnvironmentVariableSpecs.Spec spec = handler.getSpecs().getSpec(envName);
    return spec == null ? null : spec.getPossibleValues();
  }

  public void printAll(PrintWriter writer) {
    printVariable(writer, CliConstants.CONFIG_EXECUTOR, activeExecutorClassName);
    printAll(writer, shellEnvHandler);
    if(executorEnvHandler != null) {
      printAll(writer, executorEnvHandler);
    }
  }

  /**
   * Gives CliEnvironment a chance to apply settings, especially during initialization, things like
   * making default values take effect
   */
  public void finishInitialization() {
    if(executor == null) {
      if(createShellExecutor(CliConstants.DEFAULT_EXECUTOR_CLASS)) {
        activeExecutorClassName = CliConstants.DEFAULT_EXECUTOR_CLASS;
        executorEnvHandler = executor.getEnvironmentVariableHandler();
        if(delayedExecutorVars != null) {
          for (Map.Entry<String, String> entry : delayedExecutorVars.entrySet()) {
            setEnvironmentVariable(entry.getKey(), entry.getValue());
          }
          delayedExecutorVars = null;
        }
      } else {
        throw new CliException("Failed to create default executor: " + CliConstants.DEFAULT_EXECUTOR_CLASS);
      }
    }

    finishInitialization(shellEnvHandler);
    finishInitialization(executorEnvHandler);
  }

  private void finishInitialization(EnvironmentVariableHandler handler) {
    List<Pair<String, EnvironmentVariableSpecs.Spec>> list = handler.getSpecs().getAllSpecs();
    for(Pair<String, EnvironmentVariableSpecs.Spec> pair : list) {
      String envName = pair.getL();
      EnvironmentVariableSpecs.Spec spec = pair.getR();
      if(CliUtil.isNullOrEmpty(handler.getEnvironmentVariable(envName))) {
        handler.setEnvironmentVariable(envName, spec.getDefaultValue());
      }
    }
  }

  public SqlExecutor getExecutor() {
    return executor;
  }

  private boolean createShellExecutor(String executorClassName) {
    try {
      Class<?> clazz = Class.forName(executorClassName);
      Constructor<?> ctor = clazz.getConstructor();
      executor = (SqlExecutor) ctor.newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException
            | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      return false;
    }
    return true;
  }

  private EnvironmentVariableHandler getAppropriateHandler(String envName) {
    if (envName.startsWith(CliConstants.CONFIG_SHELL_PREFIX)) {
      return shellEnvHandler;
    } else {
      return executorEnvHandler;
    }
  }

  private void printAll(PrintWriter writer, EnvironmentVariableHandler handler) {
    List<Pair<String, String>> shellEnvs = handler.getAllEnvironmentVariables();
    for(Pair<String, String> pair : shellEnvs) {
      printVariable(writer, pair.getL(), pair.getR());
    }
  }

  public static void printVariable(PrintWriter writer, String envName, String value) {
    writer.print(envName);
    writer.print(" = ");
    writer.print(value);
    writer.print('\n');
  }
}