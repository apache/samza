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

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.sql.client.exceptions.CommandHandlerException;
import org.apache.samza.sql.client.interfaces.CommandHandler;
import org.apache.samza.sql.client.interfaces.EnvironmentVariableHandler;
import org.apache.samza.sql.client.interfaces.EnvironmentVariableSpecs;
import org.apache.samza.sql.client.exceptions.ExecutorException;
import org.apache.samza.sql.client.interfaces.SqlExecutor;
import org.apache.samza.sql.client.exceptions.CliException;
import org.apache.samza.sql.client.util.CliUtil;
import org.apache.samza.sql.client.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CliEnvironment handles "environment variables" that configures the shell behavior.
 */
public class CliEnvironment {
  private EnvironmentVariableHandler shellEnvHandler;
  private EnvironmentVariableHandler executorEnvHandler;
  private SqlExecutor executor;
  private List<CommandHandler> commandHandlers;
  private Map<String, String> delayedExecutorVars;

  // shell.executor is special and is specifically handled by CliEnvironment
  private String activeExecutorClassName;
  private static final Logger LOG = LoggerFactory.getLogger(CliEnvironment.class);

  CliEnvironment() {
    shellEnvHandler = new CliShellEnvironmentVariableHandler();
    commandHandlers = new ArrayList<>();
  }

  /** Sets the value of an environment variable.
   * @param name Environment variable name
   * @param value Value of the environment variable
   * @return 0 : succeed
   * @throws ExecutorException When user sets an executor but not being able to create
   * @throws CommandHandlerException When user sets a CommandHandler but was not created successfully
   * -1: invalid name
   * -2: invalid value
   */
  public int setEnvironmentVariable(String name, String value) throws ExecutorException, CommandHandlerException {
    name = name.toLowerCase();
    if(name.equals(CliConstants.CONFIG_EXECUTOR)) {
      createShellExecutor(value);
      activeExecutorClassName = value;
      executorEnvHandler = executor.getEnvironmentVariableHandler();
      return 0;
    }

    if (name.equals(CliConstants.CONFIG_COMMAND_HANDLER)) {
      List<String> commandHandlersNames = Arrays.asList(value.split(","));
      for (String commandHandlerName : commandHandlersNames) {
        createCommandHandler(commandHandlerName.trim());
      }
      return 0;
    }

    EnvironmentVariableHandler handler = getAppropriateHandler(name);
    if(handler == null) {
      // Shell doesn't recognize this variable. There's no executor handler yet. Save for future executor
      if(delayedExecutorVars == null) {
        delayedExecutorVars = new HashMap<>();
      }
      delayedExecutorVars.put(name, value);
      return 0;
    }

    return handler.setEnvironmentVariable(name, value);
  }

  /**
   * Gets the value of an environment variable.
   * @param name environment variable name
   * @return value of the environment variable. Returns null if the variable is not set.
   */
  public String getEnvironmentVariable(String name) {
    if(name.equalsIgnoreCase(CliConstants.CONFIG_EXECUTOR)) {
      return activeExecutorClassName;
    }

    EnvironmentVariableHandler handler = getAppropriateHandler(name);
    if(handler == null)
      return null;

    return handler.getEnvironmentVariable(name);
  }

  /**
   * Gets all possible valid values of an environment variable.
   * @param name environment variable name
   * @return An array of all possible valid values of the environment variable. Returns null
   * if the environment variable name given is invalid.
   */
  public String[] getPossibleValues(String name)
  {
    EnvironmentVariableHandler handler = getAppropriateHandler(name);
    if(handler == null)
      return null;

    EnvironmentVariableSpecs.Spec spec = handler.getSpecs().getSpec(name);
    return spec == null ? null : spec.getPossibleValues();
  }

  /**
   * Prints all the environment variables and their values, including both those of Shell
   * and of the Executor.
   * @param writer A writer to print to
   */
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
  public void finishInitialization() throws CliException {
    if(executor == null) {
      try {
        createShellExecutor(CliConstants.DEFAULT_EXECUTOR_CLASS);
        activeExecutorClassName = CliConstants.DEFAULT_EXECUTOR_CLASS;
        executorEnvHandler = executor.getEnvironmentVariableHandler();
        if (delayedExecutorVars != null) {
          for (Map.Entry<String, String> entry : delayedExecutorVars.entrySet()) {
            setEnvironmentVariable(entry.getKey(), entry.getValue());
          }
          delayedExecutorVars = null;
        }
      } catch (ExecutorException | CommandHandlerException e) {
        // Convert checked exception ExecutorException to an unchecked exception as
        // we have failed to create even the default executor thus not recoverable
        throw new CliException(e);
      }
    }

    finishInitialization(shellEnvHandler);
    finishInitialization(executorEnvHandler);
  }

  /*
   * Sets any environment variable that has not be set by configuration file or user to the default value
   */
  private void finishInitialization(EnvironmentVariableHandler handler) {
    List<Pair<String, EnvironmentVariableSpecs.Spec>> list = handler.getSpecs().getAllSpecs();
    for(Pair<String, EnvironmentVariableSpecs.Spec> pair : list) {
      String name = pair.getL();
      EnvironmentVariableSpecs.Spec spec = pair.getR();
      if(CliUtil.isNullOrEmpty(handler.getEnvironmentVariable(name))) {
        handler.setEnvironmentVariable(name, spec.getDefaultValue());
      }
    }
  }

  /**
   * retrieves the {@link SqlExecutor} setup in this environment
   * @return {@link SqlExecutor}
   */
  public SqlExecutor getExecutor() {
    return executor;
  }

  /**
   * retrieves the list of {@link CommandHandler}s setup in this environment
   * @return List of {@link CommandHandler}
   */
  public List<CommandHandler> getCommandHandlers() { return commandHandlers; }

  private void createShellExecutor(String executorClassName) throws ExecutorException {
    try {
      executor = (SqlExecutor) createInstance(executorClassName);
    } catch (ClassCastException e) {
      String errMsg = String.format("Error trying to cast Object of class %s to SqlExecutor", executorClassName);
      LOG.error(errMsg);
      throw new ExecutorException(errMsg, e);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new ExecutorException(e);
    }
  }

  private void createCommandHandler(String handlerClassName) throws CommandHandlerException {
    try {
    commandHandlers.add((CommandHandler) createInstance(handlerClassName));
    } catch (ClassCastException e) {
      String errMsg = String.format("Error trying to cast Object of class %s to CommandHandler", handlerClassName);
      LOG.error(errMsg);
      throw new CommandHandlerException(errMsg, e);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new CommandHandlerException(e);
    }
  }

  private Object createInstance(String className) throws Exception {
    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> ctor = clazz.getConstructor();
      return ctor.newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException
            | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new ExecutorException(e);
    }
  }

  /*
   * Gets the corresponding EnvironmentVariableHandler by the environment variable name.
   * Names starting CliConstants.CONFIG_SHELL_PREFIX are supposed by the Shell itself, otherwise by
   * the executor.
   */
  private EnvironmentVariableHandler getAppropriateHandler(String name) {
    if (name.startsWith(CliConstants.CONFIG_SHELL_PREFIX)) {
      return shellEnvHandler;
    } else {
      return executorEnvHandler;
    }
  }

  /*
   * Helper function for printing all environment variables and their values of a handler to a writer.
   */
  private void printAll(PrintWriter writer, EnvironmentVariableHandler handler) {
    List<Pair<String, String>> shellEnvs = handler.getAllEnvironmentVariables();
    for(Pair<String, String> pair : shellEnvs) {
      printVariable(writer, pair.getL(), pair.getR());
    }
  }

  /*
   * Prints "name = value\n" to a writer.
   */
  public static void printVariable(PrintWriter writer, String name, String value) {
    writer.print(name);
    writer.print(" = ");
    writer.print(value);
    writer.print('\n');
  }
}