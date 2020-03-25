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

package org.apache.samza.sql.client.interfaces;

import org.apache.samza.sql.client.cli.CliCommand;
import org.apache.samza.sql.client.cli.CliEnvironment;
import org.apache.samza.sql.client.cli.CliShell;
import org.apache.samza.sql.client.exceptions.CommandHandlerException;
import org.jline.terminal.Terminal;


/**
 * Handles commands of certain {@link CommandType}
 */
public interface CommandHandler {
  /**
   * sets-up the member variables
   * @param shell the {@link CliShell} which uses this CommandHandler
   * @param env the Shell's {@link CliEnvironment}
   * @param terminal the {@link Terminal} to print output and messages
   * @param exeContext the {@link ExecutionContext}
   */
  void init(CliShell shell, CliEnvironment env, Terminal terminal, ExecutionContext exeContext);

  /**
   * Attempts to parse the given input string line into a {@link CliCommand} of this handler's {@link CommandType}
   * @param line input line string
   * @return {@link CliCommand} on success, null otherwise
   */
  CliCommand parseLine(String line);

  /**
   * Handles the given command
   * @param command input {@link CliCommand} to handle
   * @return false if command is to quit, or fatal error happened that Shell should not continue running. True o.w.
   * @throws CommandHandlerException if unrecoverable error happened
   */
  boolean handleCommand(CliCommand command) throws CommandHandlerException;

  /**
   * Prints to terminal the help message of the commands this handler handles
   */
  void printHelpMessage();
}