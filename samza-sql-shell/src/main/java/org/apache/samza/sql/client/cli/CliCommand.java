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

import org.apache.samza.sql.client.interfaces.CommandType;

/**
 * A shell command containing {@link CommandType} and parameters.
 */
public class CliCommand {
  private CommandType commandType;
  private String parameters;

  /**
   * Constructor with empty parameters
   * @param cmdType the given {@link CommandType}
   */
  public CliCommand(CommandType cmdType) {
    this.commandType = cmdType;
  }

  /**
   * Constructor using both {@link CommandType} and parameters
   * @param cmdType the given {@link CommandType}
   * @param parameters the parameters as single String
   */
  public CliCommand(CommandType cmdType, String parameters) {
    this(cmdType);
    this.parameters = parameters;
  }

  /**
   * get the {@link CommandType} of this Command
   * @return {@link CommandType}
   */
  public CommandType getCommandType() {
    return commandType;
  }

  /**
   * @return the parameters of this command
   */
  public String getParameters() {
    return parameters;
  }

  /**
   * Sets the parameters of this command
   * @param parameters input parameters
   */
  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  /**
   * Compose full command (i.e., name + parameters) for this command
   * @return composed full command
   */
  public String getFullCommand() {
    return commandType.getCommandName() + CliConstants.SPACE + parameters;
  }
}