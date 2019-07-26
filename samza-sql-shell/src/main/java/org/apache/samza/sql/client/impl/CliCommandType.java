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

package org.apache.samza.sql.client.impl;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.sql.client.interfaces.CommandType;

/**
 * Enum all the commands we now support along with descriptions.
 */
public enum CliCommandType implements CommandType {
  SHOW_TABLES("SHOW TABLES", "Shows all available tables.", "Usage: SHOW TABLES", true),
  SHOW_FUNCTIONS("SHOW FUNCTIONS", "Shows all available UDFs.", "SHOW FUNCTION", true),
  DESCRIBE("DESCRIBE", "Describes a table.", "Usage: DESCRIBE <table name>", false),

  SELECT("SELECT", "\tExecutes a SQL SELECT query.", "SELECT uses a standard streaming SQL syntax.", false),
  EXECUTE("EXECUTE", "\tExecute a sql file.", "EXECUTE <URI of a sql file>", false),
  INSERT_INTO("INSERT INTO", "Executes a SQL INSERT INTO.", "INSERT INTO uses a standard streaming SQL syntax.", false),
  LS("LS", "\tLists all background executions.", "LS [execution ID]", true),
  STOP("STOP", "\tStops an execution.", "Usage: STOP <execution ID>", false),
  RM("RM", "\tRemoves an execution from the list.", "Usage: RM <execution ID>", false),

  HELP("HELP", "\tDisplays this help message.", "Usage: HELP [command]", true),
  SET("SET", "\tSets a variable.", "Usage: SET VAR=VAL", true),
  CLEAR("CLEAR", "\tClears the screen.", "CLEAR", true),
  EXIT("EXIT", "\tExits the shell.", "Exit", true),
  QUIT("QUIT", "\tQuits the shell.", "QUIT", true),
  VERSION("VERSION", "\tShows version information", "VERSION", true),

  INVALID_COMMAND("INVALID_COMMAND", "INVALID_COMMAND", "INVALID_COMMAND", true);

  private final String cmdName;
  private final String description;
  private final String usage;
  private final boolean argsAreOptional;

  CliCommandType(String cmdName, String description, String usage, boolean noArgs) {
    this.cmdName = cmdName;
    this.description = description;
    this.usage = usage;
    this.argsAreOptional = noArgs;
  }

  /**
   * static API to return all commands in this enumeration
   * @return list of all commands' names
   */
  public static List<String> getAllCommands() {
    List<String> cmds = new ArrayList<String>();
    for (CliCommandType t : CliCommandType.values()) {
      if (t != INVALID_COMMAND)
        cmds.add(t.getCommandName());
    }
    return cmds;
  }

  /**
   * Gets the command name
   * @return string
   */
  public String getCommandName() {
    return cmdName;
  }

  /**
   * gets the command description
   * @return string
   */
  public String getDescription() {
    return description;
  }

  /**
   * gets the command usage
   * @return string
   */
  public String getUsage() {
    return usage;
  }

  /**
   * gets the argsAreOptional flag
   * @return true if args are optional, false o.w.
   */
  public boolean argsAreOptional() {
    return argsAreOptional;
  }
}