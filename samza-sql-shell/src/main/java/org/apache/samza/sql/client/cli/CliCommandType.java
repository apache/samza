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

import java.util.ArrayList;
import java.util.List;

/**
 * Enum all the commands we now support along with descriptions.
 */
enum CliCommandType {
  SHOW_TABLES("SHOW TABLES", "Shows all available tables.", "Usage: SHOW TABLES <table name>"),
  SHOW_FUNCTIONS("SHOW FUNCTIONS", "Shows all available UDFs.", "SHOW FUNCTION"),
  DESCRIBE("DESCRIBE", "Describes a table.", "Usage: DESCRIBE <table name>"),

  SELECT("SELECT", "\tExecutes a SQL SELECT query.", "SELECT uses a standard streaming SQL syntax."),
  EXECUTE("EXECUTE", "\tExecute a sql file.", "EXECUTE <URI of a sql file>"),
  INSERT_INTO("INSERT INTO", "Executes a SQL INSERT INTO.", "INSERT INTO uses a standard streaming SQL syntax."),
  LS("LS", "\tLists all background executions.", "LS [execution ID]"),
  STOP("STOP", "\tStops an execution.", "Usage: STOP <execution ID>"),
  RM("RM", "\tRemoves an execution from the list.", "Usage: RM <execution ID>"),

  HELP("HELP", "\tDisplays this help message.", "Usage: HELP [command]"),
  SET("SET", "\tSets a variable.", "Usage: SET VAR=VAL"),
  CLEAR("CLEAR", "\tClears the screen.", "CLEAR"),
  EXIT("EXIT", "\tExits the shell.", "Exit"),
  QUIT("QUIT", "\tQuits the shell.", "QUIT"),

  INVALID_COMMAND("INVALID_COMMAND", "INVALID_COMMAND", "INVALID_COMMAND");

  private final String cmdName;
  private final String description;
  private final String usage;

  CliCommandType(String cmdName, String description, String usage) {
    this.cmdName = cmdName;
    this.description = description;
    this.usage = usage;
  }

  public static List<String> getAllCommands() {
    List<String> cmds = new ArrayList<String>();
    for (CliCommandType t : CliCommandType.values()) {
      if (t != INVALID_COMMAND)
        cmds.add(t.getCommandName());
    }
    return cmds;
  }

  public String getCommandName() {
    return cmdName;
  }

  public String getDescription() {
    return description;
  }

  public String getUsage() {
    return usage;
  }
}