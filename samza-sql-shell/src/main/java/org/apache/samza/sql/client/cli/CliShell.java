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

import java.util.stream.Collectors;
import org.apache.samza.sql.client.interfaces.*;
import org.apache.samza.sql.client.util.CliException;
import org.apache.samza.sql.client.util.CliUtil;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.schema.SqlFieldSchema;
import org.apache.samza.sql.schema.SqlSchema;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * The shell UI.
 */
class CliShell {
  private static final Logger LOG = LoggerFactory.getLogger(CliShell.class);
  private final Terminal terminal;
  private final PrintWriter writer;
  private final LineReader lineReader;
  private final String firstPrompt;
  private SqlExecutor executor;
  private final ExecutionContext exeContext;
  private CliEnvironment env;
  private boolean keepRunning = true;
  private Map<Integer, String> executions = new TreeMap<>();

  CliShell(CliEnvironment environment) throws ExecutorException{
    if (environment == null) {
      throw new IllegalArgumentException();
    }

    // Terminal
    try {
      terminal = TerminalBuilder.builder()
              .name(CliConstants.WINDOW_TITLE)
              .build();
    } catch (IOException e) {
      throw new CliException("Error when creating terminal", e);
    }

    // Terminal writer
    writer = terminal.writer();

    // LineReader
    final DefaultParser parser = new DefaultParser()
            .eofOnEscapedNewLine(true)
            .eofOnUnclosedQuote(true);
    lineReader = LineReaderBuilder.builder()
            .appName(CliConstants.APP_NAME)
            .terminal(terminal)
            .parser(parser)
            .highlighter(new CliHighlighter())
            .completer(new StringsCompleter(CliCommandType.getAllCommands()))
            .build();

    // Command Prompt
    firstPrompt = new AttributedStringBuilder()
            .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
            .append(CliConstants.PROMPT_1ST + CliConstants.PROMPT_1ST_END)
            .toAnsi();

    // Execution context and executor
    env = environment;
    executor = env.getExecutor();
    exeContext = new ExecutionContext();
    executor.start(exeContext);
  }

  Terminal getTerminal() {
    return terminal;
  }

  SqlExecutor getExecutor() {
    return executor;
  }

  ExecutionContext getExeContext() {
    return exeContext;
  }

  /**
   * Actually run the shell. Does not return until user choose to exit.
   */
  void open(String message) {
    // Remember we cannot enter alternate screen mode here as there is only one alternate
    // screen and we need it to show streaming results. Clear the screen instead.
    clearScreen();
    writer.write(CliConstants.WELCOME_MESSAGE);
    printVersion();
    if(!CliUtil.isNullOrEmpty(message)) {
      writer.println(message);
    }
    writer.println();

    try {
      // Check if jna.jar exists in class path
      try {
        ClassLoader.getSystemClassLoader().loadClass("com.sun.jna.NativeLibrary");
      } catch (ClassNotFoundException e) {
        // Something's wrong. It could be a dumb terminal if neither jna nor jansi lib is there
        writer.write("Warning: jna.jar does NOT exist. It may lead to a dumb shell or a performance hit.\n");
      }

      while (keepRunning) {
        String line;
        try {
          line = lineReader.readLine(firstPrompt);
        } catch (UserInterruptException e) {
          continue;
        } catch (EndOfFileException e) {
          commandQuit();
          break;
        }

        if (CliUtil.isNullOrEmpty(line))
          continue;

        CliCommand command = parseLine(line);
        if (command == null)
          continue;

        try {
          switch (command.getCommandType()) {
            case CLEAR:
              commandClear();
              break;

            case DESCRIBE:
              commandDescribe(command);
              break;

            case EXECUTE:
              commandExecuteFile(command);
              break;

            case EXIT:
            case QUIT:
              commandQuit();
              break;

            case HELP:
              commandHelp(command);
              break;

            case INSERT_INTO:
              commandInsertInto(command);
              break;

            case LS:
              commandLs(command);
              break;

            case RM:
              commandRm(command);
              break;

            case SELECT:
              commandSelect(command);
              break;

            case SET:
              commandSet(command);
              break;

            case SHOW_FUNCTIONS:
              commandShowFunctions(command);
              break;

            case SHOW_TABLES:
              commandShowTables(command);
              break;

            case STOP:
              commandStop(command);
              break;

            case VERSION:
              commandVersion(command);
              break;

            case INVALID_COMMAND:
              printHelpMessage();
              break;

            default:
              writer.println("UNDER DEVELOPEMENT. Command:" + command.getCommandType());
              writer.println("Parameters:" +
                      (CliUtil.isNullOrEmpty(command.getParameters()) ? "NULL" : command.getParameters()));
              writer.flush();
          }
        } catch (ExecutorException e) {
          writer.println("Error: " + e);
          LOG.error("Error in {}: ", command.getCommandType(), e);
          writer.flush();
        }
      }
    } catch (Exception e) {
      writer.print(e.getClass().getSimpleName());
      writer.print(". ");
      writer.println(e.getMessage());
      e.printStackTrace(writer);
      writer.println();
      writer.println("We are sorry but SamzaSqlShell has encountered a problem and must stop.");
    }

    writer.write("Cleaning up... ");
    writer.flush();
    try {
      executor.stop(exeContext);
      writer.write("Done.\nBye.\n\n");
      writer.flush();
      terminal.close();
    } catch (IOException | ExecutorException e) {
      // Doesn't matter
    }
  }

  private void commandVersion(CliCommand command) {
    printVersion();
  }

  private void printVersion() {
    String version = String.format("Shell version %s, Executor is %s, version %s",
            this.getClass().getPackage().getImplementationVersion(),
            executor.getClass().getName(),
            executor.getVersion());
    writer.println(version);
  }

  private void commandClear() {
    clearScreen();
  }

  private void commandDescribe(CliCommand command) throws ExecutorException {
    String parameters = command.getParameters();
    if (CliUtil.isNullOrEmpty(parameters)) {
      writer.println(command.getCommandType().getUsage());
      writer.flush();
      return;
    }

    SqlSchema schema = executor.getTableSchema(exeContext, parameters);
    List<String> lines = formatSchema4Display(schema);
    for (String line : lines) {
      writer.println(line);
    }
    writer.flush();
  }

  private void commandSet(CliCommand command) throws ExecutorException{
    String param = command.getParameters();
    if (CliUtil.isNullOrEmpty(param)) {
      env.printAll(writer);
      writer.flush();
      return;
    }
    String[] params = null;
    boolean syntaxValid = param.split(" ").length == 1;
    if(syntaxValid) {
      params = param.split("=");
      if(params.length == 1) {
        String value = env.getEnvironmentVariable(param);
        if(!CliUtil.isNullOrEmpty(value)) {
          env.printVariable(writer, param, value);
        }
        return;
      } else {
        syntaxValid = params.length == 2;
      }
    }
    if(!syntaxValid) {
      writer.println(command.getCommandType().getUsage());
      writer.flush();
      return;
    }

    String name = params[0].trim().toLowerCase();
    String value = params[1].trim();
    int ret = env.setEnvironmentVariable(name, value);
    if (ret == 0) {
      writer.print(name);
      writer.print(" set to ");
      writer.println(value);
      if(name.equals(CliConstants.CONFIG_EXECUTOR)) {
        executor.stop(exeContext);
        executor = env.getExecutor();
        executor.start(exeContext);
      }
    } else if (ret == -1) {
      writer.print("Unknow variable: ");
      writer.println(name);
    } else if (ret == -2) {
      writer.print("Invalid value: ");
      writer.print(value);
      String[] vals = env.getPossibleValues(name);
      if(vals != null && vals.length != 0) {
        writer.print(" Possible values:");
        for (String s : vals) {
          writer.print(CliConstants.SPACE);
          writer.print(s);
        }
        writer.println();
      }
    }

    writer.flush();
  }

  private void commandExecuteFile(CliCommand command) throws ExecutorException{
    String fullCmdStr = command.getFullCommand();
    String parameters = command.getParameters();
    if (CliUtil.isNullOrEmpty(parameters)) {
      writer.println("Usage: execute <fileuri>");
      writer.flush();
      return;
    }
    URI uri = null;
    boolean valid = false;
    File file = null;
    try {
      uri = new URI(parameters);
      file = new File(uri.getPath());
      valid = file.exists() && !file.isDirectory();
    } catch (URISyntaxException e) {
    }
    if (!valid) {
      writer.println("Invalid URI.");
      writer.flush();
      return;
    }

    NonQueryResult nonQueryResult = executor.executeNonQuery(exeContext, file);
    executions.put(nonQueryResult.getExecutionId(), fullCmdStr);
    List<String> submittedStmts = nonQueryResult.getSubmittedStmts();
    List<String> nonsubmittedStmts = nonQueryResult.getNonSubmittedStmts();

    writer.println("Sql file submitted. Execution ID: " + nonQueryResult.getExecutionId());
    writer.println("Submitted statements:");
    if (submittedStmts == null || submittedStmts.size() == 0) {
      writer.println("\tNone.");
    } else {
      for (String statement : submittedStmts) {
        writer.print("\t");
        writer.println(statement);
      }
    }

    if (nonsubmittedStmts != null && nonsubmittedStmts.size() != 0) {
      writer.println("Statements NOT submitted:");
      for (String statement : nonsubmittedStmts) {
        writer.print("\t");
        writer.println(statement);
      }
    }

    writer.println("Note: All query statements in a sql file are NOT submitted.");
    writer.flush();
  }

  private void commandInsertInto(CliCommand command) throws ExecutorException {
    String fullCmdStr = command.getFullCommand();
    NonQueryResult result = executor.executeNonQuery(exeContext,
            Collections.singletonList(fullCmdStr));

    writer.print("Execution submitted successfully. Id: ");
    writer.println(String.valueOf(result.getExecutionId()));
    executions.put(result.getExecutionId(), fullCmdStr);

    writer.flush();
  }

  private void commandLs(CliCommand command) {
    List<Integer> execIds = new ArrayList<>();
    String parameters = command.getParameters();
    if (CliUtil.isNullOrEmpty(parameters)) {
      execIds.addAll(executions.keySet());
    } else {
      String[] params = parameters.split("\u0020");
      for (String param : params) {
        Integer id = null;
        try {
          id = Integer.valueOf(param);
        } catch (NumberFormatException e) {
        }
        if (id != null && executions.containsKey(id)) {
          execIds.add(id);
        }
      }
    }
    if (execIds.size() == 0) {
      return;
    }

    execIds.sort(Integer::compareTo);
    final int terminalWidth = terminal.getWidth();
    final int ID_WIDTH = 3;
    final int STATUS_WIDTH = 20;
    final int CMD_WIDTH = terminalWidth - ID_WIDTH - STATUS_WIDTH - 4;

    AttributedStyle oddLineStyle = AttributedStyle.DEFAULT.BOLD.foreground(AttributedStyle.BLUE);
    AttributedStyle evenLineStyle = AttributedStyle.DEFAULT.BOLD.foreground(AttributedStyle.CYAN);
    for (int i = 0; i < execIds.size(); ++i) {
      Integer id = execIds.get(i);
      String cmd = executions.get(id);
      if (cmd == null)
        continue;

      String status = "UNKNOWN";
      try {
        ExecutionStatus execStatus = executor.queryExecutionStatus(id);
        status = execStatus.name();
      } catch (ExecutorException e) {
        LOG.error("Error in commandLs: ", e);
      }

      int cmdStartIdx = 0;
      int cmdLength = cmd.length();
      StringBuilder line;
      while (cmdStartIdx < cmdLength) {
        line = new StringBuilder(terminalWidth);
        if (cmdStartIdx == 0) {
          line.append(CliConstants.SPACE);
          line.append(id);
          CliUtil.appendTo(line, 1 + ID_WIDTH + 1, CliConstants.SPACE);
          line.append(status);
        }
        CliUtil.appendTo(line, 1 + ID_WIDTH + 1 + STATUS_WIDTH + 1, CliConstants.SPACE);

        int numToWrite = Math.min(CMD_WIDTH, cmdLength - cmdStartIdx);
        if (numToWrite > 0) {
          line.append(cmd, cmdStartIdx, cmdStartIdx + numToWrite);
          cmdStartIdx += numToWrite;
        }

        if (i % 2 == 0) {
          AttributedStringBuilder attrBuilder = new AttributedStringBuilder().style(evenLineStyle);
          attrBuilder.append(line.toString());
          writer.println(attrBuilder.toAnsi());
        } else {
          AttributedStringBuilder attrBuilder = new AttributedStringBuilder().style(oddLineStyle);
          attrBuilder.append(line.toString());
          writer.println(attrBuilder.toAnsi());
        }
      }
    }
    writer.flush();
  }

  private void commandRm(CliCommand command) {
    String parameters = command.getParameters();
    if (CliUtil.isNullOrEmpty(parameters)) {
      writer.println(command.getCommandType().getUsage());
      writer.flush();
      return;
    }

    List<Integer> execIds = new ArrayList<>();
    String[] params = parameters.split("\u0020");
    for (String param : params) {
      Integer id = null;
      try {
        id = Integer.valueOf(param);
      } catch (NumberFormatException e) {
      }
      if (id == null || !executions.containsKey(id)) {
        writer.print("Error: ");
        writer.print(param);
        writer.println(" is not a valid id.");
      } else {
        execIds.add(id);
      }
    }

    for (Integer id : execIds) {
      try {
        ExecutionStatus status = executor.queryExecutionStatus(id);
        if (status == ExecutionStatus.Running) {
          writer.println(String.format("Execution %d is still running. Stop it first.", id));
          continue;
        }
        executor.removeExecution(exeContext, id);
        executions.remove(id);
      } catch (ExecutorException e) {
        writer.println("Error: " + e);
        LOG.error("Error in commandRm: ", e);
      }
    }
    writer.flush();
  }

  private void commandQuit() {
    keepRunning = false;
  }

  private void commandSelect(CliCommand command) throws ExecutorException{
    QueryResult queryResult = executor.executeQuery(exeContext, command.getFullCommand());
    CliView view = new QueryResultLogView();
    view.open(this, queryResult);
    executor.stopExecution(exeContext, queryResult.getExecutionId());
  }

  private void commandShowTables(CliCommand command) throws ExecutorException {
    List<String> tableNames = executor.listTables(exeContext);
    for (String tableName : tableNames) {
      writer.println(tableName);
    }
    writer.flush();
  }

  private void commandShowFunctions(CliCommand command) throws ExecutorException {
    List<SqlFunction> fns = executor.listFunctions(exeContext);
    for (SqlFunction fn : fns) {
      writer.println(fn.toString());
    }
    writer.flush();
  }

  private void commandStop(CliCommand command) {
    String parameters = command.getParameters();
    if (CliUtil.isNullOrEmpty(parameters)) {
      writer.println(command.getCommandType().getUsage());
      writer.flush();
      return;
    }

    List<Integer> execIds = new ArrayList<>();
    String[] params = parameters.split("\u0020");
    for (String param : params) {
      Integer id = null;
      try {
        id = Integer.valueOf(param);
      } catch (NumberFormatException e) {
      }
      if (id == null || !executions.containsKey(id)) {
        writer.print("Error: ");
        writer.print(param);
        writer.println(" is not a valid id.");
      } else {
        execIds.add(id);
      }
    }

    for (Integer id : execIds) {
      try {
        executor.stopExecution(exeContext, id);
        writer.println(String.format("Request to stop execution %d was sent.", id));
      } catch (ExecutorException e) {
        writer.println("Error: " + e);
        LOG.error("Error in commandStop: ", e);
      }
    }
    writer.flush();
  }

  private void commandHelp(CliCommand command) {
    String parameters = command.getParameters();
    if (CliUtil.isNullOrEmpty(parameters)) {
      printHelpMessage();
      return;
    }

    parameters = parameters.trim().toUpperCase();
    for (CliCommandType cmdType : CliCommandType.values()) {
      String cmdText = cmdType.getCommandName();
      if (cmdText.equals(parameters)) {
        writer.println(cmdType.getUsage());
        writer.flush();
        return;
      }
    }

    writer.print("Unknown command: ");
    writer.println(parameters);
    writer.flush();
  }


  private CliCommand parseLine(String line) {
    line = trimCommand(line);
    if (CliUtil.isNullOrEmpty(line))
      return null;

    String upperCaseLine = line.toUpperCase();
    for (CliCommandType cmdType : CliCommandType.values()) {
      String cmdText = cmdType.getCommandName();
      if (upperCaseLine.startsWith(cmdText)) {
        if (upperCaseLine.length() == cmdText.length())
          return new CliCommand(cmdType);
        else if (upperCaseLine.charAt(cmdText.length()) <= CliConstants.SPACE) {
          String parameter = line.substring(cmdText.length()).trim();
          if (!parameter.isEmpty())
            return new CliCommand(cmdType, parameter);
        }
      }
    }
    return new CliCommand(CliCommandType.INVALID_COMMAND);
  }

  private void printHelpMessage() {
    AttributedStringBuilder builder = new AttributedStringBuilder();
    builder.append("The following commands are supported by ")
            .append(CliConstants.APP_NAME)
            .append(" at the moment.\n\n");

    for (CliCommandType cmdType : CliCommandType.values()) {
      if (cmdType == CliCommandType.INVALID_COMMAND)
        continue;

      String cmdText = cmdType.getCommandName();
      String cmdDescription = cmdType.getDescription();

      builder.style(AttributedStyle.DEFAULT.bold())
              .append(cmdText)
              .append("\t\t")
              .style(AttributedStyle.DEFAULT)
              .append(cmdDescription)
              .append("\n");
    }

    writer.println(builder.toAnsi());
    writer.println("HELP <COMMAND> to get help for a specific command.");
    writer.flush();
  }

  private void clearScreen() {
    terminal.puts(InfoCmp.Capability.clear_screen);
  }

  /*
      Field    | Type
      -------------------------
      Field1   | Type 1
      Field2   | VARCHAR(STRING)
      Field... | VARCHAR(STRING)
      -------------------------
  */
  private List<String> formatSchema4Display(SqlSchema schema) {
    final String HEADER_FIELD = "Field";
    final String HEADER_TYPE = "Type";
    final char SEPERATOR = '|';
    final char LINE_SEP = '-';

    int terminalWidth = terminal.getWidth();
    // Two spaces * 2 plus one SEPERATOR
    if (terminalWidth < 2 + 2 + 1 + HEADER_FIELD.length() + HEADER_TYPE.length()) {
      return Collections.singletonList("Not enough room.");
    }

    // Find the best seperator position for least rows
    int seperatorPos = HEADER_FIELD.length() + 2;
    int minRowNeeded = Integer.MAX_VALUE;
    int longestLineCharNum = 0;
    int rowCount = schema.getFields().size();
    for (int j = seperatorPos; j < terminalWidth - HEADER_TYPE.length() - 2; ++j) {
      boolean fieldWrapped = false;
      int rowNeeded = 0;
      for (int i = 0; i < rowCount; ++i) {
        SqlSchema.SqlField field = schema.getFields().get(i);
        int fieldLen = field.getFieldName().length();
        int typeLen = field.getFieldSchema().getFieldType().toString().length();
        int fieldRowNeeded = CliUtil.ceilingDiv(fieldLen, j - 2);
        int typeRowNeeded = CliUtil.ceilingDiv(typeLen, terminalWidth - 1 - j - 2);

        rowNeeded += Math.max(fieldRowNeeded, typeRowNeeded);
        fieldWrapped |= fieldRowNeeded > 1;
        if (typeRowNeeded > 1) {
          longestLineCharNum = terminalWidth;
        } else {
          longestLineCharNum = Math.max(longestLineCharNum, j + typeLen + 2 + 1);
        }
      }
      if (rowNeeded < minRowNeeded) {
        minRowNeeded = rowNeeded;
        seperatorPos = j;
      }
      if (!fieldWrapped)
        break;
    }

    List<String> lines = new ArrayList<>(minRowNeeded + 4);

    // Header
    StringBuilder line = new StringBuilder(terminalWidth);
    line.append(CliConstants.SPACE);
    line.append(HEADER_FIELD);
    CliUtil.appendTo(line, seperatorPos - 1, CliConstants.SPACE);
    line.append(SEPERATOR);
    line.append(CliConstants.SPACE);
    line.append(HEADER_TYPE);
    lines.add(line.toString());
    line = new StringBuilder(terminalWidth);
    CliUtil.appendTo(line, longestLineCharNum - 1, LINE_SEP);
    lines.add(line.toString());

    // Body
    AttributedStyle oddLineStyle = AttributedStyle.BOLD.foreground(AttributedStyle.BLUE);
    AttributedStyle evenLineStyle = AttributedStyle.BOLD.foreground(AttributedStyle.CYAN);

    final int fieldColSize = seperatorPos - 2;
    final int typeColSize = terminalWidth - seperatorPos - 1 - 2;
    for (int i = 0; i < rowCount; ++i) {
      SqlSchema.SqlField sqlField = schema.getFields().get(i);
      String field = sqlField.getFieldName();
      String type = getFieldDisplayValue(sqlField.getFieldSchema());
      int fieldLen = field.length();
      int typeLen = type.length();
      int fieldStartIdx = 0, typeStartIdx = 0;
      while (fieldStartIdx < fieldLen || typeStartIdx < typeLen) {
        line = new StringBuilder(terminalWidth);
        line.append(CliConstants.SPACE);
        int numToWrite = Math.min(fieldColSize, fieldLen - fieldStartIdx);
        if (numToWrite > 0) {
          line.append(field, fieldStartIdx, fieldStartIdx + numToWrite);
          fieldStartIdx += numToWrite;
        }
        CliUtil.appendTo(line, seperatorPos - 1, CliConstants.SPACE);
        line.append(SEPERATOR);
        line.append(CliConstants.SPACE);

        numToWrite = Math.min(typeColSize, typeLen - typeStartIdx);
        if (numToWrite > 0) {
          line.append(type, typeStartIdx, typeStartIdx + numToWrite);
          typeStartIdx += numToWrite;
        }

        if (i % 2 == 0) {
          AttributedStringBuilder attrBuilder = new AttributedStringBuilder().style(evenLineStyle);
          attrBuilder.append(line.toString());
          lines.add(attrBuilder.toAnsi());
        } else {
          AttributedStringBuilder attrBuilder = new AttributedStringBuilder().style(oddLineStyle);
          attrBuilder.append(line.toString());
          lines.add(attrBuilder.toAnsi());
        }
      }
    }

    // Footer
    line = new StringBuilder(terminalWidth);
    CliUtil.appendTo(line, longestLineCharNum - 1, LINE_SEP);
    lines.add(line.toString());
    return lines;
  }

  private String getFieldDisplayValue(SqlFieldSchema fieldSchema) {
    if (!isComplexField(fieldSchema.getFieldType())) {
      return fieldSchema.getFieldType().toString();
    }
    SamzaSqlFieldType fieldType = fieldSchema.getFieldType();
    switch (fieldType) {
      case ARRAY:
        return String.format("ARRAY(%s)", getFieldDisplayValue(fieldSchema.getElementSchema()));
      case MAP:
        return String.format("MAP(%s, %s)", SamzaSqlFieldType.STRING.toString(),
            getFieldDisplayValue(fieldSchema.getValueScehma()));
      case ROW:
        String rowDisplayValue = fieldSchema.getRowSchema()
            .getFields()
            .stream()
            .map(f -> getFieldDisplayValue(f.getFieldSchema()))
            .collect(Collectors.joining(","));
        return String.format("ROW(%s)", rowDisplayValue);
      default:
        throw new UnsupportedOperationException("Unknown field type " + fieldType);
    }
  }

  private boolean isComplexField(SamzaSqlFieldType fieldtype) {
    return fieldtype == SamzaSqlFieldType.ARRAY || fieldtype == SamzaSqlFieldType.MAP
        || fieldtype == SamzaSqlFieldType.ROW;
  }

  // Trims: leading spaces; trailing spaces and ";"s
  private String trimCommand(String command) {
    if (CliUtil.isNullOrEmpty(command))
      return command;

    int len = command.length();
    int st = 0;

    while ((st < len) && (command.charAt(st) <= ' ')) {
      st++;
    }
    while ((st < len) && ((command.charAt(len - 1) <= ' ')
            || command.charAt(len - 1) == ';')) {
      len--;
    }
    return ((st > 0) || (len < command.length())) ? command.substring(st, len) : command;
  }
}
