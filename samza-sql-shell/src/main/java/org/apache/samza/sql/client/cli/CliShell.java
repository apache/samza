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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.samza.sql.client.exceptions.CommandHandlerException;
import org.apache.samza.sql.client.impl.CliCommandHandler;
import org.apache.samza.sql.client.impl.CliCommandType;
import org.apache.samza.sql.client.interfaces.CommandHandler;
import org.apache.samza.sql.client.interfaces.ExecutionContext;
import org.apache.samza.sql.client.exceptions.ExecutorException;
import org.apache.samza.sql.client.interfaces.SqlExecutor;
import org.apache.samza.sql.client.exceptions.CliException;
import org.apache.samza.sql.client.util.CliUtil;
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

/**
 * The shell UI.
 */
public class CliShell {
  private static final Logger LOG = LoggerFactory.getLogger(CliShell.class);
  private final Terminal terminal;
  private final PrintWriter writer;
  private final LineReader lineReader;
  private final String firstPrompt;
  private SqlExecutor executor;
  private List<CommandHandler> commandHandlers;
  private final ExecutionContext exeContext;
  private boolean keepRunning = true;

  CliShell(CliEnvironment environment) throws ExecutorException {
    if (environment == null) {
      throw new IllegalArgumentException();
    }

    // Terminal
    try {
      terminal = TerminalBuilder.builder().name(CliConstants.WINDOW_TITLE).build();
    } catch (IOException e) {
      throw new CliException("Error when creating terminal", e);
    }

    // Terminal writer
    writer = terminal.writer();

    // LineReader
    final DefaultParser parser = new DefaultParser().eofOnEscapedNewLine(true).eofOnUnclosedQuote(true);
    lineReader = LineReaderBuilder.builder()
        .appName(CliConstants.APP_NAME)
        .terminal(terminal)
        .parser(parser)
        .highlighter(new CliHighlighter())
        .completer(new StringsCompleter(CliCommandType.getAllCommands()))
        .build();

    // Command Prompt
    firstPrompt = new AttributedStringBuilder().style(AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
        .append(CliConstants.PROMPT_1ST + CliConstants.PROMPT_1ST_END)
        .toAnsi();

    // Execution context and executor
    executor = environment.getExecutor();
    exeContext = new ExecutionContext();
    executor.start(exeContext);

    // Command handlers
    if (commandHandlers == null) {
      commandHandlers = new ArrayList<>();
    }
    commandHandlers.add(new CliCommandHandler());
    commandHandlers.addAll(environment.getCommandHandlers());
    for (CommandHandler commandHandler : commandHandlers) {
      LOG.info("init commandHandler {}", commandHandler.getClass().getName());
      commandHandler.init(this, environment, terminal, exeContext);
    }
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
          keepRunning = false;
          break;
        }

        if (CliUtil.isNullOrEmpty(line))
          continue;

        String helpCmdText = CliCommandType.HELP.getCommandName();
        if (line.toUpperCase().startsWith(helpCmdText)) {
          if (line.toLowerCase().trim().equals(helpCmdText)) {
            printHelpMessage();
          } else {
            CommandAndHandler commandAndHandler = findHandlerForCommand(line.substring(helpCmdText.length()));
            if (commandAndHandler.handler != null) {
              commandAndHandler.handler.handleCommand(commandAndHandler.handler.parseLine(line));
            } else {
              printHelpMessage();
            }
          }
          continue;
        }

        CommandAndHandler commandAndHandler = null;
        try {
          commandAndHandler = findHandlerForCommand(line);
          if (commandAndHandler.handler == null) {
            LOG.info("no commandHandler found for command {}", line);
            printHelpMessage();
            continue;
          }
          keepRunning = commandAndHandler.handler.handleCommand(commandAndHandler.command);
        } catch (CommandHandlerException e) {
          writer.println("Error: " + e);
          LOG.error("Error in {}: ", commandAndHandler.command.getCommandType(), e);
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

  private void printVersion() {
    String version = String.format("Shell version %s, Executor is %s, version %s",
            this.getClass().getPackage().getImplementationVersion(),
            executor.getClass().getName(),
            executor.getVersion());
    writer.println(version);
  }

  private void clearScreen() {
    terminal.puts(InfoCmp.Capability.clear_screen);
  }

  private void printHelpMessage() {
    for (CommandHandler commandHandler : commandHandlers) {
      commandHandler.printHelpMessage();
    }
    writer.println("HELP <COMMAND> to get help for a specific command.");
    writer.flush();
  }

  private CommandAndHandler findHandlerForCommand(String line) {
    CommandHandler commandHandler = null;
    CliCommand parsedCommand = null;
    for (CommandHandler curCommandHandler : commandHandlers) {
      parsedCommand = curCommandHandler.parseLine(line);
      if (parsedCommand != null && !parsedCommand.getCommandType().getCommandName().equals(CliCommandType.INVALID_COMMAND.getCommandName())) {
        commandHandler = curCommandHandler;
        LOG.info("Found commandHandler {} to handle command {}", commandHandler.getClass().getName(),
            parsedCommand.getFullCommand());
        break;
      }
    }
    return new CommandAndHandler(parsedCommand, commandHandler);
  }

  class CommandAndHandler {
    CliCommand command;
    CommandHandler handler;

    CommandAndHandler(CliCommand aCommand, CommandHandler itsHandler) {
      command = aCommand;
      handler = itsHandler;
    }
  }
}