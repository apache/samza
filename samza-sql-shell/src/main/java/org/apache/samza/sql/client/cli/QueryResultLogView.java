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

import org.apache.samza.sql.client.interfaces.ExecutionContext;
import org.apache.samza.sql.client.interfaces.QueryResult;
import org.apache.samza.sql.client.interfaces.SqlExecutor;
import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

import java.util.EnumSet;
import java.util.List;

import static org.jline.keymap.KeyMap.ctrl;


/**
 * A scrolling (logging) view of the query result of a streaming SELECT statement.
 */

public class QueryResultLogView implements CliView {
  private static final int DEFAULT_REFRESH_INTERVAL = 100; // all intervals are in ms

  private int refreshInterval = DEFAULT_REFRESH_INTERVAL;
  private int height;
  private Terminal terminal;
  private SqlExecutor executor;
  private ExecutionContext exeContext;
  private volatile boolean keepRunning = true;
  private boolean paused = false;

  // Stupid BindingReader doesn't have a real nonblocking mode
  // Must create a new thread to get user input
  private Thread inputThread;
  private BindingReader keyReader;

  public QueryResultLogView() {
  }

  // -- implementation of CliView -------------------------------------------

  public void open(CliShell shell, QueryResult queryResult) {
    terminal = shell.getTerminal();
    executor = shell.getExecutor();
    exeContext = shell.getExeContext();

    TerminalStatus prevStatus = setupTerminal();
    try {
      keyReader = new BindingReader(terminal.reader());
      inputThread = new InputThread();
      inputThread.start();
      while (keepRunning) {
        try {
          display();
          if (keepRunning)
            Thread.sleep(refreshInterval);
        } catch (InterruptedException e) {
          continue;
        }
      }

      try {
        inputThread.join(1 * 1000);
      } catch (InterruptedException e) {
      }
    } finally {
      restoreTerminal(prevStatus);
    }
    if (inputThread.isAlive()) {
      terminal.writer().println("Warning: input thread hang. Have to kill!");
      terminal.writer().flush();
      inputThread.interrupt();
    }
  }

  // ------------------------------------------------------------------------

  private void display() {
    updateTerminalSize();
    int rowsInBuffer = executor.getRowCount();
    if (rowsInBuffer <= 0 || paused) {
      clearStatusBar();
      drawStatusBar(rowsInBuffer);
      return;
    }

    while (rowsInBuffer > 0) {
      clearStatusBar();
      int step = 10;
      List<String[]> lines = executor.consumeQueryResult(exeContext, 0, step - 1);
      for (String[] line : lines) {
        for (int i = 0; i < line.length; ++i) {
          terminal.writer().write(line[i] == null ? "null" : line[i]);
          terminal.writer().write(i == line.length - 1 ? "\n" : " ");
        }
      }
      terminal.flush();
      clearStatusBar();
      drawStatusBar(rowsInBuffer);

      if (!keepRunning || paused)
        return;

      rowsInBuffer = executor.getRowCount();
    }
  }

  private void clearStatusBar() {
    terminal.puts(InfoCmp.Capability.save_cursor);
    terminal.puts(InfoCmp.Capability.cursor_address, height - 1, 0);
    terminal.puts(InfoCmp.Capability.delete_line, height - 1, 0);
    terminal.puts(InfoCmp.Capability.restore_cursor);
  }

  private void drawStatusBar(int rowsInBuffer) {
    terminal.puts(InfoCmp.Capability.save_cursor);
    terminal.puts(InfoCmp.Capability.cursor_address, height - 1, 0);
    AttributedStyle statusBarStyle = AttributedStyle.DEFAULT.background(AttributedStyle.WHITE)
            .foreground(AttributedStyle.BLACK);
    AttributedStringBuilder attrBuilder = new AttributedStringBuilder()
            .style(statusBarStyle.bold().italic())
            .append("Q")
            .style(statusBarStyle)
            .append(": Quit     ")
            .style(statusBarStyle.bold().italic())
            .append("SPACE")
            .style(statusBarStyle)
            .append(": Pause/Resume     ")
            .append(String.valueOf(rowsInBuffer) + " rows in buffer     ");
    if (paused) {
      attrBuilder.style(statusBarStyle.bold().foreground(AttributedStyle.RED).blink())
              .append("PAUSED");
    }
    String statusBarText = attrBuilder.toAnsi();
    terminal.writer().print(statusBarText);
    terminal.flush();
    terminal.puts(InfoCmp.Capability.restore_cursor);
  }

  private TerminalStatus setupTerminal() {
    TerminalStatus prevStatus = new TerminalStatus();

    // Signal handlers
    prevStatus.handler_INT = terminal.handle(Terminal.Signal.INT, this::handleSignal);
    prevStatus.handler_QUIT = terminal.handle(Terminal.Signal.QUIT, this::handleSignal);
    prevStatus.handler_TSTP = terminal.handle(Terminal.Signal.TSTP, this::handleSignal);
    prevStatus.handler_CONT = terminal.handle(Terminal.Signal.CONT, this::handleSignal);
    prevStatus.handler_WINCH = terminal.handle(Terminal.Signal.WINCH, this::handleSignal);

    // Attributes
    prevStatus.attributes = terminal.getAttributes();
    Attributes newAttributes = new Attributes(prevStatus.attributes);
    // (003, ETX, Ctrl-C, or also 0177, DEL, rubout) Interrupt char‐
    // acter (INTR).  Send a SIGINT signal.  Recognized when ISIG is
    // set, and then not passed as input.
    newAttributes.setControlChar(Attributes.ControlChar.VINTR, 0);
    // (034, FS, Ctrl-\) Quit character (QUIT).  Send SIGQUIT signal.
    // Recognized when ISIG is set, and then not passed as input.
    // newAttributes.setControlChar(Attributes.ControlChar.VQUIT, 0);
    newAttributes.setControlChar(Attributes.ControlChar.VMIN, 1);
    newAttributes.setControlChar(Attributes.ControlChar.VTIME, 0);
    // Enables signals and SIGTTOU signal to the process group of a background
    // process which tries to write to our terminal
    newAttributes.setLocalFlags(
            EnumSet.of(Attributes.LocalFlag.ISIG, Attributes.LocalFlag.TOSTOP), true);
    // No canonical mode, no echo, and no implementation-defined input processing
    newAttributes.setLocalFlags(EnumSet.of(
            Attributes.LocalFlag.ICANON, Attributes.LocalFlag.ECHO,
            Attributes.LocalFlag.IEXTEN), false);
    // Input flags
    newAttributes.setInputFlags(EnumSet.of(
            Attributes.InputFlag.ICRNL, Attributes.InputFlag.INLCR, Attributes.InputFlag.IXON), false);
    terminal.setAttributes(newAttributes);

    // Capabilities
    // tput smcup; use alternate screen
    terminal.puts(InfoCmp.Capability.enter_ca_mode);
    terminal.puts(InfoCmp.Capability.cursor_invisible);
    terminal.puts(InfoCmp.Capability.cursor_home);

    terminal.flush();

    return prevStatus;
  }

  private void restoreTerminal(TerminalStatus status) {
    // Signal handlers
    terminal.handle(Terminal.Signal.INT, status.handler_INT);
    terminal.handle(Terminal.Signal.QUIT, status.handler_QUIT);
    terminal.handle(Terminal.Signal.TSTP, status.handler_TSTP);
    terminal.handle(Terminal.Signal.CONT, status.handler_CONT);
    terminal.handle(Terminal.Signal.WINCH, status.handler_WINCH);

    // Attributes
    terminal.setAttributes(status.attributes);

    // Capability
    terminal.puts(InfoCmp.Capability.exit_ca_mode);
    terminal.puts(InfoCmp.Capability.cursor_visible);
  }

  private void handleSignal(Terminal.Signal signal) {
    switch (signal) {
      case INT:
      case QUIT:
        keepRunning = false;
        break;
      case TSTP:
        paused = true;
        break;
      case CONT:
        paused = false;
        break;
      case WINCH:
        updateTerminalSize();
        break;
    }
  }

  private void updateTerminalSize() {
    terminal.flush();
    height = terminal.getHeight();
  }

  private KeyMap<Action> bindActionKey() {
    KeyMap<Action> keyMap = new KeyMap<>();
    keyMap.bind(Action.QUIT, "Q", "q", ctrl('c'));
    keyMap.bind(Action.SPACE, " ");

    return keyMap;
  }

  public enum Action {
    QUIT,
    SPACE
  }

  private static class TerminalStatus {
    Terminal.SignalHandler handler_INT;
    Terminal.SignalHandler handler_QUIT;
    Terminal.SignalHandler handler_TSTP;
    Terminal.SignalHandler handler_CONT;
    Terminal.SignalHandler handler_WINCH;

    Attributes attributes;
  }

  private class InputThread extends Thread {
    public InputThread() {
    }

    public void run() {
      KeyMap<Action> keyMap = bindActionKey();

      Action action = keyReader.readBinding(keyMap, null, true);
      while (action != null && keepRunning) {
        switch (action) {
          case QUIT:
            keepRunning = false;
            return;
          case SPACE:
            paused = !paused;
            break;
        }
        action = keyReader.readBinding(keyMap, null, true);
      }
    }
  }
}
