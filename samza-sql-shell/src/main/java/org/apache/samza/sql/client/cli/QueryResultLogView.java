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
import org.jline.utils.*;

import java.util.*;

import static org.jline.keymap.KeyMap.ctrl;


/**
 *  Refer to OReilly's Posix Programming Guide Chapter 8, Terminal I/O and termios(3) for terminal control
 */


public class QueryResultLogView implements CliView {
    private static final int DEFAULT_REFRESH_INTERVAL = 100; // all intervals are in ms

    private int m_refreshInterval = DEFAULT_REFRESH_INTERVAL;
    private int m_width;
    private int m_height;
    private Terminal  m_terminal;
    private SqlExecutor m_executor;
    private ExecutionContext m_exeContext;
    private volatile boolean m_keepRunning = true;
    private boolean m_paused = false;

    // Stupid BindingReader doesn't have a real nonblocking mode
    // Must create a new thread to get user input
    private Thread m_inputThread;
    private BindingReader m_keyReader;

    public QueryResultLogView() {
    }

    // -- implementation of CliView -------------------------------------------

    public void open(CliShell shell, QueryResult queryResult) {
        m_terminal = shell.getTerminal();
        m_executor = shell.getExecutor();
        m_exeContext = shell.getExeContext();

        TerminalStatus prevStatus = setupTerminal();
        try {
            m_keyReader = new BindingReader(m_terminal.reader());
            m_inputThread = new InputThread();
            m_inputThread.start();
            while (m_keepRunning) {
                try {
                    display();
                    if(m_keepRunning)
                        Thread.sleep(m_refreshInterval);
                } catch (InterruptedException e) {
                    continue;
                }
            }

            try {
                m_inputThread.join(1* 1000);
            } catch (InterruptedException e) {
            }
        } finally {
            restoreTerminal(prevStatus);
        }
        if(m_inputThread.isAlive()) {
            m_terminal.writer().println("Warning: input thread hang. Have to kill!");
            m_terminal.writer().flush();
            m_inputThread.interrupt();
        }
    }

    // ------------------------------------------------------------------------

    private void display() {
        updateTerminalSize();
        int rowsInBuffer = m_executor.getRowCount();
        if(rowsInBuffer <= 0 || m_paused) {
            clearStatusBar();
            drawStatusBar(rowsInBuffer);
            return;
        }

        while(rowsInBuffer > 0) {
            clearStatusBar();
            int step = 10;
            List<String[]> lines = m_executor.consumeQueryResult(m_exeContext, 0, step - 1);
            for (String[] line : lines) {
                for (int i = 0; i <line.length; ++i) {
                    m_terminal.writer().write(line[i] == null ? "null" : line[i]);
                    m_terminal.writer().write(i == line.length - 1 ? "\n" : " ");
                }
            }
            m_terminal.flush();
            clearStatusBar();
            drawStatusBar(rowsInBuffer);

            if(!m_keepRunning || m_paused)
                return;

            rowsInBuffer = m_executor.getRowCount();
        }
    }

    private void clearStatusBar() {
        m_terminal.puts(InfoCmp.Capability.save_cursor);
        m_terminal.puts(InfoCmp.Capability.cursor_address, m_height - 1, 0);
        m_terminal.puts(InfoCmp.Capability.delete_line, m_height - 1, 0);
        m_terminal.puts(InfoCmp.Capability.restore_cursor);
    }

    private void drawStatusBar(int rowsInBuffer) {
        m_terminal.puts(InfoCmp.Capability.save_cursor);
        m_terminal.puts(InfoCmp.Capability.cursor_address, m_height - 1, 0);
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
        if(m_paused) {
            attrBuilder.style(statusBarStyle.bold().foreground(AttributedStyle.RED).blink())
                    .append("PAUSED");
        }
        String statusBarText = attrBuilder.toAnsi();
        m_terminal.writer().print(statusBarText);
        m_terminal.flush();
        m_terminal.puts(InfoCmp.Capability.restore_cursor);
    }

    private TerminalStatus setupTerminal() {
        TerminalStatus prevStatus = new TerminalStatus();

        // Signal handlers
        prevStatus.handler_INT = m_terminal.handle(Terminal.Signal.INT, this::handleSignal);
        prevStatus.handler_QUIT = m_terminal.handle(Terminal.Signal.QUIT, this::handleSignal);
        prevStatus.handler_TSTP = m_terminal.handle(Terminal.Signal.TSTP, this::handleSignal);
        prevStatus.handler_CONT = m_terminal.handle(Terminal.Signal.CONT, this::handleSignal);;
        prevStatus.handler_WINCH = m_terminal.handle(Terminal.Signal.WINCH, this::handleSignal);

        // Attributes
        prevStatus.attributes = m_terminal.getAttributes();
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
        m_terminal.setAttributes(newAttributes);

        // Capabilities
        // tput smcup; use alternate screen
        m_terminal.puts(InfoCmp.Capability.enter_ca_mode);
        m_terminal.puts(InfoCmp.Capability.cursor_invisible);
        m_terminal.puts(InfoCmp.Capability.cursor_home);

        m_terminal.flush();

        return prevStatus;
    }

    private void restoreTerminal(TerminalStatus status) {
        // Signal handlers
        m_terminal.handle(Terminal.Signal.INT, status.handler_INT);
        m_terminal.handle(Terminal.Signal.QUIT, status.handler_QUIT);
        m_terminal.handle(Terminal.Signal.TSTP, status.handler_TSTP);
        m_terminal.handle(Terminal.Signal.CONT, status.handler_CONT);
        m_terminal.handle(Terminal.Signal.WINCH, status.handler_WINCH);

        // Attributes
        m_terminal.setAttributes(status.attributes);

        // Capability
        m_terminal.puts(InfoCmp.Capability.exit_ca_mode);
        m_terminal.puts(InfoCmp.Capability.cursor_visible);
    }

    private void handleSignal(Terminal.Signal signal) {
        switch (signal) {
            case INT:
            case QUIT:
                m_keepRunning = false;
                break;
            case TSTP:
                m_paused = true;
                break;
            case CONT:
                m_paused = false;
                break;
            case WINCH:
                updateTerminalSize();
                break;
        }
    }

    private static class TerminalStatus {
        Terminal.SignalHandler handler_INT;
        Terminal.SignalHandler handler_QUIT;
        Terminal.SignalHandler handler_TSTP;
        Terminal.SignalHandler handler_CONT;
        Terminal.SignalHandler handler_WINCH;

        Attributes attributes;
    }

    private void updateTerminalSize() {
        m_terminal.flush();
        m_width = m_terminal.getWidth();
        m_height = m_terminal.getHeight();
    }

    public enum Action {
        QUIT,
        SPACE
    }

    private KeyMap<Action> bindActionKey() {
        KeyMap<Action> keyMap = new KeyMap<>();
        keyMap.bind(Action.QUIT, "Q", "q", ctrl('c'));
        keyMap.bind(Action.SPACE, " ");

        return keyMap;
    }

    private class InputThread extends Thread {
        public InputThread() {
        }

        public void run() {
            KeyMap<Action> keyMap = bindActionKey();

            Action action = m_keyReader.readBinding(keyMap, null, true);
            while (action != null && m_keepRunning) {
                switch (action) {
                    case QUIT:
                        m_keepRunning = false;
                        return;
                    case SPACE:
                        m_paused = !m_paused;
                        break;
                }
                action = m_keyReader.readBinding(keyMap, null, true);
            }
        }
    }
}
