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
package org.apache.samza.rest.script;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runs a script process and returns the exit code.
 *
 * The script can be run with an output handler or with output redirected to console.
 */
public class ScriptRunner {
  private static final Logger log = LoggerFactory.getLogger(ScriptRunner.class);
  private static final int DEFAULT_SCRIPT_CMD_TIMEOUT_S = 30;
  private int scriptTimeout = DEFAULT_SCRIPT_CMD_TIMEOUT_S;

  // Dont pass down the current environment vars by default
  private boolean forwardEnvironmentVars = false;
  private final Map<String, String> environment = new HashMap<>();

  public ScriptRunner() {

  }

  public ScriptRunner(boolean forwardEnvironmentVars) {
    this.forwardEnvironmentVars = forwardEnvironmentVars;
  }

  protected long getScriptTimeoutS() {
    return scriptTimeout;
  }

  /**
   * Runs a script with IO inherited from the current Java process. Typically this redirects to console.
   *
   * @param scriptPath            the path to the script file.
   * @param args                  the command line args to pass to the script.
   * @return                      the exit code returned by the script.
   * @throws IOException          if there was a problem running the process.
   * @throws InterruptedException if the thread is interrupted while waiting for the process to finish.
   */
  public int runScript(String scriptPath, String... args)
      throws IOException, InterruptedException {
    ProcessBuilder processBuilder = getProcessBuilder(scriptPath, args);
    Process p = processBuilder.inheritIO().start();

    return waitForExitValue(p);
  }

  /**
   * @param scriptPath            the path to the script file.
   * @param outputHandler         the handler for any stdout and stderr produced by the script.
   * @param args                  the command line args to pass to the script.
   * @return                      the exit code returned by the script.
   * @throws IOException          if there was a problem running the process.
   * @throws InterruptedException if the thread is interrupted while waiting for the process to finish.
   */
  public int runScript(String scriptPath, ScriptOutputHandler outputHandler, String... args)
      throws IOException, InterruptedException {
    ProcessBuilder processBuilder = getProcessBuilder(scriptPath, args);
    Process p = processBuilder.redirectErrorStream(true).start();

    InputStream output = p.getInputStream();
    outputHandler.processScriptOutput(output);

    return waitForExitValue(p);
  }

  /**
   * @param scriptPath  the path to the script file.
   * @param args        the command line args to pass to the script.
   * @return            a {@link java.lang.ProcessBuilder} for the script and args.
   */
  private ProcessBuilder getProcessBuilder(String scriptPath, String[] args) throws FileNotFoundException {
    if (!new File(scriptPath).exists()) {
      throw new FileNotFoundException("Script file does not exist: " + scriptPath);
    }

    List<String> command = new ArrayList<>(args.length + 1);
    command.add(scriptPath);
    command.addAll(Arrays.asList(args));

    log.debug("Building process with command {}", command);
    ProcessBuilder pb =  new ProcessBuilder(command);

    if (!forwardEnvironmentVars) {
      pb.environment().clear();
    }

    pb.environment().putAll(environment);
    return pb;
  }

  /**
   * Waits for a finite time interval for the script to complete.
   *
   * @param p                     the process on which this method will wait.
   * @return                      the exit code returned by the process.
   * @throws InterruptedException if the thread is interrupted while waiting for the process to finish.
   */
  private int waitForExitValue(final Process p)
      throws InterruptedException {
    log.debug("Waiting for the exit value for process {}", p);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          p.waitFor();
        } catch (InterruptedException ignore) {
          return;
        }
      }
    });

    t.start();
    try {
      t.join(TimeUnit.MILLISECONDS.convert(getScriptTimeoutS(), TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      t.interrupt();
      throw new SamzaException("Timeout running shell command", e);
    }

    int exitVal = p.exitValue();
    log.debug("Exit value {}", exitVal);
    return exitVal;
  }

  /**
   * @return true if this runner will forward the current environment variables to the child process, false otherwise.
   */
  public boolean forwardEnvironmentVars() {
    return forwardEnvironmentVars;
  }

  /**
   * Set the flag indicating whether this runner will forward the current environment variables to the child process.
   *
   * @param forwardEnvironmentVars  true to forward the current environment to the child process,
   *                                false to start with an empty environment.
   */
  public void setForwardEnvironmentVars(boolean forwardEnvironmentVars) {
    this.forwardEnvironmentVars = forwardEnvironmentVars;
  }

  /**
   * Gets the mutable map of environment variables to add to the child process environment.
   *
   * The structure is the same as {@link ProcessBuilder#environment()}, but these
   * values are added to the environment. They do not replace the other vars in the
   * environment. For that see {@link #setForwardEnvironmentVars(boolean)}.
   *
   * @return the mutable map of environment variables.
   */
  public Map<String, String> environment() {
    return environment;
  }
}
