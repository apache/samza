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
package org.apache.samza.container.host;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link SystemStatisticsGetter} that relies on using Posix commands like ps.
 */
public class PosixCommandBasedStatisticsGetter implements SystemStatisticsGetter {

  private static final Logger log = LoggerFactory.getLogger(PosixCommandBasedStatisticsGetter.class);
  private static final long COMMAND_TIMEOUT_SECONDS = 10;
  private static final int MAX_ERROR_LINES_TO_CAPTURE = 100;

  /**
   * A convenience method to execute shell commands and return all lines of their output.
   *
   * @param cmdArray the command to run
   * @return all lines of the output.
   * @throws IOException
   */
  private List<String> getAllCommandOutput(String[] cmdArray) throws IOException {
    log.debug("Executing commands {}", Arrays.toString(cmdArray));
    Process executable = Runtime.getRuntime().exec(cmdArray);
    List<String> psOutput = new ArrayList<>();

    try (BufferedReader processReader = new BufferedReader(new InputStreamReader(executable.getInputStream()));
         BufferedReader errorReader = new BufferedReader(new InputStreamReader(executable.getErrorStream()))) {

      // Read output stream
      String line;
      while ((line = processReader.readLine()) != null) {
        if (!line.isEmpty()) {
          psOutput.add(line);
        }
      }

      // Consume error stream to prevent blocking
      consumeErrorStream(errorReader, cmdArray);

      // Wait for the process to complete to prevent resource leak
      try {
        boolean finished = executable.waitFor(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!finished) {
          throw new IOException("Command timed out after " + COMMAND_TIMEOUT_SECONDS + " seconds");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for command to complete", e);
      }
    } finally {
      // Ensure the process is destroyed to free up resources
      executable.destroy();
    }

    return psOutput;
  }

  /**
   * Consumes the error stream to prevent process blocking.
   * Collects first MAX_ERROR_LINES_TO_CAPTURE lines and logs them together if any error output exists.
   *
   * @param errorReader the BufferedReader for the error stream
   * @param cmdArray the command that was executed (for logging context)
   * @throws IOException if reading from the stream fails
   */
  private void consumeErrorStream(BufferedReader errorReader, String[] cmdArray) throws IOException {
    String line;
    StringBuilder errorOutput = new StringBuilder();
    int lineCount = 0;
    int maxLinesToCapture = MAX_ERROR_LINES_TO_CAPTURE;

    while ((line = errorReader.readLine()) != null) {
      lineCount++;

      if (lineCount <= maxLinesToCapture) {
        errorOutput.append(line).append("\n");
      }
    }

    if (lineCount > 0) {
      String errorMessage = errorOutput.toString();
      if (lineCount > maxLinesToCapture) {
        errorMessage += String.format("... (%d more lines omitted)", lineCount - maxLinesToCapture);
      }
      log.error("Command {} produced error output:\n{}", Arrays.toString(cmdArray), errorMessage);
    }
  }

  private long getTotalPhysicalMemoryUsageBytes() throws IOException {
    // collect all child process ids of the main process that runs the application
    List<String> processIds = getAllCommandOutput(new String[]{"sh", "-c", "pgrep -P $PPID"});
    // add the parent process which is the main process that runs the application
    processIds.add("$PPID");
    String processIdsJoined = String.join(" ", processIds);
    // returns a list of long values that represent the rss memory of each process.
    List<String> processMemoryKBArray = getAllCommandOutput(new String[]{"sh", "-c", String.format("ps -o rss= -p %s", processIdsJoined)});
    long totalPhysicalMemoryKB = 0;
    for (String processMemory : processMemoryKBArray) {
      totalPhysicalMemoryKB += Long.parseLong(processMemory.trim());
    }
    //convert to bytes
    return totalPhysicalMemoryKB * 1024;
  }

  @Override
  public SystemMemoryStatistics getSystemMemoryStatistics() {
    try {
      long memory = getTotalPhysicalMemoryUsageBytes();
      return new SystemMemoryStatistics(memory);
    } catch (Exception e) {
      log.warn("Error when running ps: ", e);
      return null;
    }
  }

  @Override
  public ProcessCPUStatistics getProcessCPUStatistics() {
    throw new UnsupportedOperationException(
        "No appropriate Posix command available for getting recent CPU usage information. For example, the CPU information exposed by ps command 'ps -o %cpu= -p <PID>' represents the percentage of time spent running during the entire lifetime of a process not for the recent CPU usage");
  }
}
