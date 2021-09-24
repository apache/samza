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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * An implementation of {@link SystemStatisticsGetter} that relies on using Posix commands like ps.
 */
public class PosixCommandBasedStatisticsGetter implements SystemStatisticsGetter {

  private static final Logger log = LoggerFactory.getLogger(PosixCommandBasedStatisticsGetter.class);

  /**
   * A convenience method to execute shell commands and return the first line of their output.
   *
   * @param cmdArray the command to run
   * @return the first line of the output.
   * @throws IOException
   */
  private String getCommandOutput(String[] cmdArray) throws IOException {
    Process executable = Runtime.getRuntime().exec(cmdArray);
    BufferedReader processReader = null;
    String psOutput = null;

    try {
      processReader = new BufferedReader(new InputStreamReader(executable.getInputStream()));
      psOutput = processReader.readLine();
    } finally {
      if (processReader != null) {
        processReader.close();
      }
    }
    return psOutput;
  }

  /**
   * A convenience method to execute shell commands and return all lines of their output.
   *
   * @param cmdArray the command to run
   * @return all lines of the output.
   * @throws IOException
   */
  private List<String> getAllCommandOutput(String[] cmdArray) throws IOException {
    Process executable = Runtime.getRuntime().exec(cmdArray);
    BufferedReader processReader = null;
    List<String> psOutput;

    try {
      processReader = new BufferedReader(new InputStreamReader(executable.getInputStream()));
      psOutput = processReader.lines().filter(StringUtils::isNotEmpty).collect(Collectors.toList());
    } finally {
      if (processReader != null) {
        processReader.close();
      }
    }
    return psOutput;
  }

  private long getTotalPhysicalMemory() throws IOException {
    // collect all child process ids of the main process that runs the application
    List<String> processIds = getAllCommandOutput(new String[]{"sh", "-c", "pgrep -P $PPID"});
    // add the parent process which is the main process that runs the application
    processIds.add("$PPID");
    String processIdsJoined = String.join(" ", processIds);
    // returns a list of long values that represent the rss memory of each process.
    List<String> processMemoryArray = getAllCommandOutput(new String[]{"sh", "-c", String.format("ps -o rss= -p %s", processIdsJoined)});
    long totalPhysicalMemory = 0;
    for (String processMemory : processMemoryArray) {
      totalPhysicalMemory += Long.parseLong(processMemory.trim());
    }
    //convert to bytes
    return totalPhysicalMemory * 1024;
  }

  @Override
  public SystemMemoryStatistics getSystemMemoryStatistics() {
    try {
      long memory = getTotalPhysicalMemory();
      return new SystemMemoryStatistics(memory);
    } catch (Exception e) {
      log.warn("Error when running ps: ", e);
      return null;
    }
  }
}
