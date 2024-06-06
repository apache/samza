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


import static org.junit.Assert.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import java.util.logging.Logger;
import org.junit.Assume;


public class TestLinuxCgroupStatisticsGetter {
  private static final Logger LOGGER = Logger.getLogger("TestLinuxCgroupStatisticsGetter");

  @Rule
  public TemporaryFolder myTempDir = new TemporaryFolder();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testGetThrottleValue() {
    Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
    environmentVariables.set("CONTAINER_ID", "container_abc_123");
    environmentVariables.set("HADOOP_CONF_DIR", myTempDir.getRoot().toString());

    File yarnSiteXml, cpuStatFile, cpuStatDirs;
    try {
      // Need stub data at my_temp_dir/yarn-site.xml
      //                   my_temp_dir/cgroup/cpu/user.slice/container_abc_123/cpu.stat
      yarnSiteXml = myTempDir.newFile("yarn-site.xml");

      FileWriter fw1 = new FileWriter(yarnSiteXml);
      BufferedWriter bw1 = new BufferedWriter(fw1);
      bw1.write("<configuration><property>");
      bw1.write("<name>yarn.nodemanager.linux-container-executor.cgroups.hierarchy</name>");
      bw1.write("<value>user.slice</value></property>");
      bw1.write("<property><name>yarn.nodemanager.linux-container-executor.cgroups.mount-path</name>");
      bw1.write("<value>" + myTempDir.getRoot() + "/cgroup</value>");
      bw1.write("</property></configuration>");
      bw1.close();

      String cpuStatPath = myTempDir.getRoot() + "/cgroup/cpu/user.slice/container_abc_123";
      cpuStatDirs = new File(cpuStatPath);
      cpuStatDirs.mkdirs();

      cpuStatFile = new File(cpuStatDirs, "cpu.stat");
      cpuStatFile.createNewFile();

      FileWriter fw2 = new FileWriter(cpuStatFile);
      BufferedWriter bw2 = new BufferedWriter(fw2);
      bw2.write("nr_periods 340956467");
      bw2.newLine();
      bw2.write("nr_throttled 292501");
      bw2.newLine();
      bw2.write("throttled_time 5997018459867");
      bw2.newLine();
      bw2.close();

    } catch (IOException e) {
      System.err.println("Error creating temporary test files: " + e.getMessage());
    }

    LinuxCgroupStatisticsGetter linuxCgroupStatisticsGetter = new LinuxCgroupStatisticsGetter();
    LinuxCgroupStatistics cpuStat = linuxCgroupStatisticsGetter.getProcessCgroupStatistics();

    double throttleRatio = cpuStat.getCgroupCpuThrottleRatio();
    assertEquals(throttleRatio, 0.00085788371334807384, 0.05);

  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetSystemMemoryStatistics() {
    LinuxCgroupStatisticsGetter linuxCgroupStatisticsGetter = new LinuxCgroupStatisticsGetter();
    linuxCgroupStatisticsGetter.getSystemMemoryStatistics();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetProcessCPUStatistics() {
    LinuxCgroupStatisticsGetter linuxCgroupStatisticsGetter = new LinuxCgroupStatisticsGetter();
    linuxCgroupStatisticsGetter.getProcessCPUStatistics();
  }

}
