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
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import java.net.URL;
import java.net.URI;
import java.net.MalformedURLException;
import java.util.Optional;
import java.nio.file.Path;
import java.nio.file.Paths;


public class LinuxCgroupStatisticsGetter implements SystemStatisticsGetter {
  private static final Logger LOG = LoggerFactory.getLogger(LinuxCgroupStatisticsGetter.class.getName());
  private String osName;
  private String cgroupMountPath;
  private String cgroupHierarchy;
  private String containerID;

  LinuxCgroupStatisticsGetter() {
    this.osName = System.getProperty("os.name");
    this.containerID = Optional.ofNullable(System.getenv("CONTAINER_ID")).orElse("NOT_DETECTED");
    String hadoopConfDir =  Optional.ofNullable(System.getenv("HADOOP_CONF_DIR")).orElse("NOT_DETECTED");
    Configuration yarnSite;
    if (!hadoopConfDir.equals("NOT_DETECTED")) {
      yarnSite = getHadoopConf(hadoopConfDir);
      this.cgroupMountPath = yarnSite.get("yarn.nodemanager.linux-container-executor.cgroups.hierarchy", "NOT_DETECTED");
      this.cgroupHierarchy = yarnSite.get("yarn.nodemanager.linux-container-executor.cgroups.mount-path", "NOT_DETECTED");
    }
    LOG.debug("CONTAINER ID: " + this.containerID);
    LOG.debug("HADOOP_CONF_DIR: " + hadoopConfDir);
    LOG.debug("CGROUP_MOUNT_PATH: " + this.cgroupMountPath);
    LOG.debug("CGROUP_MOUNT_HIERARCHY: " + this.cgroupHierarchy);
  }

  @Override
  public SystemMemoryStatistics getSystemMemoryStatistics() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ProcessCPUStatistics getProcessCPUStatistics() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public LinuxCgroupStatistics getProcessCgroupStatistics() {
    if (this.osName != "Linux" || this.containerID.equals("NOT_DETECTED")) {
      LOG.debug("OS is not 'Linux' or Container ID not found in ENV");
      return null;
    }
    // TODO: create place holder while we figure this out
    return new LinuxCgroupStatistics(0.5);
  }

  private double getCPUStat() {
    double cpuThrottleRatio = -1.0;
    String[] controllers = {"cpu", "cpuacct", "cpu,cpuacct" };
    String cpuStatPath;
    Long nrPeriod, nrThrottled = -1L;
    for (String controller : controllers) {
      cpuStatPath = this.cgroupMountPath + controller + this.cgroupHierarchy + "/cpu.stat";
      if (cpuStatExists(this.cgroupMountPath + controller + this.cgroupHierarchy + "/cpu.stat")) {
        LOG.debug("Found cpu.stat file: " + cpuStatPath);
        try (Stream<String> lines = Files.lines(Paths.get(cpuStatPath))) {
          nrPeriod = Long.parseLong(String.valueOf(lines.map(line -> line.split(" "))
              .filter(line -> line.length == 2 && line[0].equals("nr_periods"))
              .map(line -> line[1])
              .findFirst())
          );
          nrThrottled = Long.parseLong(String.valueOf(lines.map(line -> line.split(" "))
              .filter(line -> line.length == 2 && line[0].equals("nr_throttled"))
              .map(line -> line[1])
              .findFirst())
          );
          LOG.debug("cpu.stat nr_period value: " + nrPeriod.toString());
          LOG.debug("cpu.stat nr_throttled value: " + nrThrottled.toString());
          cpuThrottleRatio = nrPeriod / nrThrottled;
        } catch (RuntimeException | IOException e) {
          LOG.error("Unable to read cpu.stat file: {}", e.getStackTrace());
        }
      }
    }
    return cpuThrottleRatio;
  }

  private boolean cpuStatExists(String cpuStatPath) {
    Path cpuStat = Paths.get(cpuStatPath);
    return Files.exists(cpuStat);
  }

  private Configuration getHadoopConf(String hConfDir) {
    Configuration hConf = new Configuration();
    try {
      String yarnSiteURI = "file://" + hConfDir + "/yarn-site.xml";
      LOG.debug("yarn-site.xml URI: " + yarnSiteURI);
      URL yarnSiteUrl = URI.create(yarnSiteURI).toURL();
      hConf.addResource(yarnSiteUrl);
    } catch (MalformedURLException | IllegalArgumentException e) {
      LOG.error("Unable to construct URL to yarn-site.xml: " + e.getMessage());
    }
    return hConf;
  }
}

