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
import java.util.Properties;
import java.io.FileReader;
import java.nio.file.Files;
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
  private String cgroupMountPath;
  private String cgroupHierarchy;
  private String containerID;

  @Override
  public SystemMemoryStatistics getSystemMemoryStatistics() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ProcessCPUStatistics getProcessCPUStatistics() {
    throw new UnsupportedOperationException("Not implemented");
  }

  LinuxCgroupStatisticsGetter() {
    this.containerID = Optional.ofNullable(System.getenv("CONTAINER_ID")).orElse("NOT_DETECTED");
    String hadoopConfDir =  Optional.ofNullable(System.getenv("HADOOP_CONF_DIR")).orElse("NOT_DETECTED");
    Configuration yarnSite;
    if (!hadoopConfDir.equals("NOT_DETECTED")) {
      yarnSite = getHadoopConf(hadoopConfDir);
      this.cgroupHierarchy = yarnSite.get("yarn.nodemanager.linux-container-executor.cgroups.hierarchy", "NOT_DETECTED");
      this.cgroupMountPath = yarnSite.get("yarn.nodemanager.linux-container-executor.cgroups.mount-path", "NOT_DETECTED");
    }
    LOG.debug("CONTAINER ID: " + this.containerID);
    LOG.debug("HADOOP_CONF_DIR: " + hadoopConfDir);
    LOG.debug("CGROUP_MOUNT_PATH: " + this.cgroupMountPath);
    LOG.debug("CGROUP_MOUNT_HIERARCHY: " + this.cgroupHierarchy);
  }

  @Override
  public LinuxCgroupStatistics getProcessCgroupStatistics() {
    try {
      double ratio = getCPUStat();
      return new LinuxCgroupStatistics(ratio);
    } catch (Exception e) {
      LOG.debug("Error reading cgroups information: ", e);
      return null;
    }
  }

  private double getCPUStat() {
    String[] controllers = {"cpu", "cpuacct", "cpu,cpuacct" };
    double cpuThrottledRatio = -1.0;
    String cpuStatPath;
    for (String controller : controllers) {
      cpuStatPath = this.cgroupMountPath + "/" + controller + "/" + this.cgroupHierarchy + "/" + this.containerID + "/cpu.stat";
      if (cpuStatExists(cpuStatPath)) {
        LOG.debug("Found cpu.stat file: " + cpuStatPath);
        try {
          Properties cpuStatValues = new Properties(); // Treat cpu.stat as a properties file as content is space delimited key value data.
          cpuStatValues.load(new FileReader(cpuStatPath));
          long nrPeriod = Long.parseLong(cpuStatValues.getProperty("nr_periods", "-1.0"));
          long nrThrottled = Long.parseLong(cpuStatValues.getProperty("nr_throttled", "-1.0"));
          LOG.debug("cpu.stat nr_period value: " + nrPeriod);
          LOG.debug("cpu.stat nr_throttled value: " + nrThrottled);
          cpuThrottledRatio = (double) nrThrottled / nrPeriod;
          break;
        } catch (IOException | RuntimeException e) {
          throw new RuntimeException("Caught exception reading cpu.stat file: ", e);
        }
      }
    }
    return cpuThrottledRatio;
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


