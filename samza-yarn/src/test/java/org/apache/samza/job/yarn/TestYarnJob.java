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
package org.apache.samza.job.yarn;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.samza.classloader.IsolationUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.YarnConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.Util;
import org.junit.Test;
import scala.collection.JavaConverters;

import static org.junit.Assert.*;


public class TestYarnJob {
  @Test
  public void testBuildAmCmd() {
    // application master isolation is not enabled; use script from __package directory
    Config config = new MapConfig();
    assertEquals("./__package/bin/run-jc.sh", YarnJob$.MODULE$.buildAmCmd(config, new JobConfig(config)));

    // application master isolation is enabled; use script from framework infrastructure directory
    Config configApplicationMasterIsolationEnabled =
        new MapConfig(ImmutableMap.of(JobConfig.SAMZA_APPLICATION_MASTER_ISOLATION_ENABLED, "true"));
    assertEquals(String.format("./%s/bin/run-jc.sh", IsolationUtils.APPLICATION_MASTER_INFRASTRUCTURE_DIRECTORY),
        YarnJob$.MODULE$.buildAmCmd(configApplicationMasterIsolationEnabled,
            new JobConfig(configApplicationMasterIsolationEnabled)));
  }

  @Test
  public void testBuildEnvironment() throws IOException {
    String amJvmOptions = "-Xmx1g -Dconfig.key='config value'";
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(JobConfig.JOB_NAME, "jobName")
        .put(JobConfig.JOB_ID, "jobId")
        .put(JobConfig.JOB_COORDINATOR_SYSTEM, "jobCoordinatorSystem")
        .put(YarnConfig.AM_JVM_OPTIONS, amJvmOptions) // needs escaping
        .put(JobConfig.SAMZA_APPLICATION_MASTER_ISOLATION_ENABLED, "false")
        .build());
    String expectedCoordinatorStreamConfigStringValue = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG(), expectedCoordinatorStreamConfigStringValue,
        ShellCommandConfig.ENV_JAVA_OPTS(), Util.envVarEscape(amJvmOptions),
        ShellCommandConfig.ENV_APPLICATION_MASTER_ISOLATION_ENABLED(), "false");
    assertEquals(expected, JavaConverters.mapAsJavaMapConverter(
        YarnJob$.MODULE$.buildEnvironment(config, new YarnConfig(config), new JobConfig(config))).asJava());
  }

  @Test
  public void testBuildEnvironmentApplicationMasterIsolationEnabled() throws IOException {
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(JobConfig.JOB_NAME, "jobName")
        .put(JobConfig.JOB_ID, "jobId")
        .put(JobConfig.JOB_COORDINATOR_SYSTEM, "jobCoordinatorSystem")
        .put(YarnConfig.AM_JVM_OPTIONS, "")
        .put(JobConfig.SAMZA_APPLICATION_MASTER_ISOLATION_ENABLED, "true")
        .build());
    String expectedCoordinatorStreamConfigStringValue = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG(), expectedCoordinatorStreamConfigStringValue,
        ShellCommandConfig.ENV_JAVA_OPTS(), "",
        ShellCommandConfig.ENV_APPLICATION_MASTER_ISOLATION_ENABLED(), "true",
        ShellCommandConfig.ENV_APPLICATION_LIB_DIR(), "./__package/lib");
    assertEquals(expected, JavaConverters.mapAsJavaMapConverter(
        YarnJob$.MODULE$.buildEnvironment(config, new YarnConfig(config), new JobConfig(config))).asJava());
  }

  @Test
  public void testBuildEnvironmentWithAMJavaHome() throws IOException {
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(JobConfig.JOB_NAME, "jobName")
        .put(JobConfig.JOB_ID, "jobId")
        .put(JobConfig.JOB_COORDINATOR_SYSTEM, "jobCoordinatorSystem")
        .put(YarnConfig.AM_JVM_OPTIONS, "")
        .put(JobConfig.SAMZA_APPLICATION_MASTER_ISOLATION_ENABLED, "false")
        .put(YarnConfig.AM_JAVA_HOME, "/some/path/to/java/home")
        .build());
    String expectedCoordinatorStreamConfigStringValue = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG(), expectedCoordinatorStreamConfigStringValue,
        ShellCommandConfig.ENV_JAVA_OPTS(), "",
        ShellCommandConfig.ENV_APPLICATION_MASTER_ISOLATION_ENABLED(), "false",
        ShellCommandConfig.ENV_JAVA_HOME(), "/some/path/to/java/home");
    assertEquals(expected, JavaConverters.mapAsJavaMapConverter(
        YarnJob$.MODULE$.buildEnvironment(config, new YarnConfig(config), new JobConfig(config))).asJava());
  }
}