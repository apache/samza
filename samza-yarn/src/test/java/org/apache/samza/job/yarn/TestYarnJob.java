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

import java.io.IOException;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.classloader.DependencyIsolationUtils;
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

import static org.junit.Assert.assertEquals;


public class TestYarnJob {
  @Test
  public void testBuildJobCoordinatorCmd() {
    // cluster-based job coordinator dependency isolation is not enabled; use script from __package directory
    Config config = new MapConfig();
    assertEquals("./__package/bin/run-jc.sh", YarnJob$.MODULE$.buildJobCoordinatorCmd(config, new JobConfig(config)));

    // split deployment is enabled; use script from framework infrastructure directory
    Config splitDeploymentEnabled =
        new MapConfig(ImmutableMap.of(JobConfig.JOB_SPLIT_DEPLOYMENT_ENABLED, "true"));
    assertEquals(String.format("./%s/bin/run-jc.sh", DependencyIsolationUtils.FRAMEWORK_INFRASTRUCTURE_DIRECTORY),
        YarnJob$.MODULE$.buildJobCoordinatorCmd(splitDeploymentEnabled, new JobConfig(splitDeploymentEnabled)));
  }

  @Test
  public void testBuildEnvironment() throws IOException {
    String amJvmOptions = "-Xmx1g -Dconfig.key='config value'";
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(JobConfig.JOB_NAME, "jobName")
        .put(JobConfig.JOB_ID, "jobId")
        .put(JobConfig.JOB_COORDINATOR_SYSTEM, "jobCoordinatorSystem")
        .put(YarnConfig.AM_JVM_OPTIONS, amJvmOptions) // needs escaping
        .put(JobConfig.JOB_SPLIT_DEPLOYMENT_ENABLED, "false")
        .build());
    String expectedCoordinatorStreamConfigStringValue = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG, expectedCoordinatorStreamConfigStringValue,
        ShellCommandConfig.ENV_JAVA_OPTS, Util.envVarEscape(amJvmOptions),
        ShellCommandConfig.ENV_SPLIT_DEPLOYMENT_ENABLED, "false",
        ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR, "");
    assertEquals(expected, JavaConverters.mapAsJavaMapConverter(
        YarnJob$.MODULE$.buildEnvironment(config, new YarnConfig(config), new JobConfig(config))).asJava());
  }

  @Test
  public void testBuildEnvironmentJobCoordinatorDependencyIsolationEnabled() throws IOException {
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(JobConfig.JOB_NAME, "jobName")
        .put(JobConfig.JOB_ID, "jobId")
        .put(JobConfig.JOB_COORDINATOR_SYSTEM, "jobCoordinatorSystem")
        .put(YarnConfig.AM_JVM_OPTIONS, "")
        .put(JobConfig.JOB_SPLIT_DEPLOYMENT_ENABLED, "true")
        .build());
    String expectedCoordinatorStreamConfigStringValue = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG, expectedCoordinatorStreamConfigStringValue,
        ShellCommandConfig.ENV_JAVA_OPTS, "",
        ShellCommandConfig.ENV_SPLIT_DEPLOYMENT_ENABLED, "true",
        ShellCommandConfig.ENV_APPLICATION_LIB_DIR, "./__package/lib",
        ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR, "");
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
        .put(JobConfig.JOB_SPLIT_DEPLOYMENT_ENABLED, "false")
        .put(YarnConfig.AM_JAVA_HOME, "/some/path/to/java/home")
        .build());
    String expectedCoordinatorStreamConfigStringValue = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG, expectedCoordinatorStreamConfigStringValue,
        ShellCommandConfig.ENV_JAVA_OPTS, "",
        ShellCommandConfig.ENV_SPLIT_DEPLOYMENT_ENABLED, "false",
        ShellCommandConfig.ENV_JAVA_HOME, "/some/path/to/java/home",
        ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR, "");
    assertEquals(expected, JavaConverters.mapAsJavaMapConverter(
        YarnJob$.MODULE$.buildEnvironment(config, new YarnConfig(config), new JobConfig(config))).asJava());
  }

  @Test
  public void testBuildJobSubmissionEnvironment() throws IOException {
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(JobConfig.JOB_NAME, "jobName")
        .put(JobConfig.JOB_ID, "jobId")
        .put(JobConfig.CONFIG_LOADER_FACTORY, "org.apache.samza.config.loaders.PropertiesConfigLoaderFactory")
        .put(YarnConfig.AM_JVM_OPTIONS, "")
        .put(JobConfig.JOB_SPLIT_DEPLOYMENT_ENABLED, "true")
        .build());
    String expectedSubmissionConfig = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(config));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_SUBMISSION_CONFIG, expectedSubmissionConfig,
        ShellCommandConfig.ENV_JAVA_OPTS, "",
        ShellCommandConfig.ENV_SPLIT_DEPLOYMENT_ENABLED, "true",
        ShellCommandConfig.ENV_APPLICATION_LIB_DIR, "./__package/lib",
        ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR, "");
    assertEquals(expected, JavaConverters.mapAsJavaMapConverter(
        YarnJob$.MODULE$.buildEnvironment(config, new YarnConfig(config), new JobConfig(config))).asJava());
  }

  @Test
  public void testBuildJobWithAdditionalClassPath() throws IOException {
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(JobConfig.JOB_NAME, "jobName")
        .put(JobConfig.JOB_ID, "jobId")
        .put(JobConfig.CONFIG_LOADER_FACTORY, "org.apache.samza.config.loaders.PropertiesConfigLoaderFactory")
        .put(YarnConfig.AM_JVM_OPTIONS, "")
        .put(JobConfig.JOB_SPLIT_DEPLOYMENT_ENABLED, "true")
        .put(ShellCommandConfig.ADDITIONAL_CLASSPATH_DIR, "./sqlapp/lib/*")
        .build());
    String expectedSubmissionConfig = Util.envVarEscape(SamzaObjectMapper.getObjectMapper()
        .writeValueAsString(config));
    Map<String, String> expected = ImmutableMap.of(
        ShellCommandConfig.ENV_SUBMISSION_CONFIG, expectedSubmissionConfig,
        ShellCommandConfig.ENV_JAVA_OPTS, "",
        ShellCommandConfig.ENV_SPLIT_DEPLOYMENT_ENABLED, "true",
        ShellCommandConfig.ENV_APPLICATION_LIB_DIR, "./__package/lib",
        ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR, "./sqlapp/lib/*");
    assertEquals(expected, JavaConverters.mapAsJavaMapConverter(
        YarnJob$.MODULE$.buildEnvironment(config, new YarnConfig(config), new JobConfig(config))).asJava());
  }
}