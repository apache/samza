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

package org.apache.samza.job;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestShellCommandBuilder {
  private static final String URL_STRING = "http://www.google.com";

  @Test
  public void testBasicBuild() throws MalformedURLException {
    Config config = new MapConfig(ImmutableMap.of(ShellCommandConfig.COMMAND_SHELL_EXECUTE, "foo"));
    ShellCommandBuilder shellCommandBuilder = new ShellCommandBuilder();
    shellCommandBuilder.setConfig(config);
    shellCommandBuilder.setId("1");
    shellCommandBuilder.setUrl(new URL(URL_STRING));
    Map<String, String> expectedEnvironment = ImmutableMap.of(
        ShellCommandConfig.ENV_CONTAINER_ID, "1",
        ShellCommandConfig.ENV_COORDINATOR_URL, URL_STRING,
        ShellCommandConfig.ENV_JAVA_OPTS, "",
        ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR, "");
    // assertions when command path is not set
    assertEquals("foo", shellCommandBuilder.buildCommand());
    assertEquals(expectedEnvironment, shellCommandBuilder.buildEnvironment());
    // assertions when command path is set to empty string
    shellCommandBuilder.setCommandPath("");
    assertEquals("foo", shellCommandBuilder.buildCommand());
    assertEquals(expectedEnvironment, shellCommandBuilder.buildEnvironment());
  }

  @Test
  public void testBuildEnvironment() throws MalformedURLException {
    Config config = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put(ShellCommandConfig.COMMAND_SHELL_EXECUTE, "foo")
        .put(ShellCommandConfig.TASK_JVM_OPTS, "-Xmx4g")
        .put(ShellCommandConfig.ADDITIONAL_CLASSPATH_DIR, "/path/to/additional/classpath")
        .put(ShellCommandConfig.TASK_JAVA_HOME, "/path/to/java/home")
        .build());
    ShellCommandBuilder shellCommandBuilder = new ShellCommandBuilder();
    shellCommandBuilder.setConfig(config);
    shellCommandBuilder.setId("1");
    shellCommandBuilder.setUrl(new URL(URL_STRING));
    Map<String, String> expectedEnvironment = new ImmutableMap.Builder<String, String>()
        .put(ShellCommandConfig.ENV_CONTAINER_ID, "1")
        .put(ShellCommandConfig.ENV_COORDINATOR_URL, URL_STRING)
        .put(ShellCommandConfig.ENV_JAVA_OPTS, "-Xmx4g")
        .put(ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR, "/path/to/additional/classpath")
        .put(ShellCommandConfig.ENV_JAVA_HOME, "/path/to/java/home")
        .build();
    assertEquals(expectedEnvironment, shellCommandBuilder.buildEnvironment());
  }

  @Test
  public void testBuildCommandWithCommandPath() throws MalformedURLException {
    Config config = new MapConfig(ImmutableMap.of(ShellCommandConfig.COMMAND_SHELL_EXECUTE, "foo"));
    ShellCommandBuilder shellCommandBuilder = new ShellCommandBuilder();
    shellCommandBuilder.setConfig(config);
    shellCommandBuilder.setId("1");
    shellCommandBuilder.setUrl(new URL(URL_STRING));
    shellCommandBuilder.setCommandPath("/package/path");
    assertEquals("/package/path/foo", shellCommandBuilder.buildCommand());
  }
}