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
package org.apache.samza.config;

import java.util.Optional;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestShellCommandConfig {
  @Test
  public void testGetCommand() {
    ShellCommandConfig shellCommandConfig = new ShellCommandConfig(new MapConfig());
    assertEquals("bin/run-container.sh", shellCommandConfig.getCommand());

    shellCommandConfig = new ShellCommandConfig(
        new MapConfig(ImmutableMap.of(ShellCommandConfig.COMMAND_SHELL_EXECUTE, "my-run-container.sh")));
    assertEquals("my-run-container.sh", shellCommandConfig.getCommand());
  }

  @Test
  public void testGetTaskOptsAutosizingDisabled() {
    ShellCommandConfig shellCommandConfig =
        new ShellCommandConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "false")));
    assertEquals(Optional.empty(), shellCommandConfig.getTaskOpts());

    String taskOpts = "-Dproperty=value";
    shellCommandConfig = new ShellCommandConfig(new MapConfig(
        ImmutableMap.of(ShellCommandConfig.TASK_JVM_OPTS, taskOpts, JobConfig.JOB_AUTOSIZING_ENABLED, "false")));
    assertEquals(Optional.of(taskOpts), shellCommandConfig.getTaskOpts());
  }

  @Test
  public void testGetTaskOptsAutosizingEnabled() {
    // opts not set, autosizing max heap not set
    ShellCommandConfig shellCommandConfig =
        new ShellCommandConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "true")));
    assertEquals(Optional.empty(), shellCommandConfig.getTaskOpts());

    // opts set, autosizing max heap not set
    String taskOpts = "-Dproperty=value";
    shellCommandConfig = new ShellCommandConfig(new MapConfig(
        ImmutableMap.of(ShellCommandConfig.TASK_JVM_OPTS, taskOpts, JobConfig.JOB_AUTOSIZING_ENABLED, "true")));
    assertEquals(Optional.of(taskOpts), shellCommandConfig.getTaskOpts());

    // opts not set, autosizing max heap set
    shellCommandConfig = new ShellCommandConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "true", JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_HEAP_MB,
            "1024")));
    assertEquals(Optional.of("-Xmx1024m"), shellCommandConfig.getTaskOpts());

    // opts set with Xmx, autosizing max heap set
    shellCommandConfig = new ShellCommandConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "true", JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_HEAP_MB,
            "1024", "task.opts", "-Xmx10m -Dproperty=value")));
    assertEquals(Optional.of("-Xmx1024m -Dproperty=value"), shellCommandConfig.getTaskOpts());

    // opts set without -Xmx, autosizing max heap set
    shellCommandConfig = new ShellCommandConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "true", JobConfig.JOB_AUTOSIZING_CONTAINER_MAX_HEAP_MB,
            "1024", "task.opts", "-Dproperty=value")));
    assertEquals(Optional.of("-Dproperty=value -Xmx1024m"), shellCommandConfig.getTaskOpts());
  }

  @Test
  public void testGetWorkerOptsAutosizingDisabled() {
    ShellCommandConfig shellCommandConfig = new ShellCommandConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_AUTOSIZING_WORKER_MAX_HEAP_MB,
            "1024", "worker.opts", "-Xmx10m -Dproperty=value")));

    String workerOpts = shellCommandConfig.getWorkerOpts()
        .orElse(null);
    String expectedOpts = "-Xmx10m -Dproperty=value";

    assertNotNull(workerOpts);
    assertEquals(expectedOpts, workerOpts);
  }

  @Test
  public void testGetWorkerOptsAutosizingEnabled() {
    ShellCommandConfig shellCommandConfig = new ShellCommandConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "true", JobConfig.JOB_AUTOSIZING_WORKER_MAX_HEAP_MB,
            "1024", "worker.opts", "-Xmx10m -Dproperty=value")));

    String workerOpts = shellCommandConfig.getWorkerOpts()
        .orElse(null);
    String expectedOpts = "-Xmx1024m -Dproperty=value";

    assertNotNull(workerOpts);
    assertEquals(expectedOpts, workerOpts);
  }

  @Test
  public void testGetFinalJvmOptionsAutosizingDisabled() {
    ShellCommandConfig shellCommandConfig =
        new ShellCommandConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "false")));
    String jvmOptions = "";
    String expectedJvmOptions = "";

    // no override passed
    assertEquals(expectedJvmOptions, shellCommandConfig.getFinalJvmOptions(jvmOptions, ""));

    // ignore override since autosizing is disabled
    assertEquals(expectedJvmOptions, shellCommandConfig.getFinalJvmOptions(jvmOptions, "2048"));
  }

  @Test
  public void testGetFinalJvmOptionsAutosizingEnabled() {
    ShellCommandConfig shellCommandConfig =
        new ShellCommandConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_AUTOSIZING_ENABLED, "true")));
    String jvmOptions = "-Xmx1024m";
    String expectedJvmOptions = "-Xmx1024m";
    assertEquals(expectedJvmOptions, shellCommandConfig.getFinalJvmOptions(jvmOptions, ""));

    // override should take effect with autosizing enabled
    expectedJvmOptions = "-Xmx2048m";
    assertEquals(expectedJvmOptions, shellCommandConfig.getFinalJvmOptions(jvmOptions, "2048"));

    // override should take effect even if xmx is not set
    jvmOptions = "-Dproperty=value";
    expectedJvmOptions = "-Dproperty=value -Xmx2048m";
    assertEquals(expectedJvmOptions, shellCommandConfig.getFinalJvmOptions(jvmOptions, "2048"));
  }

  @Test
  public void testGetJavaHome() {
    ShellCommandConfig shellCommandConfig = new ShellCommandConfig(new MapConfig());
    assertFalse(shellCommandConfig.getJavaHome().isPresent());

    shellCommandConfig =
        new ShellCommandConfig(new MapConfig(ImmutableMap.of(ShellCommandConfig.TASK_JAVA_HOME, "/location/java")));
    assertEquals(Optional.of("/location/java"), shellCommandConfig.getJavaHome());
  }

  @Test
  public void testGetAdditionalClasspathDir() {
    ShellCommandConfig shellCommandConfig = new ShellCommandConfig(new MapConfig());
    assertFalse(shellCommandConfig.getAdditionalClasspathDir().isPresent());

    shellCommandConfig = new ShellCommandConfig(
        new MapConfig(ImmutableMap.of(ShellCommandConfig.ADDITIONAL_CLASSPATH_DIR, "/location/classpath")));
    assertEquals(Optional.of("/location/classpath"), shellCommandConfig.getAdditionalClasspathDir());
  }
}