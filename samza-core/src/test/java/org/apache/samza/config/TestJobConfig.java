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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.samza.SamzaException;
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory;
import org.apache.samza.container.grouper.stream.HashSystemStreamPartitionMapperFactory;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamMetadataStoreFactory;
import org.apache.samza.runtime.DefaultLocationIdProviderFactory;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestJobConfig {
  @Test
  public void testGetName() {
    String jobName = "job-name";
    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_NAME, jobName)));
    assertEquals(Optional.of(jobName), jobConfig.getName());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getName());
  }

  @Test
  public void testGetCoordinatorSystemName() {
    String coordinatorSystemName = "coordinator-system", jobDefaultSystem = "job-default-system";

    // has job coordinator system and default system; choose job coordinator system
    JobConfig jobConfig = new JobConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_COORDINATOR_SYSTEM, coordinatorSystemName, JobConfig.JOB_DEFAULT_SYSTEM,
            jobDefaultSystem)));
    assertEquals(coordinatorSystemName, jobConfig.getCoordinatorSystemName());

    // has job coordinator system only; choose job coordinator system
    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_COORDINATOR_SYSTEM, coordinatorSystemName)));
    assertEquals(coordinatorSystemName, jobConfig.getCoordinatorSystemName());

    // has default system only; choose default system
    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEFAULT_SYSTEM, jobDefaultSystem)));
    assertEquals(jobDefaultSystem, jobConfig.getCoordinatorSystemName());

    try {
      new JobConfig(new MapConfig()).getCoordinatorSystemName();
      fail("Should have gotten a ConfigException");
    } catch (ConfigException e) {
      // expected
    }
  }

  @Test
  public void testGetCoordinatorSystemNameOrNull() {
    String coordinatorSystemName = "coordinator-system", jobDefaultSystem = "job-default-system";

    // has job coordinator system and default system; choose job coordinator system
    JobConfig jobConfig = new JobConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_COORDINATOR_SYSTEM, coordinatorSystemName, JobConfig.JOB_DEFAULT_SYSTEM,
            jobDefaultSystem)));
    assertEquals(coordinatorSystemName, jobConfig.getCoordinatorSystemNameOrNull());

    // has job coordinator system only; choose job coordinator system
    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_COORDINATOR_SYSTEM, coordinatorSystemName)));
    assertEquals(coordinatorSystemName, jobConfig.getCoordinatorSystemNameOrNull());

    // has default system only; choose default system
    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEFAULT_SYSTEM, jobDefaultSystem)));
    assertEquals(jobDefaultSystem, jobConfig.getCoordinatorSystemNameOrNull());

    jobConfig = new JobConfig(new MapConfig());
    assertNull(jobConfig.getCoordinatorSystemNameOrNull());
  }

  @Test
  public void testGetDefaultSystem() {
    String jobDefaultSystem = "job-default-system";

    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEFAULT_SYSTEM, jobDefaultSystem)));
    assertEquals(Optional.of(jobDefaultSystem), jobConfig.getDefaultSystem());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getDefaultSystem());
  }

  @Test
  public void testGetContainerCount() {
    int jobContainerCount = 10, yarnContainerCount = 5;

    // has job container count and yarn container count; choose job container count
    JobConfig jobConfig = new JobConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_CONTAINER_COUNT, Integer.toString(jobContainerCount), "yarn.container.count",
            Integer.toString(yarnContainerCount))));
    assertEquals(jobContainerCount, jobConfig.getContainerCount());

    // has job container count only; choose job container count
    jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.JOB_CONTAINER_COUNT, Integer.toString(jobContainerCount))));
    assertEquals(jobContainerCount, jobConfig.getContainerCount());

    // has yarn container count only; choose yarn container count
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of("yarn.container.count", Integer.toString(yarnContainerCount))));
    assertEquals(yarnContainerCount, jobConfig.getContainerCount());

    // not specified; use default
    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_JOB_CONTAINER_COUNT, jobConfig.getContainerCount());
  }

  @Test
  public void testGetMonitorRegexDisabled() {
    // positive means enabled
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.MONITOR_INPUT_REGEX_FREQUENCY_MS, Integer.toString(100))));
    assertFalse(jobConfig.getMonitorRegexDisabled());

    // zero means disabled
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.MONITOR_INPUT_REGEX_FREQUENCY_MS, Integer.toString(0))));
    assertTrue(jobConfig.getMonitorRegexDisabled());

    // negative means disabled
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.MONITOR_INPUT_REGEX_FREQUENCY_MS, Integer.toString(-1))));
    assertTrue(jobConfig.getMonitorRegexDisabled());

    // not specified uses the default monitor partition change frequency, which means enabled
    jobConfig = new JobConfig(new MapConfig());
    assertFalse(jobConfig.getMonitorRegexDisabled());
  }

  @Test
  public void testGetMonitorPartitionChangeFrequency() {
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.MONITOR_PARTITION_CHANGE_FREQUENCY_MS, Integer.toString(100))));
    assertEquals(100, jobConfig.getMonitorPartitionChangeFrequency());

    jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.MONITOR_PARTITION_CHANGE_FREQUENCY_MS, Integer.toString(0))));
    assertEquals(0, jobConfig.getMonitorPartitionChangeFrequency());

    jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.MONITOR_PARTITION_CHANGE_FREQUENCY_MS, Integer.toString(-1))));
    assertEquals(-1, jobConfig.getMonitorPartitionChangeFrequency());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_MONITOR_PARTITION_CHANGE_FREQUENCY_MS,
        jobConfig.getMonitorPartitionChangeFrequency());
  }

  @Test
  public void testGetMonitorRegexFrequency() {
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.MONITOR_INPUT_REGEX_FREQUENCY_MS, Integer.toString(100))));
    assertEquals(100, jobConfig.getMonitorRegexFrequency());

    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.MONITOR_INPUT_REGEX_FREQUENCY_MS, Integer.toString(0))));
    assertEquals(0, jobConfig.getMonitorRegexFrequency());

    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.MONITOR_INPUT_REGEX_FREQUENCY_MS, Integer.toString(-1))));
    assertEquals(-1, jobConfig.getMonitorRegexFrequency());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_MONITOR_INPUT_REGEX_FREQUENCY_MS, jobConfig.getMonitorRegexFrequency());
  }

  @Test
  public void testGetMonitorRegexPatternMap() {
    // 2 different rewriters to system0, 1 rewriter to system1, 1 rewriter which isn't in rewritersList
    String system0 = "system0", system1 = "system1";
    String system0Rewriter0 = "system-0-rewriter-0", system0Rewriter1 = "system-0-rewriter-1", system1Rewriter =
        "system-1-rewriter", systemOnlyRewriter = "system-only-rewriter";
    String system0Rewriter0Streams = "system-0-rewriter-0-.*", system0Rewriter1Streams = "system-0-rewriter-1-.*",
        system1RewriterStreams = "system-1-rewriter-.*";
    JobConfig jobConfig = new JobConfig(new MapConfig(
        new ImmutableMap.Builder<String, String>().put(String.format(JobConfig.REGEX_RESOLVED_SYSTEM, system0Rewriter0),
            system0)
            .put(String.format(JobConfig.REGEX_RESOLVED_STREAMS, system0Rewriter0), system0Rewriter0Streams)
            .put(String.format(JobConfig.REGEX_RESOLVED_SYSTEM, system0Rewriter1), system0)
            .put(String.format(JobConfig.REGEX_RESOLVED_STREAMS, system0Rewriter1), system0Rewriter1Streams)
            .put(String.format(JobConfig.REGEX_RESOLVED_SYSTEM, system1Rewriter), system1)
            .put(String.format(JobConfig.REGEX_RESOLVED_STREAMS, system1Rewriter), system1RewriterStreams)
            // not passed in as a rewriter when calling getMonitorRegexPatternMap
            .put(String.format(JobConfig.REGEX_RESOLVED_SYSTEM, "unused-rewriter"), system0)
            .put(String.format(JobConfig.REGEX_RESOLVED_STREAMS, "unused-rewriter"), "unused-rewriter-.*")
            // should not be included since there is no regex
            .put(String.format(JobConfig.REGEX_RESOLVED_SYSTEM, systemOnlyRewriter), system0)
            .build()));
    // Pattern.equals only checks that the references are the same, so can't compare maps directly
    Map<String, Pattern> actual = jobConfig.getMonitorRegexPatternMap(String.join(",",
        ImmutableList.of(system0Rewriter0, system0Rewriter1, system1Rewriter, systemOnlyRewriter,
            "not-a-regex-rewriter")));
    // only should have rewriters for system0 and system1
    assertEquals(2, actual.size());
    assertEquals(system0Rewriter0Streams + "|" + system0Rewriter1Streams, actual.get(system0).pattern());
    assertEquals(system1RewriterStreams, actual.get(system1).pattern());

    // empty configs should produce an empty map
    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Collections.<String, Pattern>emptyMap(), jobConfig.getMonitorRegexPatternMap(system0Rewriter0));
    assertEquals(Collections.<String, Pattern>emptyMap(), jobConfig.getMonitorRegexPatternMap(""));
  }

  @Test
  public void testGetRegexResolvedStreams() {
    String rewriterName = "rewriter-name", regex = "my-stream-.*";
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(String.format(JobConfig.REGEX_RESOLVED_STREAMS, rewriterName), regex)));
    assertEquals(Optional.of(regex), jobConfig.getRegexResolvedStreams(rewriterName));
    assertEquals(Optional.empty(), jobConfig.getRegexResolvedStreams("other-rewriter"));
  }

  @Test
  public void testGetRegexResolvedSystem() {
    String rewriterName = "rewriter-name", regex = "my-system-.*";
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(String.format(JobConfig.REGEX_RESOLVED_SYSTEM, rewriterName), regex)));
    assertEquals(Optional.of(regex), jobConfig.getRegexResolvedSystem(rewriterName));
    assertEquals(Optional.empty(), jobConfig.getRegexResolvedSystem("other-rewriter"));
  }

  @Test
  public void testGetRegexResolvedInheritedConfig() {
    String rewriterName = "rewriter-name";
    String key0 = "key0", value0 = "value0", key1 = "other.key1", value1 = "value1";
    JobConfig jobConfig = new JobConfig(new MapConfig(
        ImmutableMap.of(String.format(JobConfig.REGEX_INHERITED_CONFIG + "." + key0, rewriterName), value0,
            String.format(JobConfig.REGEX_INHERITED_CONFIG + "." + key1, rewriterName), value1)));
    assertEquals(new MapConfig(ImmutableMap.of(key0, value0, key1, value1)),
        jobConfig.getRegexResolvedInheritedConfig(rewriterName));
    assertEquals(new MapConfig(), jobConfig.getRegexResolvedInheritedConfig("other-rewriter"));
  }

  @Test
  public void testGetStreamJobFactoryClass() {
    String jobFactoryClass = "my.job.Factory.class";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.STREAM_JOB_FACTORY_CLASS, jobFactoryClass)));
    assertEquals(Optional.of(jobFactoryClass), jobConfig.getStreamJobFactoryClass());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getStreamJobFactoryClass());
  }

  @Test
  public void testGetJobId() {
    String jobId = "job-id";

    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_ID, jobId)));
    assertEquals(jobId, jobConfig.getJobId());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_JOB_ID, jobConfig.getJobId());
  }

  @Test
  public void testFailOnCheckpointValidation() {
    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_FAIL_CHECKPOINT_VALIDATION, "true")));
    assertTrue(jobConfig.failOnCheckpointValidation());

    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_FAIL_CHECKPOINT_VALIDATION, "false")));
    assertFalse(jobConfig.failOnCheckpointValidation());

    jobConfig = new JobConfig(new MapConfig());
    assertTrue(jobConfig.failOnCheckpointValidation());
  }

  @Test
  public void testGetConfigRewriters() {
    String configRewriters = "rewriter0,rewriter1";

    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.CONFIG_REWRITERS, configRewriters)));
    assertEquals(Optional.of(configRewriters), jobConfig.getConfigRewriters());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getConfigRewriters());
  }

  @Test
  public void testGetConfigRewriterClass() {
    String rewriterName = "rewriter-name", className = "my.Rewriter.class";
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(String.format(JobConfig.CONFIG_REWRITER_CLASS, rewriterName), className)));
    assertEquals(Optional.of(className), jobConfig.getConfigRewriterClass(rewriterName));
    assertEquals(Optional.empty(), jobConfig.getConfigRewriterClass("other-rewriter"));
  }

  @Test
  public void testGetSystemStreamPartitionGrouperFactory() {
    String sspGrouperFactory = "my.ssp.grouper.Factory.class";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.SSP_GROUPER_FACTORY, sspGrouperFactory)));
    assertEquals(sspGrouperFactory, jobConfig.getSystemStreamPartitionGrouperFactory());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(GroupByPartitionFactory.class.getName(), jobConfig.getSystemStreamPartitionGrouperFactory());
  }

  @Test
  public void testGetLocationIdProviderFactory() {
    String locationIdProviderFactory = "my.location.id.provider.Factory.class";

    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.LOCATION_ID_PROVIDER_FACTORY, locationIdProviderFactory)));
    assertEquals(locationIdProviderFactory, jobConfig.getLocationIdProviderFactory());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(DefaultLocationIdProviderFactory.class.getName(), jobConfig.getLocationIdProviderFactory());
  }

  @Test
  public void testGetSecurityManagerFactory() {
    String securityManagerFactory = "my.security.manager.factory";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_SECURITY_MANAGER_FACTORY, securityManagerFactory)));
    assertEquals(Optional.of(securityManagerFactory), jobConfig.getSecurityManagerFactory());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getSecurityManagerFactory());
  }

  @Test
  public void testGetSSPMatcherClass() {
    String sspMatcherClass = "my.ssp.Matcher.class";

    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.SSP_MATCHER_CLASS, sspMatcherClass)));
    assertEquals(Optional.of(sspMatcherClass), jobConfig.getSSPMatcherClass());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getSSPMatcherClass());
  }

  @Test
  public void testGetSSPMatcherConfigRegex() {
    String sspMatcherConfigRegex = "ssp-.*";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.SSP_MATCHER_CONFIG_REGEX, sspMatcherConfigRegex)));
    assertEquals(sspMatcherConfigRegex, jobConfig.getSSPMatcherConfigRegex());

    try {
      new JobConfig(new MapConfig()).getSSPMatcherConfigRegex();
      fail("Expected a SamzaException");
    } catch (SamzaException e) {
      // expected
    }
  }

  @Test
  public void testGetSSPMatcherConfigRanges() {
    String sspMatcherConfigRanges = "1,2,3";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.SSP_MATCHER_CONFIG_RANGES, sspMatcherConfigRanges)));
    assertEquals(sspMatcherConfigRanges, jobConfig.getSSPMatcherConfigRanges());

    try {
      new JobConfig(new MapConfig()).getSSPMatcherConfigRanges();
      fail("Expected a SamzaException");
    } catch (SamzaException e) {
      // expected
    }
  }

  @Test
  public void testGetSSPMatcherConfigJobFactoryRegex() {
    String sspMatcherConfigJobFactoryRegex = ".*JobFactory";

    JobConfig jobConfig = new JobConfig(new MapConfig(
        ImmutableMap.of(JobConfig.SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX, sspMatcherConfigJobFactoryRegex)));
    assertEquals(sspMatcherConfigJobFactoryRegex, jobConfig.getSSPMatcherConfigJobFactoryRegex());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX,
        jobConfig.getSSPMatcherConfigJobFactoryRegex());
  }

  @Test
  public void testGetThreadPoolSize() {
    JobConfig jobConfig = new JobConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE, "10")));
    assertEquals(10, jobConfig.getThreadPoolSize());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(0, jobConfig.getThreadPoolSize());
  }

  @Test
  public void testGetDebounceTimeMs() {
    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEBOUNCE_TIME_MS, Integer.toString(100))));
    assertEquals(100, jobConfig.getDebounceTimeMs());

    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEBOUNCE_TIME_MS, Integer.toString(0))));
    assertEquals(0, jobConfig.getDebounceTimeMs());

    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEBOUNCE_TIME_MS, Integer.toString(-1))));
    assertEquals(-1, jobConfig.getDebounceTimeMs());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_DEBOUNCE_TIME_MS, jobConfig.getDebounceTimeMs());
  }

  @Test
  public void testGetNonLoggedStorePath() {
    String nonLoggedStorePath = "/path/to/non/logged/store";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_NON_LOGGED_STORE_BASE_DIR, nonLoggedStorePath)));
    assertEquals(Optional.of(nonLoggedStorePath), jobConfig.getNonLoggedStorePath());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getNonLoggedStorePath());
  }

  @Test
  public void testGetLoggedStorePath() {
    String loggedStorePath = "/path/to/logged/store";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_LOGGED_STORE_BASE_DIR, loggedStorePath)));
    assertEquals(Optional.of(loggedStorePath), jobConfig.getLoggedStorePath());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(Optional.empty(), jobConfig.getLoggedStorePath());
  }

  @Test
  public void testGetMetadataStoreFactory() {
    String metadataStoreFactory = "my.metadata.store.Factory.class";

    JobConfig jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.METADATA_STORE_FACTORY, metadataStoreFactory)));
    assertEquals(metadataStoreFactory, jobConfig.getMetadataStoreFactory());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(CoordinatorStreamMetadataStoreFactory.class.getName(), jobConfig.getMetadataStoreFactory());
  }

  @Test
  public void testGetDiagnosticsEnabled() {
    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DIAGNOSTICS_ENABLED, "true")));
    assertTrue(jobConfig.getDiagnosticsEnabled());

    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DIAGNOSTICS_ENABLED, "false")));
    assertFalse(jobConfig.getDiagnosticsEnabled());

    jobConfig = new JobConfig(new MapConfig());
    assertFalse(jobConfig.getDiagnosticsEnabled());
  }

  @Test
  public void testGetJMXEnabled() {
    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_JMX_ENABLED, "true")));
    assertTrue(jobConfig.getJMXEnabled());

    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_JMX_ENABLED, "false")));
    assertFalse(jobConfig.getJMXEnabled());

    jobConfig = new JobConfig(new MapConfig());
    assertTrue(jobConfig.getJMXEnabled());
  }

  @Test
  public void testGetSystemStreamPartitionMapperFactoryName() {
    String sspMapperFactory = "my.ssp.mapper.Factory.class";

    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.SYSTEM_STREAM_PARTITION_MAPPER_FACTORY, sspMapperFactory)));
    assertEquals(sspMapperFactory, jobConfig.getSystemStreamPartitionMapperFactoryName());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(HashSystemStreamPartitionMapperFactory.class.getName(),
        jobConfig.getSystemStreamPartitionMapperFactoryName());
  }

  @Test
  public void testGetStandbyTasksEnabled() {
    // greater than 1 means enabled
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, Integer.toString(100))));
    assertTrue(jobConfig.getStandbyTasksEnabled());

    // zero means disabled
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, Integer.toString(0))));
    assertFalse(jobConfig.getStandbyTasksEnabled());

    // negative means disabled
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, Integer.toString(-1))));
    assertFalse(jobConfig.getStandbyTasksEnabled());

    // not specified uses the default standby count, which means disabled
    jobConfig = new JobConfig(new MapConfig());
    assertFalse(jobConfig.getStandbyTasksEnabled());
  }

  @Test
  public void testGetStandbyTaskReplicationFactor() {
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, Integer.toString(100))));
    assertEquals(100, jobConfig.getStandbyTaskReplicationFactor());

    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, Integer.toString(0))));
    assertEquals(0, jobConfig.getStandbyTaskReplicationFactor());

    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.STANDBY_TASKS_REPLICATION_FACTOR, Integer.toString(-1))));
    assertEquals(-1, jobConfig.getStandbyTaskReplicationFactor());

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_STANDBY_TASKS_REPLICATION_FACTOR, jobConfig.getStandbyTaskReplicationFactor());
  }

  @Test
  public void testGetMetadataFile() {
    String execEnvContainerId = "container-id";
    String containerMetadataDirectory = "/tmp/samza/log/dir";
    System.setProperty(JobConfig.CONTAINER_METADATA_DIRECTORY_SYS_PROPERTY, containerMetadataDirectory);
    assertEquals(new File(containerMetadataDirectory,
            String.format(JobConfig.CONTAINER_METADATA_FILENAME_FORMAT, execEnvContainerId)).getPath(),
        JobConfig.getMetadataFile(execEnvContainerId).get().getPath());
    System.clearProperty(JobConfig.CONTAINER_METADATA_DIRECTORY_SYS_PROPERTY);

    assertEquals(Optional.empty(), JobConfig.getMetadataFile(null));
  }

  @Test
  public void testGetCoordinatorStreamFactory() {
    JobConfig jobConfig = new JobConfig(new MapConfig(ImmutableMap.of("test", "")));
    assertEquals(jobConfig.getCoordinatorStreamFactory(), JobConfig.DEFAULT_COORDINATOR_STREAM_CONFIG_FACTORY);

    jobConfig = new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.COORDINATOR_STREAM_FACTORY, "specific_coordinator_stream")));
    assertEquals(jobConfig.getCoordinatorStreamFactory(), "specific_coordinator_stream");
  }

  @Test
  public void testAutosizingConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("job.autosizing.enabled", "true");
    config.put("job.container.count", "1");
    config.put("job.autosizing.container.count", "2");
    config.put("job.container.thread.pool.size", "1");
    config.put("job.autosizing.container.thread.pool.size", "3");
    config.put("job.autosizing.container.maxheap.mb", "500");

    config.put("cluster-manager.container.memory.mb", "500");
    config.put("job.autosizing.container.memory.mb", "900");
    config.put("cluster-manager.container.cpu.cores", "1");
    config.put("job.autosizing.container.cpu.cores", "2");
    JobConfig jobConfig = new JobConfig(new MapConfig(config));
    Assert.assertTrue(jobConfig.getAutosizingEnabled());
    Assert.assertEquals(2, jobConfig.getContainerCount());
    Assert.assertEquals(3, jobConfig.getThreadPoolSize());

    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(new MapConfig(config));
    Assert.assertEquals(900, clusterManagerConfig.getContainerMemoryMb());
    Assert.assertEquals(2, clusterManagerConfig.getNumCores());
  }

  @Test
  public void testGetContainerHeartbeatMonitorEnabled() {
    assertTrue(new JobConfig(new MapConfig()).getContainerHeartbeatMonitorEnabled());
    assertTrue(new JobConfig(new MapConfig(
        ImmutableMap.of(JobConfig.CONTAINER_HEARTBEAT_MONITOR_ENABLED, "true"))).getContainerHeartbeatMonitorEnabled());
    assertFalse(new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.CONTAINER_HEARTBEAT_MONITOR_ENABLED,
        "false"))).getContainerHeartbeatMonitorEnabled());
  }

  @Test
  public void testGetElastictyEnabled() {
    // greater than 1 means enabled
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(2))));
    assertTrue(jobConfig.getElasticityEnabled());

    // one means disabled
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(1))));
    assertFalse(jobConfig.getElasticityEnabled());

    // zero means disabled
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(0))));
    boolean exceptionCaught = false;
    try {
      jobConfig.getElasticityEnabled();
    } catch (ConfigException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);

    // negative means disabled
    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(-1))));
    exceptionCaught = false;
    try {
      jobConfig.getElasticityEnabled();
    } catch (ConfigException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);

    // not specified uses the default standby count, which means disabled
    jobConfig = new JobConfig(new MapConfig());
    assertFalse(jobConfig.getElasticityEnabled());
  }

  @Test
  public void testGetElasticityFactor() {
    JobConfig jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(2))));
    assertEquals(2, jobConfig.getElasticityFactor());

    jobConfig = new JobConfig(
        new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(1))));
    assertEquals(1, jobConfig.getElasticityFactor());

    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(0))));
    boolean exceptionCaught = false;
    try {
      jobConfig.getElasticityFactor();
    } catch (ConfigException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);

    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(-1))));
    exceptionCaught = false;
    try {
      jobConfig.getElasticityFactor();
    } catch (ConfigException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);

    jobConfig =
        new JobConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_ELASTICITY_FACTOR, Integer.toString(17))));
    exceptionCaught = false;
    try {
      jobConfig.getElasticityFactor();
    } catch (ConfigException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);

    jobConfig = new JobConfig(new MapConfig());
    assertEquals(JobConfig.DEFAULT_JOB_ELASTICITY_FACTOR, jobConfig.getElasticityFactor());
  }
}