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
package org.apache.samza.coordinator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.RegExTopicGenerator;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory;
import org.apache.samza.container.grouper.task.GroupByContainerCount;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.TaskNameGrouper;
import org.apache.samza.container.grouper.task.TaskNameGrouperFactory;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionMatcher;
import org.apache.samza.util.ConfigUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.collection.JavaConverters;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unfortunately, we still need powermock for testing the regex topic flow, since that flow checks for a specific
 * rewriter class, and setting up the support for that specific class is too cumbersome.
 * Many of the tests do not need powermock.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ConfigUtil.class})
public class TestJobModelCalculator {
  private static final String REGEX_REWRITER0 = "regexRewriter0";
  private static final String REGEX_REWRITER1 = "regexRewriter1";
  private static final SystemStream SYSTEM_STREAM0 = new SystemStream("system0", "stream0");
  private static final SystemStream SYSTEM_STREAM1 = new SystemStream("system1", "stream1");

  @Mock
  private StreamMetadataCache streamMetadataCache;
  @Mock
  private GrouperMetadata grouperMetadata;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    addStreamMetadataCacheMetadata(this.streamMetadataCache,
        ImmutableMap.of(SYSTEM_STREAM0, buildSystemStreamMetadata(4), SYSTEM_STREAM1, buildSystemStreamMetadata(3)));
  }

  @Test
  public void testBasicSingleStream() {
    addStreamMetadataCacheMetadata(this.streamMetadataCache,
        ImmutableMap.of(SYSTEM_STREAM0, buildSystemStreamMetadata(4)));
    Map<TaskName, Integer> changeLogPartitionMapping = changelogPartitionMapping(4);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0), ImmutableMap.of());
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0), taskName(2), taskModel(2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1), taskName(3), taskModel(3, 3))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changeLogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testBasicMultipleStreams() {
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(4);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1), ImmutableMap.of());
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testCustomSSPGrouper() {
    // custom grouper only groups into two tasks, so only need 2 changelog partitions
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(2);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1),
        ImmutableMap.of(JobConfig.SSP_GROUPER_FACTORY, Partition0SeparateFactory.class.getName()));
    when(this.grouperMetadata.getProcessorLocality()).thenReturn(
        ImmutableMap.of("0", mock(LocationId.class), "1", mock(LocationId.class)));
    Set<SystemStreamPartition> sspsForTask1 = new ImmutableSet.Builder<SystemStreamPartition>().add(
        new SystemStreamPartition(SYSTEM_STREAM0, new Partition(1)))
        .add(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(2)))
        .add(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(3)))
        .add(new SystemStreamPartition(SYSTEM_STREAM1, new Partition(1)))
        .add(new SystemStreamPartition(SYSTEM_STREAM1, new Partition(2)))
        .build();
    Map<String, ContainerModel> containerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0))), "1",
            new ContainerModel("1",
                ImmutableMap.of(taskName(1), new TaskModel(taskName(1), sspsForTask1, new Partition(1)))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testCustomTaskNameGrouper() {
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(4);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1),
        ImmutableMap.of(TaskConfig.GROUPER_FACTORY, Task0SeparateFactory.class.getName()));
    when(this.grouperMetadata.getProcessorLocality()).thenReturn(
        ImmutableMap.of("0", mock(LocationId.class), "1", mock(LocationId.class)));
    Map<String, ContainerModel> containerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0))), "1",
            new ContainerModel("1",
                ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(2), taskModel(2, 2, 2), taskName(3),
                    taskModel(3, 3))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testWithRegexTopicRewriters() {
    // this is the SystemStream that is directly in the config
    SystemStream existingSystemStream = new SystemStream("existingSystem", "existingStream");
    addStreamMetadataCacheMetadata(this.streamMetadataCache,
        ImmutableMap.of(SYSTEM_STREAM0, buildSystemStreamMetadata(4), SYSTEM_STREAM1, buildSystemStreamMetadata(3),
            existingSystemStream, buildSystemStreamMetadata(1)));
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(4);

    PowerMockito.mockStatic(ConfigUtil.class);
    // add SYSTEM_STREAM0 for one rewriter
    PowerMockito.when(ConfigUtil.applyRewriter(any(), eq(REGEX_REWRITER0)))
        .thenAnswer(invocation -> addSystemStreamInput(SYSTEM_STREAM0, invocation.getArgumentAt(0, Config.class)));
    // add SYSTEM_STREAM1 for another rewriter
    PowerMockito.when(ConfigUtil.applyRewriter(any(), eq(REGEX_REWRITER1)))
        .thenAnswer(invocation -> addSystemStreamInput(SYSTEM_STREAM1, invocation.getArgumentAt(0, Config.class)));

    Config config = config(ImmutableList.of(existingSystemStream),
        ImmutableMap.of(JobConfig.CONFIG_REWRITERS, String.format("%s,%s", REGEX_REWRITER0, REGEX_REWRITER1),
            String.format(JobConfig.CONFIG_REWRITER_CLASS, REGEX_REWRITER0), RegExTopicGenerator.class.getName(),
            String.format(JobConfig.CONFIG_REWRITER_CLASS, REGEX_REWRITER1), RegExTopicGenerator.class.getName()));
    Set<SystemStreamPartition> sspsForTask0 =
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(0)),
            new SystemStreamPartition(SYSTEM_STREAM1, new Partition(0)),
            new SystemStreamPartition(existingSystemStream, new Partition(0)));
    TaskModel taskModel0 = new TaskModel(taskName(0), sspsForTask0, new Partition(0));
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel0, taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    Map<String, String> expectedConfigMap = new HashMap<>(config);
    expectedConfigMap.put(TaskConfig.INPUT_STREAMS,
        String.format("%s,%s,%s", taskInputString(existingSystemStream), taskInputString(SYSTEM_STREAM0),
            taskInputString(SYSTEM_STREAM1)));
    JobModel expected = new JobModel(new MapConfig(expectedConfigMap), containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testWithSSPFilter() {
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(4);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1),
        ImmutableMap.of(JobConfig.SSP_MATCHER_CLASS, Partition0Or1Filter.class.getName(),
            JobConfig.SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX, ".*MyJobFactory",
            // this needs to match the regex in the line above
            JobConfig.STREAM_JOB_FACTORY_CLASS, "org.apache.samza.custom.MyJobFactory"));
    Map<String, ContainerModel> containerModels =
        ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0))), "1",
            new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testSSPMatcherConfigJobFactoryRegexNotMatched() {
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(4);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1),
        ImmutableMap.of(JobConfig.SSP_MATCHER_CLASS, Partition0Or1Filter.class.getName(),
            JobConfig.SSP_MATCHER_CONFIG_JOB_FACTORY_REGEX, ".*MyJobFactory",
            // this needs to not match the regex in the line above
            JobConfig.STREAM_JOB_FACTORY_CLASS, "org.apache.samza.custom.OtherJobFactory"));
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testNoPreviousTasksAssignsNewChangelogPartitions() {
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1), ImmutableMap.of());
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual = JobModelCalculator.INSTANCE.calculateJobModel(config, ImmutableMap.of(), this.streamMetadataCache,
        this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testPreviousChangelogPartitionsMaintained() {
    // existing changelog mapping has 2 tasks, but the job model ultimately will need 4 tasks
    // intentionally using an "out-of-order" changelog mapping to make sure it gets maintained
    Map<TaskName, Integer> changelogPartitionMapping = ImmutableMap.of(taskName(0), 1, taskName(1), 0);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1), ImmutableMap.of());
    // these task models have special changelog partitions from the previous mapping
    TaskModel taskModel0 = new TaskModel(taskName(0),
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(0)),
            new SystemStreamPartition(SYSTEM_STREAM1, new Partition(0))), new Partition(1));
    TaskModel taskModel1 = new TaskModel(taskName(1),
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(1)),
            new SystemStreamPartition(SYSTEM_STREAM1, new Partition(1))), new Partition(0));
    // tasks 2 and 3 will get assigned new changelog partitions
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel0, taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel1, taskName(3), taskModel(3, 3))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testSSPGrouperProxyUsed() {
    addStreamMetadataCacheMetadata(this.streamMetadataCache,
        ImmutableMap.of(SYSTEM_STREAM0, buildSystemStreamMetadata(4)));
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(2);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0),
        ImmutableMap.of(JobConfig.SSP_GROUPER_FACTORY, Partition0SeparateFactory.class.getName(),
            // need this to trigger SSPGrouperProxy logic
            String.format(StorageConfig.FACTORY, "myStore"), "MyCustomStore"));
    // custom SSP grouper expects a certain processor locality for another test, so add the locality here too
    when(this.grouperMetadata.getProcessorLocality()).thenReturn(
        ImmutableMap.of("0", mock(LocationId.class), "1", mock(LocationId.class)));
    /*
     * Even though the custom grouper factory would normally send the additional SSPs to task 1, the SSP grouper proxy
     * should give task 0 some of the SSPs.
     */
    when(this.grouperMetadata.getPreviousTaskToSSPAssignment()).thenReturn(
        ImmutableMap.of(taskName(0), ImmutableList.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(0))),
            taskName(1), ImmutableList.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(1)))));
    Set<SystemStreamPartition> sspsForTask0 =
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(0)),
            new SystemStreamPartition(SYSTEM_STREAM0, new Partition(2)));
    Set<SystemStreamPartition> sspsForTask1 =
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(1)),
            new SystemStreamPartition(SYSTEM_STREAM0, new Partition(3)));
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0", new ContainerModel("0",
            ImmutableMap.of(taskName(0), new TaskModel(taskName(0), sspsForTask0, new Partition(0)))), "1",
        new ContainerModel("1",
            ImmutableMap.of(taskName(1), new TaskModel(taskName(1), sspsForTask1, new Partition(1)))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  @Test
  public void testHostAffinityEnabled() {
    Map<TaskName, Integer> changelogPartitionMapping = changelogPartitionMapping(4);
    Config config = config(ImmutableList.of(SYSTEM_STREAM0, SYSTEM_STREAM1),
        ImmutableMap.of(ClusterManagerConfig.HOST_AFFINITY_ENABLED, "true",
            // make sure the group method which accepts GrouperMetadata is used
            TaskConfig.GROUPER_FACTORY, GroupByContainerCountOverrideFactory.class.getName()));
    Map<String, ContainerModel> containerModels = ImmutableMap.of("0",
        new ContainerModel("0", ImmutableMap.of(taskName(0), taskModel(0, 0, 0), taskName(2), taskModel(2, 2, 2))), "1",
        new ContainerModel("1", ImmutableMap.of(taskName(1), taskModel(1, 1, 1), taskName(3), taskModel(3, 3))));
    JobModel expected = new JobModel(config, containerModels);
    JobModel actual =
        JobModelCalculator.INSTANCE.calculateJobModel(config, changelogPartitionMapping, this.streamMetadataCache,
            this.grouperMetadata);
    assertEquals(expected, actual);
  }

  private static SystemStreamMetadata buildSystemStreamMetadata(int numPartitions) {
    SystemStreamMetadata systemStreamMetadata = mock(SystemStreamMetadata.class);
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionToMetadata =
        IntStream.range(0, numPartitions)
            .boxed()
            .collect(
                Collectors.toMap(Partition::new, i -> mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class)));
    when(systemStreamMetadata.getSystemStreamPartitionMetadata()).thenReturn(partitionToMetadata);
    return systemStreamMetadata;
  }

  private static TaskName taskName(int id) {
    return new TaskName("Partition " + id);
  }

  private static TaskModel taskModel(int id, int partitionForSystemStream0) {
    return new TaskModel(new TaskName("Partition " + id),
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(partitionForSystemStream0))),
        new Partition(id));
  }

  private static TaskModel taskModel(int id, int partitionForSystemStream0, int partitionForSystemStream1) {
    return new TaskModel(taskName(id),
        ImmutableSet.of(new SystemStreamPartition(SYSTEM_STREAM0, new Partition(partitionForSystemStream0)),
            new SystemStreamPartition(SYSTEM_STREAM1, new Partition(partitionForSystemStream1))), new Partition(id));
  }

  private static Map<TaskName, Integer> changelogPartitionMapping(int numPartitions) {
    return IntStream.range(0, numPartitions)
        .boxed()
        .collect(Collectors.toMap(TestJobModelCalculator::taskName, Function.identity()));
  }

  private static Config config(List<SystemStream> inputs, Map<String, String> extraConfigs) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_CONTAINER_COUNT, "2");
    configMap.put(TaskConfig.INPUT_STREAMS,
        inputs.stream().map(TestJobModelCalculator::taskInputString).collect(Collectors.joining(",")));
    configMap.putAll(extraConfigs);
    return new MapConfig(configMap);
  }

  private static String taskInputString(SystemStream systemStream) {
    return String.format("%s.%s", systemStream.getSystem(), systemStream.getStream());
  }

  private static void addStreamMetadataCacheMetadata(StreamMetadataCache mockStreamMetadataCache,
      Map<SystemStream, SystemStreamMetadata> systemStreamMetadataMap) {
    scala.collection.immutable.Set<SystemStream> systemStreams =
        JavaConverters.asScalaSetConverter(systemStreamMetadataMap.keySet()).asScala().toSet();
    scala.collection.immutable.Map<SystemStream, SystemStreamMetadata> scalaSystemStreamMetadataMap =
        ScalaJavaUtil.toScalaMap(systemStreamMetadataMap);
    when(mockStreamMetadataCache.getStreamMetadata(systemStreams, true)).thenReturn(scalaSystemStreamMetadataMap);
  }

  public static class Partition0SeparateFactory implements SystemStreamPartitionGrouperFactory {
    @Override
    public SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
      // check that the "processor.list" gets passed through the config
      assertEquals(ImmutableSet.of("0", "1"), ImmutableSet.copyOf(config.get(JobConfig.PROCESSOR_LIST).split(",")));
      return new Partition0Separate();
    }
  }

  /**
   * Groups all SSPs into two tasks. The first task gets all of the SSPs with a partition id of 0. The second task gets
   * all other SSPs. This is used for testing a custom {@link SystemStreamPartitionGrouper}.
   */
  private static class Partition0Separate implements SystemStreamPartitionGrouper {
    @Override
    public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> systemStreamPartitions) {
      // if partition 0, then add to this first set
      Set<SystemStreamPartition> sspsForTask0 = new HashSet<>();
      // all SSPs that are not partition 0 go into this second set
      Set<SystemStreamPartition> sspsForTask1 = new HashSet<>();
      systemStreamPartitions.forEach(ssp -> {
        if (ssp.getPartition().getPartitionId() == 0) {
          sspsForTask0.add(ssp);
        } else {
          sspsForTask1.add(ssp);
        }
      });
      return ImmutableMap.of(taskName(0), sspsForTask0, taskName(1), sspsForTask1);
    }
  }

  public static class Task0SeparateFactory implements TaskNameGrouperFactory {
    @Override
    public TaskNameGrouper build(Config config) {
      return new Task0Separate();
    }
  }

  private static class Task0Separate implements TaskNameGrouper {
    @Override
    public Set<ContainerModel> group(Set<TaskModel> taskModels, List<String> containerIds) {
      // if task 0, then add to this map
      Map<TaskName, TaskModel> tasksForContainer0 = new HashMap<>();
      // all tasks that aren't task 0 go into this map
      Map<TaskName, TaskModel> tasksForContainer1 = new HashMap<>();
      taskModels.forEach(taskModel -> {
        if (taskName(0).equals(taskModel.getTaskName())) {
          tasksForContainer0.put(taskName(0), taskModel);
        } else {
          tasksForContainer1.put(taskModel.getTaskName(), taskModel);
        }
      });
      return ImmutableSet.of(new ContainerModel("0", tasksForContainer0), new ContainerModel("1", tasksForContainer1));
    }
  }

  public static class Partition0Or1Filter implements SystemStreamPartitionMatcher {
    @Override
    public Set<SystemStreamPartition> filter(Set<SystemStreamPartition> systemStreamPartitions, Config config) {
      return systemStreamPartitions.stream()
          .filter(ssp -> ssp.getPartition().getPartitionId() <= 1)
          .collect(Collectors.toSet());
    }
  }

  public static class GroupByContainerCountOverrideFactory implements TaskNameGrouperFactory {
    @Override
    public TaskNameGrouper build(Config config) {
      return new GroupByContainerCountOverride(new JobConfig(config).getContainerCount());
    }
  }

  private static class GroupByContainerCountOverride extends GroupByContainerCount {
    public GroupByContainerCountOverride(int containerCount) {
      super(containerCount);
    }

    @Override
    public Set<ContainerModel> group(Set<TaskModel> taskModels, List<String> containersIds) {
      throw new UnsupportedOperationException("This should not be called");
    }
  }

  private static Config addSystemStreamInput(SystemStream systemStream, Config config) {
    String existingInputs = config.get(TaskConfig.INPUT_STREAMS);
    Preconditions.checkArgument(StringUtils.isNotEmpty(existingInputs));
    Map<String, String> newConfig = new HashMap<>(config);
    newConfig.put(TaskConfig.INPUT_STREAMS, String.format("%s,%s", existingInputs, taskInputString(systemStream)));
    return new MapConfig(newConfig);
  }
}
