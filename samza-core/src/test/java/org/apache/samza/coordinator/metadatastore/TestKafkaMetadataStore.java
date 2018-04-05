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
package org.apache.samza.coordinator.metadatastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.runtime.LocationId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.Map;

public class TestKafkaMetadataStore {

  private static final String TEST_PROCESSOR_ID1 = "processor-1";
  private static final String TEST_PROCESSOR_ID2 = "processor-2";
  private static final String TEST_PROCESSOR_HOST1 = "TEST_PROCESSOR_HOST1";
  private static final String TEST_PROCESSOR_HOST2 = "processorHost2";

  private KafkaMetadataStore kafkaMetadataStore;
  private LocalityManager localityManagerMock;
  private CoordinatorStreamManager streamManagerMock;
  private TaskAssignmentManager taskAssignmentManagerMock;

  @Before
  public void setUp() throws Exception {
    streamManagerMock = Mockito.mock(CoordinatorStreamManager.class);
    localityManagerMock = Mockito.mock(LocalityManager.class);
    taskAssignmentManagerMock = Mockito.mock(TaskAssignmentManager.class);
    Mockito.when(localityManagerMock.getTaskAssignmentManager())
           .thenReturn(taskAssignmentManagerMock);

    kafkaMetadataStore = new KafkaMetadataStore(new MapConfig(), streamManagerMock, localityManagerMock);
  }

  @Test
  public void testReadProcessorLocality() {
    Map<String, Map<String, String>> mockContainerLocality = new HashMap<>();
    mockContainerLocality.put(TEST_PROCESSOR_ID1, ImmutableMap.of(SetContainerHostMapping.HOST_KEY, TEST_PROCESSOR_HOST1));
    mockContainerLocality.put(TEST_PROCESSOR_ID2, ImmutableMap.of(SetContainerHostMapping.HOST_KEY, TEST_PROCESSOR_HOST2));

    Mockito.when(localityManagerMock.readContainerLocality()).thenReturn(mockContainerLocality);

    Map<String, LocationId> expectedProcessorLocality = ImmutableMap.of(TEST_PROCESSOR_ID1, new LocationId(TEST_PROCESSOR_HOST1),
        TEST_PROCESSOR_ID2, new LocationId(TEST_PROCESSOR_HOST2));

    Assert.assertEquals(expectedProcessorLocality, kafkaMetadataStore.readProcessorLocality());
  }

  @Test
  public void testReadTaskLocality() {
    String taskName = "task-1";

    Mockito.when(taskAssignmentManagerMock.readTaskAssignment()).thenReturn(ImmutableMap.of(taskName, TEST_PROCESSOR_ID1));

    Map<String, Map<String, String>> mockContainerLocality = new HashMap<>();
    mockContainerLocality.put(TEST_PROCESSOR_ID1, ImmutableMap.of(SetContainerHostMapping.HOST_KEY, TEST_PROCESSOR_HOST1));
    mockContainerLocality.put(TEST_PROCESSOR_ID2, ImmutableMap.of(SetContainerHostMapping.HOST_KEY, TEST_PROCESSOR_HOST2));

    Mockito.when(localityManagerMock.readContainerLocality()).thenReturn(mockContainerLocality);
    Map<TaskName, LocationId> expectedTaskNameLocality = ImmutableMap.of(new TaskName(taskName), new LocationId(TEST_PROCESSOR_HOST1));

    Assert.assertEquals(expectedTaskNameLocality, kafkaMetadataStore.readTaskLocality());
  }

  @Test
  public void testDeleteTaskLocality() {
    Mockito.doNothing()
           .when(taskAssignmentManagerMock)
           .deleteTaskContainerMappings(Mockito.anyList());

    kafkaMetadataStore.deleteTaskLocality(ImmutableList.of(new TaskName("task-1"), new TaskName("task-2")));

    Mockito.verify(taskAssignmentManagerMock, Mockito.atLeastOnce()).deleteTaskContainerMappings(Mockito.anyList());
  }

  @Test
  public void testWriteTaskLocality() {
    String testJmxUrl = "testJmxUrl";
    String testJmxTunnelingUrl = "testJmxTunnelingUrl";

    Mockito.doNothing()
           .when(localityManagerMock)
           .writeContainerToHostMapping(TEST_PROCESSOR_ID1, TEST_PROCESSOR_HOST1, testJmxUrl, testJmxTunnelingUrl);

    kafkaMetadataStore.writeTaskLocality(TEST_PROCESSOR_ID1, ImmutableMap.of(new TaskName("task-1"), new LocationId(TEST_PROCESSOR_HOST1)),
                                         testJmxUrl, testJmxTunnelingUrl);

    Mockito.verify(localityManagerMock, Mockito.atLeastOnce())
           .writeContainerToHostMapping(TEST_PROCESSOR_ID1, TEST_PROCESSOR_HOST1, testJmxUrl, testJmxTunnelingUrl);
  }
}
