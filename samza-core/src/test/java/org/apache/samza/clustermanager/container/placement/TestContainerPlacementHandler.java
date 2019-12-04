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
package org.apache.samza.clustermanager.container.placement;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.apache.samza.clustermanager.ContainerProcessManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.placement.ContainerPlacementMessage;
import org.apache.samza.container.placement.ContainerPlacementRequestMessage;
import org.apache.samza.container.placement.ContainerPlacementResponseMessage;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class TestContainerPlacementHandler {
  private static final Config CONFIG =
      new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));
  private CoordinatorStreamStore coordinatorStreamStore;

  private ContainerPlacementHandler containerPlacementHandler;

  @Before
  public void setup() {
    ContainerProcessManager manager = mock(ContainerProcessManager.class);
    doNothing().when(manager).registerContainerPlacementAction(any());
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
    containerPlacementHandler = new ContainerPlacementHandler(coordinatorStreamStore, manager);
  }

  @After
  public void teardown() {
    containerPlacementHandler.stop();
    coordinatorStreamStore.close();
  }

  @Test
  public void testDefaultMetadataStore() {
    Assert.assertNotNull(containerPlacementHandler);
    Assert.assertEquals(NamespaceAwareCoordinatorStreamStore.class,
        containerPlacementHandler.getRequestStore().getClass());
    Assert.assertEquals(NamespaceAwareCoordinatorStreamStore.class,
        containerPlacementHandler.getResponseStore().getClass());
  }

  @Test
  public void testReadWriteContainerPlacementRequestMessages() {
    ContainerPlacementRequestMessage messageWrittenToMetastore =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST");
    containerPlacementHandler.writeContainerPlacementRequestMessage(messageWrittenToMetastore);
    Optional<ContainerPlacementRequestMessage> messageReadFromMetastore =
        containerPlacementHandler.readContainerPlacementRequestMessage(messageWrittenToMetastore.getProcessorId());
    Assert.assertTrue(messageReadFromMetastore.isPresent());
    Assert.assertEquals(messageWrittenToMetastore, messageReadFromMetastore.get());
    // Check for non existent key
    Optional<ContainerPlacementRequestMessage> readNull =
        containerPlacementHandler.readContainerPlacementRequestMessage("vague");
    Assert.assertTrue(!readNull.isPresent());
    // No new messages should be added to the Response Store
    Assert.assertTrue(containerPlacementHandler.getResponseStore().all().size() == 0);
    Assert.assertEquals(1, containerPlacementHandler.readAllContainerPlacementRequestMessages().size());
  }

  @Test
  public void testReadWriteContainerPlacementResponseMessages() {
    ContainerPlacementResponseMessage messageWrittenToMetastore =
        new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001",
            Integer.toString(new Random().nextInt(5)), "ANY_HOST", ContainerPlacementMessage.StatusCode.BAD_REQUEST,
            "Request ignored redundant");

    containerPlacementHandler.writeContainerPlacementResponseMessage(messageWrittenToMetastore);
    Optional<ContainerPlacementResponseMessage> messageReadFromMetastore =
        containerPlacementHandler.readContainerPlacementResponseMessage(messageWrittenToMetastore.getProcessorId());
    Assert.assertTrue(messageReadFromMetastore.isPresent());
    Assert.assertEquals(messageWrittenToMetastore, messageReadFromMetastore.get());

    // Request store must not contain anything
    Optional<ContainerPlacementRequestMessage> readNull =
        containerPlacementHandler.readContainerPlacementRequestMessage(messageWrittenToMetastore.getProcessorId());
    Assert.assertTrue(!readNull.isPresent());
    // No new messages should be added to the Response Store
    Assert.assertTrue(containerPlacementHandler.getRequestStore().all().size() == 0);
  }

  @Test
  public void testContainerPlacementMessageDeletion() {
    ContainerPlacementRequestMessage requestMessage1 =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST");
    ContainerPlacementRequestMessage requestMessage2 =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "1", "host2");

    containerPlacementHandler.writeContainerPlacementRequestMessage(requestMessage1);
    containerPlacementHandler.writeContainerPlacementRequestMessage(requestMessage2);

    ContainerPlacementResponseMessage responseMessage1 =
        new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST",
            ContainerPlacementMessage.StatusCode.BAD_REQUEST, "Request ignored redundant");
    ContainerPlacementResponseMessage responseMessage2 =
        new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001", "1", "ANY_HOST",
            ContainerPlacementMessage.StatusCode.IN_PROGRESS, "Requested resources");

    containerPlacementHandler.writeContainerPlacementResponseMessage(responseMessage1);
    containerPlacementHandler.writeContainerPlacementResponseMessage(responseMessage2);

    Assert.assertEquals(2, containerPlacementHandler.getRequestStore().all().size());
    Assert.assertEquals(2, containerPlacementHandler.getResponseStore().all().size());

    containerPlacementHandler.deleteContainerPlacementResponseMessage(responseMessage1.getProcessorId());

    Assert.assertEquals(2, containerPlacementHandler.getRequestStore().all().size());
    Assert.assertEquals(1, containerPlacementHandler.getResponseStore().all().size());

    containerPlacementHandler.deleteContainerPlacementRequestMessage(requestMessage1.getProcessorId());

    Assert.assertEquals(1, containerPlacementHandler.getRequestStore().all().size());
    Assert.assertEquals(1, containerPlacementHandler.getResponseStore().all().size());

    Assert.assertEquals(
        containerPlacementHandler.readContainerPlacementRequestMessage(requestMessage2.getProcessorId()).get(),
        requestMessage2);
    Assert.assertEquals(
        containerPlacementHandler.readContainerPlacementResponseMessage(responseMessage2.getProcessorId()).get(),
        responseMessage2);

    containerPlacementHandler.deleteAllContainerPlacementMessages();

    Assert.assertEquals(0, containerPlacementHandler.readAllContainerPlacementRequestMessages().size());
    Assert.assertEquals(0, containerPlacementHandler.getResponseStore().all().size());
  }
}
