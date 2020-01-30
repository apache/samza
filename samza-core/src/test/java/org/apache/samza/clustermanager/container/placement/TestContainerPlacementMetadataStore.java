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
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
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


@RunWith(MockitoJUnitRunner.class)
public class TestContainerPlacementMetadataStore {
  private static final Config CONFIG =
      new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));
  private CoordinatorStreamStore coordinatorStreamStore;

  private ContainerPlacementMetadataStore containerPlacementMetadataStore;

  @Before
  public void setup() {
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
    containerPlacementMetadataStore = new ContainerPlacementMetadataStore(coordinatorStreamStore);
    containerPlacementMetadataStore.start();
  }

  @After
  public void teardown() {
    containerPlacementMetadataStore.stop();
    coordinatorStreamStore.close();
  }

  @Test
  public void testDefaultMetadataStore() {
    Assert.assertNotNull(containerPlacementMetadataStore);
    Assert.assertEquals(NamespaceAwareCoordinatorStreamStore.class,
        containerPlacementMetadataStore.getContainerPlacementStore().getClass());
  }

  @Test
  public void testReadWriteContainerPlacementRequestMessages() {
    Long timestamp = System.currentTimeMillis();
    UUID uuid = containerPlacementMetadataStore.writeContainerPlacementRequestMessage("app-attempt-001", "4", "ANY_HOST", null,
        timestamp);
    Optional<ContainerPlacementRequestMessage> messageReadFromMetastore =
        containerPlacementMetadataStore.readContainerPlacementRequestMessage(uuid);
    Assert.assertTrue(messageReadFromMetastore.isPresent());
    assertContainerPlacementRequestMessage(uuid, "app-attempt-001", "4", "ANY_HOST", null, timestamp,
        messageReadFromMetastore.get());
    // Check for non existent key
    Optional<ContainerPlacementRequestMessage> readNull =
        containerPlacementMetadataStore.readContainerPlacementRequestMessage(UUID.randomUUID());
    Assert.assertTrue(!readNull.isPresent());
    // No response messages should exist
    Assert.assertTrue(!containerPlacementMetadataStore.readContainerPlacementResponseMessage(uuid).isPresent());
    Assert.assertEquals(1, containerPlacementMetadataStore.readAllContainerPlacementRequestMessages().size());
  }

  @Test
  public void testReadWriteContainerPlacementResponseMessages() {
    ContainerPlacementResponseMessage messageWrittenToMetastore =
        new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001",
            Integer.toString(new Random().nextInt(5)), "ANY_HOST", ContainerPlacementMessage.StatusCode.BAD_REQUEST,
            "Request ignored redundant", System.currentTimeMillis());

    containerPlacementMetadataStore.writeContainerPlacementResponseMessage(messageWrittenToMetastore);
    Optional<ContainerPlacementResponseMessage> messageReadFromMetastore =
        containerPlacementMetadataStore.readContainerPlacementResponseMessage(messageWrittenToMetastore.getUuid());
    Assert.assertTrue(messageReadFromMetastore.isPresent());
    Assert.assertEquals(messageWrittenToMetastore, messageReadFromMetastore.get());

    // Request store must not contain anything
    Optional<ContainerPlacementRequestMessage> readNull =
        containerPlacementMetadataStore.readContainerPlacementRequestMessage(messageWrittenToMetastore.getUuid());
    Assert.assertTrue(!readNull.isPresent());
    // No request messages should exist
    Assert.assertTrue(
        !containerPlacementMetadataStore.readContainerPlacementRequestMessage(messageWrittenToMetastore.getUuid()).isPresent());
    Assert.assertTrue(containerPlacementMetadataStore.getContainerPlacementStore().all().size() == 1);
  }

  @Test
  public void testContainerPlacementMessageDeletion() {
    Long timestamp = System.currentTimeMillis();
    UUID requestMessage1UUID =
        containerPlacementMetadataStore.writeContainerPlacementRequestMessage("app-attempt-001", "4", "ANY_HOST", null,
            timestamp);
    UUID requestMessage2UUID =
        containerPlacementMetadataStore.writeContainerPlacementRequestMessage("app-attempt-001", "1", "host2",
            Duration.ofMillis(100), timestamp);

    ContainerPlacementResponseMessage responseMessage1 =
        new ContainerPlacementResponseMessage(requestMessage1UUID, "app-attempt-001", "4", "ANY_HOST",
            ContainerPlacementMessage.StatusCode.BAD_REQUEST, "Request ignored redundant", System.currentTimeMillis());
    ContainerPlacementResponseMessage responseMessage2 =
        new ContainerPlacementResponseMessage(requestMessage2UUID, "app-attempt-001", "1", "ANY_HOST",
            ContainerPlacementMessage.StatusCode.IN_PROGRESS, "Requested resources", System.currentTimeMillis());

    containerPlacementMetadataStore.writeContainerPlacementResponseMessage(responseMessage1);
    containerPlacementMetadataStore.writeContainerPlacementResponseMessage(responseMessage2);

    Assert.assertTrue(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage1UUID).isPresent());
    Assert.assertTrue(containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage2UUID).isPresent());
    Assert.assertEquals(4, containerPlacementMetadataStore.getContainerPlacementStore().all().size());

    containerPlacementMetadataStore.deleteContainerPlacementResponseMessage(requestMessage1UUID);

    Assert.assertEquals(3, containerPlacementMetadataStore.getContainerPlacementStore().all().size());
    Assert.assertTrue(containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage1UUID).isPresent());

    containerPlacementMetadataStore.deleteContainerPlacementRequestMessage(requestMessage1UUID);

    Assert.assertEquals(2, containerPlacementMetadataStore.getContainerPlacementStore().all().size());
    assertContainerPlacementRequestMessage(requestMessage2UUID, "app-attempt-001", "1", "host2", Duration.ofMillis(100),
        timestamp, containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage2UUID).get());

    Assert.assertEquals(containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage2UUID).get(),
        responseMessage2);
    // requestMessage1 & associated responseMessage1 should not be present
    Assert.assertTrue(!containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage1UUID).isPresent());
    Assert.assertTrue(!containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage1UUID).isPresent());

    requestMessage1UUID =
        containerPlacementMetadataStore.writeContainerPlacementRequestMessage("app-attempt-001", "4", "ANY_HOST", null,
            System.currentTimeMillis());
    containerPlacementMetadataStore.writeContainerPlacementResponseMessage(responseMessage1);
    containerPlacementMetadataStore.deleteAllContainerPlacementMessages(requestMessage1UUID);
    // requestMessage1 & associated responseMessage1 should not be present
    Assert.assertTrue(!containerPlacementMetadataStore.readContainerPlacementRequestMessage(requestMessage1UUID).isPresent());
    Assert.assertTrue(!containerPlacementMetadataStore.readContainerPlacementResponseMessage(requestMessage1UUID).isPresent());

    containerPlacementMetadataStore.deleteAllContainerPlacementMessages();
    Assert.assertEquals(0, containerPlacementMetadataStore.readAllContainerPlacementRequestMessages().size());
  }

  public void assertContainerPlacementRequestMessage(UUID uuid, String deploymentId, String processorId,
      String destinationHost, Duration requestExpiry, long timestamp, ContainerPlacementRequestMessage other) {
    Assert.assertEquals(other.getUuid(), uuid);
    Assert.assertEquals(other.getDeploymentId(), deploymentId);
    Assert.assertEquals(other.getProcessorId(), processorId);
    Assert.assertEquals(other.getDestinationHost(), destinationHost);
    Assert.assertEquals(other.getRequestExpiry(), requestExpiry);
    Assert.assertEquals(other.getTimestamp(), timestamp);
  }
}
