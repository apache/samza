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
public class TestContainerPlacementUtil {
  private static final Config CONFIG =
      new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));
  private CoordinatorStreamStore coordinatorStreamStore;

  private ContainerPlacementUtil containerPlacementUtil;

  @Before
  public void setup() {
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
    containerPlacementUtil = new ContainerPlacementUtil(coordinatorStreamStore);
    containerPlacementUtil.start();
  }

  @After
  public void teardown() {
    containerPlacementUtil.stop();
    coordinatorStreamStore.close();
  }

  @Test
  public void testDefaultMetadataStore() {
    Assert.assertNotNull(containerPlacementUtil);
    Assert.assertEquals(NamespaceAwareCoordinatorStreamStore.class,
        containerPlacementUtil.getContainerPlacementStore().getClass());
  }

  @Test
  public void testReadWriteContainerPlacementRequestMessages() {
    ContainerPlacementRequestMessage messageWrittenToMetastore =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST",
            System.currentTimeMillis());
    containerPlacementUtil.writeContainerPlacementRequestMessage(messageWrittenToMetastore);
    Optional<ContainerPlacementRequestMessage> messageReadFromMetastore =
        containerPlacementUtil.readContainerPlacementRequestMessage(messageWrittenToMetastore.getUuid());
    Assert.assertTrue(messageReadFromMetastore.isPresent());
    Assert.assertEquals(messageWrittenToMetastore, messageReadFromMetastore.get());
    // Check for non existent key
    Optional<ContainerPlacementRequestMessage> readNull =
        containerPlacementUtil.readContainerPlacementRequestMessage(UUID.randomUUID());
    Assert.assertTrue(!readNull.isPresent());
    Assert.assertEquals(1, containerPlacementUtil.readAllContainerPlacementRequestMessages().size());
  }

  @Test
  public void testReadWriteContainerPlacementResponseMessages() {
    ContainerPlacementResponseMessage messageWrittenToMetastore =
        new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001",
            Integer.toString(new Random().nextInt(5)), "ANY_HOST", ContainerPlacementMessage.StatusCode.BAD_REQUEST,
            "Request ignored redundant", System.currentTimeMillis());

    containerPlacementUtil.writeContainerPlacementResponseMessage(messageWrittenToMetastore);
    Optional<ContainerPlacementResponseMessage> messageReadFromMetastore =
        containerPlacementUtil.readContainerPlacementResponseMessage(messageWrittenToMetastore.getUuid());
    Assert.assertTrue(messageReadFromMetastore.isPresent());
    Assert.assertEquals(messageWrittenToMetastore, messageReadFromMetastore.get());

    // Request store must not contain anything
    Optional<ContainerPlacementRequestMessage> readNull =
        containerPlacementUtil.readContainerPlacementRequestMessage(messageWrittenToMetastore.getUuid());
    Assert.assertTrue(!readNull.isPresent());
    Assert.assertTrue(containerPlacementUtil.getContainerPlacementStore().all().size() == 1);
  }

  @Test
  public void testContainerPlacementMessageDeletion() {
    ContainerPlacementRequestMessage requestMessage1 =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST",
            System.currentTimeMillis());
    ContainerPlacementRequestMessage requestMessage2 =
        new ContainerPlacementRequestMessage(UUID.randomUUID(), "app-attempt-001", "1", "host2",
            System.currentTimeMillis());

    containerPlacementUtil.writeContainerPlacementRequestMessage(requestMessage1);
    containerPlacementUtil.writeContainerPlacementRequestMessage(requestMessage2);

    ContainerPlacementResponseMessage responseMessage1 =
        new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001", "4", "ANY_HOST",
            ContainerPlacementMessage.StatusCode.BAD_REQUEST, "Request ignored redundant", System.currentTimeMillis());
    ContainerPlacementResponseMessage responseMessage2 =
        new ContainerPlacementResponseMessage(UUID.randomUUID(), "app-attempt-001", "1", "ANY_HOST",
            ContainerPlacementMessage.StatusCode.IN_PROGRESS, "Requested resources", System.currentTimeMillis());

    containerPlacementUtil.writeContainerPlacementResponseMessage(responseMessage1);
    containerPlacementUtil.writeContainerPlacementResponseMessage(responseMessage2);

    Assert.assertEquals(4, containerPlacementUtil.getContainerPlacementStore().all().size());

    containerPlacementUtil.deleteContainerPlacementResponseMessage(responseMessage1.getUuid());

    Assert.assertEquals(3, containerPlacementUtil.getContainerPlacementStore().all().size());

    containerPlacementUtil.deleteContainerPlacementRequestMessage(requestMessage1.getUuid());

    Assert.assertEquals(2, containerPlacementUtil.getContainerPlacementStore().all().size());

    Assert.assertEquals(containerPlacementUtil.readContainerPlacementRequestMessage(requestMessage2.getUuid()).get(),
        requestMessage2);
    Assert.assertEquals(containerPlacementUtil.readContainerPlacementResponseMessage(responseMessage2.getUuid()).get(),
        responseMessage2);

    containerPlacementUtil.deleteAllContainerPlacementMessages();

    Assert.assertEquals(0, containerPlacementUtil.readAllContainerPlacementRequestMessages().size());
    Assert.assertEquals(0, containerPlacementUtil.getContainerPlacementStore().all().size());
  }
}
