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
package org.apache.samza.drain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests for {@link DrainUtils}
 * */
public class DrainUtilsTests {
  private static final String TEST_DEPLOYMENT_ID = "foo";
  private static final Config CONFIG = new MapConfig(ImmutableMap.of(
      "job.name", "test-job",
      "job.coordinator.system", "test-kafka",
      ApplicationConfig.APP_RUN_ID, TEST_DEPLOYMENT_ID));

  private CoordinatorStreamStore coordinatorStreamStore;

  @Before
  public void setup() {
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    coordinatorStreamStore.init();
  }

  @After
  public void teardown() {
    DrainUtils.cleanupAll(coordinatorStreamStore);
    coordinatorStreamStore.close();
  }

  @Test
  public void testWrites() {
    String deploymentId1 = "foo1";
    String deploymentId2 = "foo2";
    String deploymentId3 = "foo3";

    UUID uuid1 = DrainUtils.writeDrainNotification(coordinatorStreamStore, deploymentId1);
    UUID uuid2 = DrainUtils.writeDrainNotification(coordinatorStreamStore, deploymentId2);
    UUID uuid3 = DrainUtils.writeDrainNotification(coordinatorStreamStore, deploymentId3);

    DrainNotification expectedDrainNotification1 = new DrainNotification(uuid1, deploymentId1);
    DrainNotification expectedDrainNotification2 = new DrainNotification(uuid2, deploymentId2);
    DrainNotification expectedDrainNotification3 = new DrainNotification(uuid3, deploymentId3);
    Set<DrainNotification> expectedDrainNotifications = new HashSet<>(Arrays.asList(expectedDrainNotification1,
        expectedDrainNotification2, expectedDrainNotification3));

    Optional<List<DrainNotification>> drainNotifications = readDrainNotificationMessages(coordinatorStreamStore);
    Assert.assertTrue(drainNotifications.isPresent());
    Assert.assertEquals(3, drainNotifications.get().size());
    Assert.assertEquals(expectedDrainNotifications, new HashSet<>(drainNotifications.get()));
  }

  @Test
  public void testCleanup() {
    DrainUtils.writeDrainNotification(coordinatorStreamStore, TEST_DEPLOYMENT_ID);
    DrainUtils.cleanup(coordinatorStreamStore, CONFIG);
    final Optional<List<DrainNotification>> drainNotifications1 = readDrainNotificationMessages(coordinatorStreamStore);
    Assert.assertFalse(drainNotifications1.isPresent());

    final String deploymentId = "bar";
    DrainUtils.writeDrainNotification(coordinatorStreamStore, deploymentId);
    DrainUtils.cleanup(coordinatorStreamStore, CONFIG);
    final Optional<List<DrainNotification>> drainNotifications2 = readDrainNotificationMessages(coordinatorStreamStore);
    Assert.assertTrue(drainNotifications2.isPresent());
    Assert.assertEquals(deploymentId, drainNotifications2.get().get(0).getDeploymentId());
  }

  @Test
  public void testCleanupAll() {
    DrainUtils.writeDrainNotification(coordinatorStreamStore, TEST_DEPLOYMENT_ID);
    DrainUtils.writeDrainNotification(coordinatorStreamStore, TEST_DEPLOYMENT_ID);
    DrainUtils.writeDrainNotification(coordinatorStreamStore, "bar");
    DrainUtils.cleanupAll(coordinatorStreamStore);
    final Optional<List<DrainNotification>> drainNotifications = readDrainNotificationMessages(coordinatorStreamStore);
    Assert.assertFalse(drainNotifications.isPresent());
  }

  private static Optional<List<DrainNotification>> readDrainNotificationMessages(CoordinatorStreamStore metadataStore) {
    final NamespaceAwareCoordinatorStreamStore drainMetadataStore =
        new NamespaceAwareCoordinatorStreamStore(metadataStore, DrainUtils.DRAIN_METADATA_STORE_NAMESPACE);
    final ObjectMapper objectMapper = DrainNotificationObjectMapper.getObjectMapper();
    final ImmutableList<DrainNotification> drainNotifications = drainMetadataStore.all()
        .values()
        .stream()
        .map(bytes -> {
          try {
            return objectMapper.readValue(bytes, DrainNotification.class);
          } catch (IOException e) {
            throw new SamzaException(e);
          }
        })
        .collect(ImmutableList.toImmutableList());
    return drainNotifications.size() > 0
        ? Optional.of(drainNotifications)
        : Optional.empty();
  }
}
