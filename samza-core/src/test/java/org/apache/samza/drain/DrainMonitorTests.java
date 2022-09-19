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

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.metadatastore.MetadataStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for {@link DrainMonitor}
 * */
public class DrainMonitorTests {
  private static final String TEST_RUN_ID = "foo";

  private static final Config
      CONFIG = new MapConfig(ImmutableMap.of(
          "job.name", "test-job",
      "job.coordinator.system", "test-kafka",
      ApplicationConfig.APP_RUN_ID, TEST_RUN_ID));

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

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test()
  public void testConstructorFailureWhenDrainManagerIsNull() {
    exceptionRule.expect(NullPointerException.class);
    exceptionRule.expectMessage("MetadataStore parameter cannot be null.");
    DrainMonitor unusedMonitor = new DrainMonitor(null, null, 100L);
  }

  @Test()
  public void testConstructorFailureWhenConfigIsNull() {
    exceptionRule.expect(NullPointerException.class);
    exceptionRule.expectMessage("Config parameter cannot be null.");
    DrainMonitor unusedMonitor = new DrainMonitor(Mockito.mock(MetadataStore.class), null, 100L);
  }

  @Test()
  public void testConstructorFailureWithInvalidPollingInterval() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Polling interval specified is 0 ms. It should be greater than 0.");
    DrainMonitor unusedMonitor = new DrainMonitor(Mockito.mock(MetadataStore.class), Mockito.mock(Config.class), 0);
  }

  @Test()
  public void testDrainMonitorStartFailureWhenCallbackIsNotSet() {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("Drain Callback needs to be set using registerCallback(callback) prior to "
        + "starting the DrainManager.");
    DrainMonitor drainMonitor = new DrainMonitor(coordinatorStreamStore, CONFIG, 100L);
    drainMonitor.start();
  }

  @Test
  public void testSuccessfulCallbackRegistration() {
    DrainMonitor drainMonitor = new DrainMonitor(coordinatorStreamStore, CONFIG, 100L);
    DrainMonitor.DrainCallback emptyCallback = () -> { };
    boolean callbackRegistrationResult1 = drainMonitor.registerDrainCallback(emptyCallback);
    // first registration of callback should succeed
    Assert.assertTrue(callbackRegistrationResult1);
    boolean callbackRegistrationResult2 = drainMonitor.registerDrainCallback(emptyCallback);
    // repeat registration of callback should fail
    Assert.assertFalse(callbackRegistrationResult2);
  }

  @Test
  public void testCallbackCalledIfMonitorEncountersDrainOnStart() throws InterruptedException {
    final AtomicInteger numCallbacks = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);
    // write drain before monitor start
    DrainUtils.writeDrainNotification(coordinatorStreamStore);
    DrainMonitor drainMonitor = new DrainMonitor(coordinatorStreamStore, CONFIG);
    drainMonitor.registerDrainCallback(() -> {
      numCallbacks.incrementAndGet();
      latch.countDown();
    });
    // monitor shouldn't go into RUNNING state as DrainNotification was already present and it shouldn't start poll
    drainMonitor.start();
    if (!latch.await(2, TimeUnit.SECONDS)) {
      Assert.fail("Timed out waiting for drain callback to complete");
    }
    Assert.assertEquals(1, numCallbacks.get());
    Assert.assertEquals(DrainMonitor.State.INIT, drainMonitor.getState());
  }

  @Test
  public void testCallbackCalledOnDrain() throws InterruptedException {
    final AtomicInteger numCallbacks = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);

    DrainMonitor drainMonitor = new DrainMonitor(coordinatorStreamStore, CONFIG, 100L);

    drainMonitor.registerDrainCallback(() -> {
      numCallbacks.incrementAndGet();
      latch.countDown();
    });
    drainMonitor.start();
    DrainUtils.writeDrainNotification(coordinatorStreamStore);
    if (!latch.await(2, TimeUnit.SECONDS)) {
      Assert.fail("Timed out waiting for drain callback to complete");
    }
    Assert.assertEquals(DrainMonitor.State.STOPPED, drainMonitor.getState());
    Assert.assertEquals(1, numCallbacks.get());
  }

  @Test
  public void testDrainMonitorStop() {
    DrainMonitor drainMonitor = new DrainMonitor(coordinatorStreamStore, CONFIG, 100L);
    drainMonitor.registerDrainCallback(() -> { });
    drainMonitor.start();
    drainMonitor.stop();
    Assert.assertEquals(drainMonitor.getState(), DrainMonitor.State.STOPPED);
  }

  @Test
  public void testShouldDrain() {
    DrainUtils.writeDrainNotification(coordinatorStreamStore);
    NamespaceAwareCoordinatorStreamStore drainStore =
        new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, DrainUtils.DRAIN_METADATA_STORE_NAMESPACE);
    Assert.assertTrue(DrainMonitor.shouldDrain(drainStore, TEST_RUN_ID));
  }
}
