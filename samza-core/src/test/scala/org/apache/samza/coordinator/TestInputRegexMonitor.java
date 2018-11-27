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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import scala.collection.JavaConversions$;


public class TestInputRegexMonitor {

  private InputStreamRegexMonitor inputStreamRegexMonitor;
  private CountDownLatch callbackCount;

  private int inputRegexMs = 10;
  private String systemName = "kafka";
  private int expectedNumberOfCallbacks = 1;
  private Set<SystemStream> inputStreamsDiscovered;
  private final SystemStream sampleStream = new SystemStream(systemName, "test-1");

  @Before
  public void setUp() {

    inputStreamsDiscovered = new HashSet<>();
    Map<String, Pattern> patternMap = new HashMap<>();
    patternMap.put(systemName, Pattern.compile("test-.*"));

    StreamMetadataCache mockStreamMetadataCache = new MockStreamMetadataCache(null, 1, null);

    MetricsRegistry metrics = Mockito.mock(MetricsRegistry.class);
    this.callbackCount = new CountDownLatch(expectedNumberOfCallbacks);

    // Creating an inputStreamRegexMonitor with empty-input set and test-.* regex input
    this.inputStreamRegexMonitor =
        new InputStreamRegexMonitor(new HashSet<>(), patternMap, mockStreamMetadataCache, metrics, inputRegexMs,
            newInputStreams -> {
        this.callbackCount.countDown();
        this.inputStreamsDiscovered.addAll(newInputStreams);

        // Check that the newInputStream discovered is "kafka" "Test-1"
        Assert.assertTrue(inputStreamsDiscovered.size() == 1);
        Assert.assertTrue(inputStreamsDiscovered.contains(sampleStream));
      });
  }

  @Test
  public void testStartStop() throws InterruptedException {
    Assert.assertFalse(inputStreamRegexMonitor.isRunning());

    // Normal start
    inputStreamRegexMonitor.start();
    Assert.assertTrue(inputStreamRegexMonitor.isRunning());

    // Start ought to be idempotent
    inputStreamRegexMonitor.start();
    Assert.assertTrue(inputStreamRegexMonitor.isRunning());

    // Normal stop
    inputStreamRegexMonitor.stop();
    Assert.assertTrue(inputStreamRegexMonitor.awaitTermination(1, TimeUnit.SECONDS));
    Assert.assertFalse(inputStreamRegexMonitor.isRunning());

    try {
      inputStreamRegexMonitor.start();
    } catch (Exception e) {
      Assert.assertTrue(e.getClass().equals(IllegalStateException.class));
    }

    // Stop ought to be idempotent
    Assert.assertFalse(inputStreamRegexMonitor.isRunning());
    inputStreamRegexMonitor.stop();
    Assert.assertFalse(inputStreamRegexMonitor.isRunning());
  }

  @Test
  public void testSchedulingAndInputAddition() throws Exception {
    this.inputStreamRegexMonitor.start();
    try {
      if (!callbackCount.await(1, TimeUnit.SECONDS)) {
        throw new Exception(
            "Did not see " + expectedNumberOfCallbacks + " callbacks after waiting. " + callbackCount.toString());
      }
    } finally {
      System.out.println("CallbackCount is " + callbackCount.getCount());
      this.inputStreamRegexMonitor.stop();
    }
  }

  private class MockStreamMetadataCache extends StreamMetadataCache {

    public MockStreamMetadataCache(SystemAdmins systemAdmins, int cacheTTLms, Clock clock) {
      super(systemAdmins, cacheTTLms, clock);
    }

    @Override
    public scala.collection.mutable.Set getAllSystemStreams(String systemName) {
      Set<SystemStream> s = new HashSet<>();
      return JavaConversions$.MODULE$.asScalaSet(new HashSet<SystemStream>(Arrays.asList(sampleStream)));
    }
  }
}
