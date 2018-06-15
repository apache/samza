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

package org.apache.samza.util;

import java.time.Duration;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;


public class TestShutdownUtil {
  @Test
  public void testBoundedShutdown() throws Exception {
    long longTimeout = Duration.ofSeconds(60).toMillis();
    long shortTimeout = Duration.ofMillis(100).toMillis();

    Runnable shortRunnable = () -> {
      try {
        Thread.sleep(shortTimeout);
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    };
    long start = System.currentTimeMillis();
    Assert.assertTrue("expect the shutdown task to terminate",
        ShutdownUtil.boundedShutdown(Collections.singletonList(shortRunnable), "testLongTimeout", longTimeout));
    long end = System.currentTimeMillis();
    Assert.assertTrue("boundedShutdown should complete if the shutdown function completes earlier",
        (end - start) < longTimeout / 2);

    Runnable longRunnable = () -> {
      try {
        Thread.sleep(longTimeout);
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    };
    start = System.currentTimeMillis();
    Assert.assertFalse("expect the shutdown task to be unfinished",
        ShutdownUtil.boundedShutdown(Collections.singletonList(longRunnable), "testShortTimeout", shortTimeout));
    end = System.currentTimeMillis();
    Assert.assertTrue("boundedShutdown should complete even if the shutdown function takes long time",
        (end - start) < longTimeout / 2);
  }
}
