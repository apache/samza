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

package org.apache.samza.job.yarn.util;

import org.apache.hadoop.yarn.api.records.Container;

import static org.junit.Assert.assertTrue;

public class MockContainerListener {
  private static final int NUM_CONDITIONS = 2;
  private boolean allContainersAdded = false;
  private boolean allContainersReleased = false;
  private final int numExpectedContainersAdded;
  private final int numExpectedContainersReleased;
  private final Runnable addContainerAssertions;
  private final Runnable releaseContainerAssertions;

  public MockContainerListener(int numExpectedContainersAdded,
      int numExpectedContainersReleased,
      Runnable addContainerAssertions,
      Runnable releaseContainerAssertions) {
    this.numExpectedContainersAdded = numExpectedContainersAdded;
    this.numExpectedContainersReleased = numExpectedContainersReleased;
    this.addContainerAssertions = addContainerAssertions;
    this.releaseContainerAssertions = releaseContainerAssertions;
  }

  public synchronized void postAddContainer(Container container, int totalAddedContainers) {
    if (totalAddedContainers == numExpectedContainersAdded) {
      if (addContainerAssertions != null) {
        addContainerAssertions.run();
      }

      allContainersAdded = true;
      this.notifyAll();
    }
  }

  public synchronized void postReleaseContainers(int totalReleasedContainers) {
    if (totalReleasedContainers == numExpectedContainersReleased) {
      if (releaseContainerAssertions != null) {
        releaseContainerAssertions.run();
      }

      allContainersReleased = true;
      this.notifyAll();
    }
  }

  public synchronized void verify() {
    // There could be 1 notifyAll() for each condition, so we must wait up to that many times
    for (int i = 0; i < NUM_CONDITIONS && !(allContainersAdded && allContainersReleased); i++) {
      try {
        this.wait(5000);
      } catch (InterruptedException e) {
        // Do nothing
      }
    }

    assertTrue("Not all containers were added.", allContainersAdded);
    assertTrue("Not all containers were released.", allContainersReleased);
  }
}
