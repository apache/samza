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

package org.apache.samza.clustermanager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class MockContainerListener {
  private final CountDownLatch conditionLatch;


  private final AsyncCountableCondition containersAdded;
  private final AsyncCountableCondition containersReleased;
  private final AsyncCountableCondition containersAssigned;
  private final AsyncCountableCondition containersRunning;

  private final AsyncCountableCondition[] allConditions;

  public MockContainerListener(int numExpectedContainersAdded,
                               int numExpectedContainersReleased,
                               int numExpectedContainersAssigned,
                               int numExpectedContainersRunning,
                               Runnable addContainerAssertions,
                               Runnable releaseContainerAssertions,
                               Runnable assignContainerAssertions,
                               Runnable runContainerAssertions) {
    containersAdded = new AsyncCountableCondition("containers added", numExpectedContainersAdded, addContainerAssertions);
    containersReleased = new AsyncCountableCondition("containers released", numExpectedContainersReleased, releaseContainerAssertions);
    containersAssigned = new AsyncCountableCondition("containers assigned", numExpectedContainersAssigned, assignContainerAssertions);
    containersRunning = new AsyncCountableCondition("containers running", numExpectedContainersRunning, runContainerAssertions);

    allConditions = new AsyncCountableCondition[] {containersAdded, containersReleased, containersAssigned, containersRunning};

    int unsatisfiedConditions = 0;
    for (AsyncCountableCondition condition : allConditions) {
      if (!condition.isSatisfied()) {
        unsatisfiedConditions++;
      }
    }

    conditionLatch = new CountDownLatch(unsatisfiedConditions);
  }

  public void postAddContainer(int totalAddedContainers) {
    System.out.println("post add contaienr " + totalAddedContainers);
    if (containersAdded.update(totalAddedContainers)) {
      System.out.println("counting ");
      conditionLatch.countDown();
    }
  }

  public void postReleaseContainers(int totalReleasedContainers) {
    if (containersReleased.update(totalReleasedContainers)) {
      conditionLatch.countDown();
    }
  }

  public void postUpdateRequestStateAfterAssignment(int totalAssignedContainers) {
    if (containersAssigned.update(totalAssignedContainers)) {
      conditionLatch.countDown();
    }
  }

  public void postRunContainer(int totalRunningContainers) {
    if (containersRunning.update(totalRunningContainers)) {
      conditionLatch.countDown();
    }
  }

  /**
   * This method should be called in the main thread. It waits for all the conditions to occur in the other
   * threads and then verifies that they were in fact satisfied.
   */
  public void verify() throws InterruptedException {
    conditionLatch.await(5, TimeUnit.SECONDS);

    for (AsyncCountableCondition condition : allConditions) {
      condition.verify();
    }
  }

  private static class AsyncCountableCondition {
    private boolean satisfied = false;
    private final int expectedCount;
    private final Runnable postConditionAssertions;
    private final String name;
    private AssertionError assertionError = null;

    private AsyncCountableCondition(String name, int expectedCount, Runnable postConditionAssertions) {
      this.name = name;
      this.expectedCount = expectedCount;
      if (expectedCount == 0) satisfied = true;
      this.postConditionAssertions = postConditionAssertions;
    }

    public boolean update(int latestCount) {
      if (!satisfied && latestCount == expectedCount) {
        if (postConditionAssertions != null) {
          try {
            postConditionAssertions.run();
          } catch (Throwable t) {
            assertionError = new AssertionError(String.format("Assertion for '%s' failed", name), t);
          }
        }

        satisfied = true;
        return true;
      }
      return false;
    }

    public boolean isSatisfied() {
      return satisfied;
    }

    public void verify() {
      assertTrue(String.format("Condition '%s' was not satisfied", name), isSatisfied());

      if (assertionError != null) {
        throw assertionError;
      }
    }

    @Override
    public String toString() {
      return name;
    }
  }
}