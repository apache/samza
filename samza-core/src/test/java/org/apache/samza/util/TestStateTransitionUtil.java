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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test class for {@link StateTransitionUtil}
 */
public class TestStateTransitionUtil {
  private static final List<String> STATES = ImmutableList.of("A", "B", "C", "D", "E");
  private static final Map<String, List<String>> VALID_TRANSITIONS = new ImmutableMap.Builder<String, List<String>>()
      .put("A", ImmutableList.of("C", "D"))
      .put("B", ImmutableList.of("D", "E"))
      .put("C", ImmutableList.of("D", "A", "E"))
      .put("D", ImmutableList.of("A", "B"))
      .put("E", ImmutableList.of("B", "C"))
      .build();

  private static final Map<String, List<String>> INVALID_TRANSITIONS = new ImmutableMap.Builder<String, List<String>>()
      .put("A", ImmutableList.of("A", "B", "E"))
      .put("B", ImmutableList.of("A", "B", "C"))
      .put("C", ImmutableList.of("B", "C"))
      .put("D", ImmutableList.of("C", "D", "E"))
      .put("E", ImmutableList.of("A", "D", "E"))
      .build();

  private static final Random RANDOM = new Random();

  @Test
  public void testCompareAndSet() {
    AtomicReference<String> reference = new AtomicReference<>();

    for (String desiredState : STATES) {
      String currentState = STATES.get(RANDOM.nextInt(STATES.size()));
      reference.set(currentState);

      boolean actual = StateTransitionUtil.compareAndSet(reference, VALID_TRANSITIONS.get(desiredState), desiredState);
      boolean expected = VALID_TRANSITIONS.get(desiredState).contains(currentState);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testCompareNotInAndSet() {
    AtomicReference<String> reference = new AtomicReference<>();

    for (String desiredState : STATES) {
      String currentState = STATES.get(RANDOM.nextInt(STATES.size()));
      reference.set(currentState);

      boolean actual = StateTransitionUtil.compareNotInAndSet(reference, INVALID_TRANSITIONS.get(desiredState), desiredState);
      boolean expected = !INVALID_TRANSITIONS.get(desiredState).contains(currentState);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testCompareAndSetWithBarrierTimeout() {
    String currentState = STATES.get(RANDOM.nextInt(STATES.size()));
    String desiredState = STATES.get(RANDOM.nextInt(STATES.size()));
    AtomicReference<String> reference = new AtomicReference<>(currentState);

    final CountDownLatch barrier = new CountDownLatch(1);
    Duration timeout = Duration.ofMillis(RANDOM.nextInt(1000));
    boolean transitionResult = StateTransitionUtil.compareAndSetWithBarrier(reference, currentState, desiredState, barrier, timeout);

    assertFalse(transitionResult);
  }

  @Test
  public void testCompareAndSetWithBarrierAlreadyCompleted() {
    String currentState = STATES.get(RANDOM.nextInt(STATES.size()));
    String desiredState = STATES.get(RANDOM.nextInt(STATES.size()));
    AtomicReference<String> reference = new AtomicReference<>(currentState);

    final CountDownLatch barrier = new CountDownLatch(0);
    Duration timeout = Duration.ofMillis(RANDOM.nextInt(1000));
    boolean transitionResult = StateTransitionUtil.compareAndSetWithBarrier(reference, currentState, desiredState, barrier, timeout);

    assertTrue(transitionResult);
  }

  @Test
  public void testCompareAndSetWithBarrierCompletionWithStateChange() {
    String currentState = STATES.get(RANDOM.nextInt(STATES.size()));
    String desiredState = STATES.get(RANDOM.nextInt(STATES.size()));
    AtomicReference<String> reference = new AtomicReference<>(currentState);

    final CountDownLatch barrier = new CountDownLatch(1);
    Duration timeout = Duration.ofMillis(RANDOM.nextInt(1000));

    CompletableFuture.runAsync(() -> {
        try {
          Thread.sleep(timeout.toMillis());
        }   catch (InterruptedException e) {

        }

        for (String state : STATES) {
          if (!currentState.equals(state)) {
            reference.set(state);
            break;
          }
        }
        barrier.countDown();
      });

    boolean transitionResult = StateTransitionUtil.compareAndSetWithBarrier(reference, currentState, desiredState, barrier, Duration.ofMillis(-1));
    assertFalse(transitionResult);
  }

  @Test
  public void testCompareAndSetWithBarrierCompletion() {
    String currentState = STATES.get(RANDOM.nextInt(STATES.size()));
    String desiredState = STATES.get(RANDOM.nextInt(STATES.size()));
    AtomicReference<String> reference = new AtomicReference<>(currentState);

    final CountDownLatch barrier = new CountDownLatch(1);
    Duration timeout = Duration.ofMillis(RANDOM.nextInt(1000));

    CompletableFuture.runAsync(() -> {
        try {
          Thread.sleep(timeout.toMillis());
        } catch (InterruptedException e) {

        }

        barrier.countDown();
      });

    boolean transitionResult = StateTransitionUtil.compareAndSetWithBarrier(reference, currentState, desiredState, barrier, Duration.ofMillis(-1));
    assertTrue(transitionResult);
  }
}
