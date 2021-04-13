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
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import org.apache.samza.SamzaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFutureUtil {

  /**
   * Test all futures in all collections complete before allOf completes.
   * Test completes exceptionally if any complete exceptionally.
   * Test works with heterogeneous value types.
   * Test works with heterogeneous collection types.
   * Test works with completion stages as well as completable futures.
   */
  @Test
  public void testAllOf() {
    // verify that there is no short circuiting
    CompletableFuture<String> future1 = new CompletableFuture<>();
    CompletableFuture<String> future2 = new CompletableFuture<>();
    CompletableFuture<String> future3 = new CompletableFuture<>();
    CompletableFuture<Integer> future4 = new CompletableFuture<>();
    ImmutableList<CompletableFuture<?>> collection1 =
        ImmutableList.of(future1, future2);
    ImmutableSet<CompletionStage<?>> collection2 =
        ImmutableSet.of(future3, future4);

    CompletableFuture<Void> allFuture = FutureUtil.allOf(collection1, collection2);
    future1.complete("1");
    assertFalse(allFuture.isDone());
    RuntimeException ex2 = new RuntimeException("2");
    future2.completeExceptionally(ex2);
    assertFalse(allFuture.isDone());
    assertFalse(allFuture.isCompletedExceptionally());
    future3.complete("3");
    assertFalse(allFuture.isDone());
    assertFalse(allFuture.isCompletedExceptionally());
    future4.complete(4);
    assertTrue(allFuture.isDone());
    assertTrue(allFuture.isCompletedExceptionally());

    try {
      allFuture.join();
    } catch (Exception e) {
      assertEquals(ex2, FutureUtil.unwrapExceptions(CompletionException.class, e));
    }
  }

  @Test
  public void testAllOfIgnoringErrorsCompletesSuccessfullyIfNoErrors() {
    CompletableFuture<String> future1 = new CompletableFuture<>();
    CompletableFuture<String> future2 = new CompletableFuture<>();

    CompletableFuture<Void> allFuture = FutureUtil.allOf(t -> false, future1, future2);
    future1.complete("1");
    assertFalse(allFuture.isDone());
    future2.complete("2");
    assertTrue(allFuture.isDone());
    assertFalse(allFuture.isCompletedExceptionally());
  }

  @Test
  public void testAllOfIgnoringErrorsCompletesSuccessfullyIfOnlyIgnoredErrors() {
    CompletableFuture<String> future1 = new CompletableFuture<>();
    CompletableFuture<String> future2 = new CompletableFuture<>();

    CompletableFuture<Void> allFuture = FutureUtil.allOf(t -> true, future1, future2);
    future1.complete("1");
    assertFalse(allFuture.isDone());
    RuntimeException ex2 = new RuntimeException("2");
    future2.completeExceptionally(ex2);
    assertTrue(allFuture.isDone());
    assertFalse(allFuture.isCompletedExceptionally());
  }

  @Test
  public void testAllOfIgnoringErrorsCompletesExceptionallyIfNonIgnoredErrors() {
    // also test that each future is checked individually
    CompletableFuture<String> future1 = new CompletableFuture<>();
    CompletableFuture<String> future2 = new CompletableFuture<>();

    Predicate<Throwable> mockPredicate = mock(Predicate.class);
    when(mockPredicate.test(any()))
        .thenReturn(true)
        .thenReturn(false);
    CompletableFuture<Void> allFuture = FutureUtil.allOf(mockPredicate, future1, future2);
    future1.completeExceptionally(new SamzaException());
    assertFalse(allFuture.isDone());
    RuntimeException ex2 = new RuntimeException("2");
    future2.completeExceptionally(ex2);
    assertTrue(allFuture.isDone());
    assertTrue(allFuture.isCompletedExceptionally());
    verify(mockPredicate, times(2)).test(any());
  }

  @Test
  public void testFutureOfMapCompletesExceptionallyIfAValueFutureCompletesExceptionally() {
    Map<String, CompletableFuture<String>> map = new HashMap<>();
    map.put("1", CompletableFuture.completedFuture("1"));
    map.put("2", FutureUtil.failedFuture(new SamzaException()));

    assertTrue(FutureUtil.toFutureOfMap(map).isCompletedExceptionally());
  }

  @Test
  public void testFutureOfMapCompletesSuccessfullyIfNoErrors() {
    Map<String, CompletableFuture<String>> map = new HashMap<>();
    map.put("1", CompletableFuture.completedFuture("1"));
    map.put("2", CompletableFuture.completedFuture("2"));

    CompletableFuture<Map<String, String>> result = FutureUtil.toFutureOfMap(t -> true, map);
    assertTrue(result.isDone());
    assertFalse(result.isCompletedExceptionally());
  }

  @Test
  public void testFutureOfMapCompletesSuccessfullyIfOnlyIgnoredErrors() {
    Map<String, CompletableFuture<String>> map = new HashMap<>();
    map.put("1", CompletableFuture.completedFuture("1"));
    map.put("2", FutureUtil.failedFuture(new SamzaException()));

    CompletableFuture<Map<String, String>> result = FutureUtil
        .toFutureOfMap(t -> FutureUtil.unwrapExceptions(CompletionException.class, t) instanceof SamzaException, map);
    assertTrue(result.isDone());
    result.join();
    assertFalse(result.isCompletedExceptionally());
    assertEquals("1", result.join().get("1"));
    assertFalse(result.join().containsKey("2"));
  }

  @Test
  public void testFutureOfMapCompletesExceptionallyIfAnyNonIgnoredErrors() {
    Map<String, CompletableFuture<String>> map = new HashMap<>();
    map.put("1", FutureUtil.failedFuture(new RuntimeException()));
    SamzaException samzaException = new SamzaException();
    map.put("2", FutureUtil.failedFuture(samzaException));

    Predicate<Throwable> mockPredicate = mock(Predicate.class);
    when(mockPredicate.test(any()))
        .thenReturn(true)
        .thenReturn(false);

    CompletableFuture<Map<String, String>> result = FutureUtil.toFutureOfMap(mockPredicate, map);
    assertTrue(result.isDone());
    assertTrue(result.isCompletedExceptionally());
    verify(mockPredicate, times(2)).test(any()); // verify that each failed value future is tested

    try {
      result.join();
      fail("Should have thrown an exception.");
    } catch (Exception e) {
      assertEquals(samzaException, FutureUtil.unwrapExceptions(CompletionException.class, e));
    }
  }

  @Test
  public void testUnwrapExceptionUnwrapsMultipleExceptions() {
    IllegalArgumentException cause = new IllegalArgumentException();
    Throwable t = new SamzaException(new SamzaException(cause));
    Throwable unwrappedThrowable = FutureUtil.unwrapExceptions(SamzaException.class, t);
    assertEquals(cause, unwrappedThrowable);
  }

  @Test
  public void testUnwrapExceptionReturnsOriginalExceptionIfNoWrapper() {
    IllegalArgumentException cause = new IllegalArgumentException();
    Throwable unwrappedThrowable = FutureUtil.unwrapExceptions(SamzaException.class, cause);
    assertEquals(cause, unwrappedThrowable);
  }

  @Test
  public void testUnwrapExceptionReturnsNullIfNoNonWrapperCause() {
    Throwable t = new SamzaException(new SamzaException());
    Throwable unwrappedThrowable = FutureUtil.unwrapExceptions(SamzaException.class, t);
    assertNull(unwrappedThrowable);
  }

  @Test
  public void testUnwrapExceptionReturnsNullIfOriginalExceptionIsNull() {
    Throwable unwrappedThrowable = FutureUtil.unwrapExceptions(SamzaException.class, null);
    assertNull(unwrappedThrowable);
  }
}

