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
package org.apache.samza.container.disk;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class TestWatermarkDiskQuotaPolicy {
  @Test
  public void testNoEntries() {
    WatermarkDiskQuotaPolicy policy = new WatermarkDiskQuotaPolicy(Collections.<WatermarkDiskQuotaPolicy.Entry>emptyList());

    assertEquals(1.0, policy.apply(1.0));
    assertEquals(1.0, policy.apply(0.5));
    assertEquals(1.0, policy.apply(0.0));
  }

  @Test
  public void testOneEntryUntriggered() {
    double workFactor = 0.5f;
    List<WatermarkDiskQuotaPolicy.Entry> entries = Collections.singletonList(new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, workFactor));
    WatermarkDiskQuotaPolicy policy = new WatermarkDiskQuotaPolicy(entries);

    assertEquals(1.0, policy.apply(1.0));
    assertEquals(1.0, policy.apply(0.75));
    assertEquals(1.0, policy.apply(0.5));
  }

  @Test
  public void testOneEntryTriggered() {
    double workFactor = 0.5f;
    List<WatermarkDiskQuotaPolicy.Entry> entries = Collections.singletonList(new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, workFactor));
    WatermarkDiskQuotaPolicy policy = new WatermarkDiskQuotaPolicy(entries);

    assertEquals(workFactor, policy.apply(0.25));
  }

  @Test
  public void testTwoEntriesTriggered() {
    double workFactor1 = 0.5f;
    double workFactor2 = 0.25f;
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, workFactor1),
        new WatermarkDiskQuotaPolicy.Entry(0.2, 0.4, workFactor2));
    WatermarkDiskQuotaPolicy policy = new WatermarkDiskQuotaPolicy(entries);

    assertEquals(1.0, policy.apply(0.5));
    assertEquals(workFactor1, policy.apply(0.4));
    assertEquals(workFactor2, policy.apply(0.1));
  }

  @Test
  public void testTwoEntriesTriggeredSkipFirst() {
    double workFactor1 = 0.5f;
    double workFactor2 = 0.25f;
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, workFactor1),
        new WatermarkDiskQuotaPolicy.Entry(0.2, 0.4, workFactor2));
    WatermarkDiskQuotaPolicy policy = new WatermarkDiskQuotaPolicy(entries);

    assertEquals(workFactor2, policy.apply(0.1));
  }

  @Test
  public void testTwoEntriesReversedOrder() {
    // Results should be the same regardless of order as we sort policies at construction time.
    double workFactor1 = 0.5f;
    double workFactor2 = 0.25f;
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.2, 0.4, workFactor2),
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, workFactor1));
    WatermarkDiskQuotaPolicy policy = new WatermarkDiskQuotaPolicy(entries);

    assertEquals(workFactor2, policy.apply(0.1));
  }

  @Test
  public void testTriggerEntriesAndRecover() {
    double workFactor1 = 0.5f;
    double workFactor2 = 0.25f;
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, workFactor1),
        new WatermarkDiskQuotaPolicy.Entry(0.2, 0.4, workFactor2));
    WatermarkDiskQuotaPolicy policy = new WatermarkDiskQuotaPolicy(entries);

    assertEquals(workFactor2, policy.apply(0.1));
    assertEquals(workFactor1, policy.apply(0.4));
    assertEquals(1.0, policy.apply(1.0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWorkFactorTooHigh() {
    new WatermarkDiskQuotaPolicy(
        Collections.singletonList(new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, 1.5)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWorkFactorTooLow() {
    new WatermarkDiskQuotaPolicy(
        Collections.singletonList(new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, -1.0)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRaiseWorkFactorWithLowerThreshold() {
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, 0.5),
        new WatermarkDiskQuotaPolicy.Entry(0.2, 0.4, 0.75));
    new WatermarkDiskQuotaPolicy(entries);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicatedRange() {
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, 0.5),
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, 0.75));
    new WatermarkDiskQuotaPolicy(entries);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFullyOverlappedRange1() {
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.5, 1.0, 0.5),
        new WatermarkDiskQuotaPolicy.Entry(0.25, 1.0, 0.75));
    new WatermarkDiskQuotaPolicy(entries);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFullyOverlappedRange2() {
    List<WatermarkDiskQuotaPolicy.Entry> entries = Arrays.asList(
        new WatermarkDiskQuotaPolicy.Entry(0.5, 0.75, 0.75),
        new WatermarkDiskQuotaPolicy.Entry(0.25, 1.0, 0.5));
    new WatermarkDiskQuotaPolicy(entries);
  }
}
