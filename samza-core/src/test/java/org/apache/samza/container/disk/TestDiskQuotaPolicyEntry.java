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

import static junit.framework.Assert.assertEquals;

public class TestDiskQuotaPolicyEntry {
  private static final double ARBITRARY_HIGH_WATER_MARK = 0.75;
  private static final double ARBITRARY_LOW_WATER_MARK = 0.25;
  private static final double ARBITRARY_WORK_FACTOR = 1.0;

  @Test
  public void testConstruction() {
    final WatermarkDiskQuotaPolicy.Entry policy = new WatermarkDiskQuotaPolicy.Entry(
        ARBITRARY_LOW_WATER_MARK,
        ARBITRARY_HIGH_WATER_MARK,
        ARBITRARY_WORK_FACTOR);

    assertEquals(ARBITRARY_LOW_WATER_MARK, policy.getLowWaterMarkPercent());
    assertEquals(ARBITRARY_HIGH_WATER_MARK, policy.getHighWaterMarkPercent());
    assertEquals(ARBITRARY_WORK_FACTOR, policy.getWorkFactor());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLowWaterMarkGreaterThanHighWaterMark() {
    new WatermarkDiskQuotaPolicy.Entry(
        1.0,
        ARBITRARY_HIGH_WATER_MARK,
        ARBITRARY_WORK_FACTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLowWaterMarkBelowZero() {
    new WatermarkDiskQuotaPolicy.Entry(
        -1.0,
        ARBITRARY_HIGH_WATER_MARK,
        ARBITRARY_WORK_FACTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHighWaterMarkAboveOne() {
    new WatermarkDiskQuotaPolicy.Entry(
        ARBITRARY_LOW_WATER_MARK,
        2.0,
        ARBITRARY_WORK_FACTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWorkFactorOfZero() {
    new WatermarkDiskQuotaPolicy.Entry(
        ARBITRARY_LOW_WATER_MARK,
        ARBITRARY_HIGH_WATER_MARK,
        0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWorkFactorGreaterThanOne() {
    new WatermarkDiskQuotaPolicy.Entry(
        ARBITRARY_LOW_WATER_MARK,
        ARBITRARY_HIGH_WATER_MARK,
        2.0);
  }
}
