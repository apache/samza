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

import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMathUtils {

  @Test(expected = IllegalArgumentException.class)
  public void testGcdWithNullInputs() {
    MathUtil.gcd(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGcdWithEmptyInputs() {
    MathUtil.gcd(Collections.emptyList());
  }

  @Test
  public void testGcdWithValidInputs() {
    // gcd(x, x) = x
    assertEquals(2, MathUtil.gcd(ImmutableList.of(2L, 2L)));
    assertEquals(15, MathUtil.gcd(ImmutableList.of(15L)));
    assertEquals(1, MathUtil.gcd(ImmutableList.of(1L)));

    // gcd(0,x) = x
    assertEquals(2, MathUtil.gcd(ImmutableList.of(2L, 0L)));

    // gcd(1,x) = 1
    assertEquals(1, MathUtil.gcd(ImmutableList.of(10L, 20L, 30L, 40L, 50L, 1L)));

    // other happy path test cases
    assertEquals(10, MathUtil.gcd(ImmutableList.of(10L, 20L, 30L, 40L, 50L, 0L)));
    assertEquals(10, MathUtil.gcd(ImmutableList.of(10L, 20L, 30L, 40L, 50L)));
    assertEquals(5, MathUtil.gcd(ImmutableList.of(25L, 35L, 45L, 55L)));

    assertEquals(1, MathUtil.gcd(ImmutableList.of(25L, 35L, 45L, 55L, 13L)));
  }

  @Test
  public void testClampAdd() {
    assertEquals(0, MathUtil.clampAdd(0, 0));
    assertEquals(2, MathUtil.clampAdd(1, 1));
    assertEquals(-2, MathUtil.clampAdd(-1, -1));
    assertEquals(Long.MAX_VALUE, MathUtil.clampAdd(Long.MAX_VALUE, 0));
    assertEquals(Long.MAX_VALUE - 1, MathUtil.clampAdd(Long.MAX_VALUE, -1));
    assertEquals(Long.MAX_VALUE, MathUtil.clampAdd(Long.MAX_VALUE, 1));
    assertEquals(Long.MAX_VALUE, MathUtil.clampAdd(Long.MAX_VALUE, Long.MAX_VALUE));
    assertEquals(Long.MIN_VALUE, MathUtil.clampAdd(Long.MIN_VALUE, 0));
    assertEquals(Long.MIN_VALUE, MathUtil.clampAdd(Long.MIN_VALUE, -1));
    assertEquals(Long.MIN_VALUE + 1, MathUtil.clampAdd(Long.MIN_VALUE, 1));
    assertEquals(Long.MIN_VALUE, MathUtil.clampAdd(Long.MIN_VALUE, Long.MIN_VALUE));
    assertEquals(-1, MathUtil.clampAdd(Long.MAX_VALUE, Long.MIN_VALUE));
  }
}
