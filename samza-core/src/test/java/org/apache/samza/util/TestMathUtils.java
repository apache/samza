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
import junit.framework.Assert;
import org.apache.samza.operators.util.MathUtils;
import org.junit.Test;

import java.util.Collections;


public class TestMathUtils {

  @Test(expected = IllegalArgumentException.class)
  public void testGcdWithNullInputs() {
    MathUtils.gcd(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGcdWithEmptyInputs() {
    MathUtils.gcd(Collections.emptyList());
  }

  @Test
  public void testGcdWithValidInputs() {
    // gcd(x, x) = x
    Assert.assertEquals(2, MathUtils.gcd(ImmutableList.of(2L, 2L)));
    Assert.assertEquals(15, MathUtils.gcd(ImmutableList.of(15L)));
    Assert.assertEquals(1, MathUtils.gcd(ImmutableList.of(1L)));

    // gcd(0,x) = x
    Assert.assertEquals(2, MathUtils.gcd(ImmutableList.of(2L, 0L)));

    // gcd(1,x) = 1
    Assert.assertEquals(1, MathUtils.gcd(ImmutableList.of(10L, 20L, 30L, 40L, 50L, 1L)));

    // other happy path test cases
    Assert.assertEquals(10, MathUtils.gcd(ImmutableList.of(10L, 20L, 30L, 40L, 50L, 0L)));
    Assert.assertEquals(10, MathUtils.gcd(ImmutableList.of(10L, 20L, 30L, 40L, 50L)));
    Assert.assertEquals(5, MathUtils.gcd(ImmutableList.of(25L, 35L, 45L, 55L)));

    Assert.assertEquals(1, MathUtils.gcd(ImmutableList.of(25L, 35L, 45L, 55L, 13L)));
  }

}
