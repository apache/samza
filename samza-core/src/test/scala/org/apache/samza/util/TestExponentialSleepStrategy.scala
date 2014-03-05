/*
 *
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
 *
 */

package org.apache.samza.util

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.util.ExponentialSleepStrategy

class TestExponentialSleepStrategy {

  @Test def testGetNextDelayReturnsIncrementalDelay() = {
    val st = new ExponentialSleepStrategy
    var nextDelay = st.getNextDelay(0L)
    assertEquals(nextDelay, 100L)
    nextDelay = st.getNextDelay(nextDelay)
    assertEquals(nextDelay, 200L)
    nextDelay = st.getNextDelay(nextDelay)
    assertEquals(nextDelay, 400L)
  }

  @Test def testGetNextDelayReturnsMaximumDelayWhenDelayCapReached() = {
    val st = new ExponentialSleepStrategy
    var nextDelay = st.getNextDelay(6400L)
    assertEquals(nextDelay, 10000L)
    nextDelay = st.getNextDelay(nextDelay)
    assertEquals(nextDelay, 10000L)
  }

  @Test def testSleepStrategyIsConfigurable() = {
    val st = new ExponentialSleepStrategy(backOffMultiplier = 3.0, initialDelayMs = 10)
    var nextDelay = st.getNextDelay(0L)
    assertEquals(nextDelay, 10L)
    nextDelay = st.getNextDelay(nextDelay)
    assertEquals(nextDelay, 30L)
    nextDelay = st.getNextDelay(nextDelay)
    assertEquals(nextDelay, 90L)
  }
}
