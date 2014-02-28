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

import org.apache.samza.util.ExponentialThreadSleepStrategy
import org.junit.Assert._
import org.junit.Test

class TestExponentialThreadSleepStrategy {

  @Test def testGetNextRedeliveryDelayCorrectlyReturnsMaximumDelayWhenDelayCapReached() = {
    val st = new ExponentialThreadSleepStrategy
    var nextDelay = st.getNextDelay(ExponentialThreadSleepStrategy.SLEEP_CAP_DELAY_MS)
    assertEquals(nextDelay, 10000l)
    nextDelay = st.getNextDelay(ExponentialThreadSleepStrategy.SLEEP_CAP_DELAY_MS)
    assertEquals(nextDelay, 10000l)
  }

  @Test def testGetNextRedeliveryDelayCorrectlyReturnsIncrementalDelay() = {
    val st = new ExponentialThreadSleepStrategy
    var nextDelay = st.getNextDelay(0l)
    assertEquals(nextDelay, 100l)
    nextDelay = st.getNextDelay(nextDelay)
    assertEquals(nextDelay, 200l)
    nextDelay = st.getNextDelay(nextDelay)
    assertEquals(nextDelay, 400l)
  }
}


