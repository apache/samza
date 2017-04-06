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

package org.apache.samza.util

import org.apache.samza.util.ExponentialSleepStrategy.RetryLoop
import org.junit.Assert._
import org.junit.Test

class TestExponentialSleepStrategy {

  @Test def testGetNextDelayReturnsIncrementalDelay {
    val strategy = new ExponentialSleepStrategy
    assertEquals(100, strategy.getNextDelay(0))
    assertEquals(200, strategy.getNextDelay(100))
    assertEquals(400, strategy.getNextDelay(200))
    assertEquals(800, strategy.getNextDelay(400))
  }

  @Test def testGetNextDelayReturnsMaximumDelayWhenDelayCapReached {
    val strategy = new ExponentialSleepStrategy
    assertEquals(10000, strategy.getNextDelay(6400))
    assertEquals(10000, strategy.getNextDelay(10000))
  }

  @Test def testSleepStrategyIsConfigurable {
    val strategy = new ExponentialSleepStrategy(backOffMultiplier = 3.0, initialDelayMs = 10)
    assertEquals(10, strategy.getNextDelay(0))
    assertEquals(30, strategy.getNextDelay(10))
    assertEquals(90, strategy.getNextDelay(30))
    assertEquals(270, strategy.getNextDelay(90))
  }

  @Test def testResetToInitialDelay {
    val strategy = new ExponentialSleepStrategy
    val loop = strategy.startLoop.asInstanceOf[ExponentialSleepStrategy#RetryLoopState]
    loop.previousDelay = strategy.getNextDelay(loop.previousDelay)
    assertEquals(100, loop.previousDelay)
    loop.previousDelay = strategy.getNextDelay(loop.previousDelay)
    loop.previousDelay = strategy.getNextDelay(loop.previousDelay)
    assertEquals(400, loop.previousDelay)
    loop.reset
    loop.previousDelay = strategy.getNextDelay(loop.previousDelay)
    assertEquals(100, loop.previousDelay)
  }

  @Test def testRetryWithoutException {
    val strategy = new ExponentialSleepStrategy(initialDelayMs = 1)
    var iterations = 0
    var loopObject: RetryLoop = null
    val result = strategy.run(
      loop => {
        loopObject = loop
        iterations += 1
        if (iterations == 3) loop.done
        iterations
      },
      (exception, loop) => throw exception
    )
    assertEquals(Some(3), result)
    assertEquals(3, iterations)
    assertEquals(2, loopObject.sleepCount)
  }

  @Test def testRetryWithException {
    val strategy = new ExponentialSleepStrategy(initialDelayMs = 1)
    var iterations = 0
    var loopObject: RetryLoop = null
    strategy.run(
      loop => { throw new IllegalArgumentException("boom") },
      (exception, loop) => {
        assertEquals("boom", exception.getMessage)
        loopObject = loop
        iterations += 1
        if (iterations == 3) loop.done
      }
    )
    assertEquals(3, iterations)
    assertEquals(2, loopObject.sleepCount)
  }

  @Test def testReThrowingException {
    val strategy = new ExponentialSleepStrategy(initialDelayMs = 1)
    var iterations = 0
    var loopObject: RetryLoop = null
    try {
      strategy.run(
        loop => {
          loopObject = loop
          iterations += 1
          throw new IllegalArgumentException("boom")
        },
        (exception, loop) => throw exception
      )
      fail("expected exception to be thrown")
    } catch {
      case e: IllegalArgumentException => assertEquals("boom", e.getMessage)
    }
    assertEquals(1, iterations)
    assertEquals(0, loopObject.sleepCount)
  }

  def interruptedThread(operation: => Unit) = {
    var exception: Option[Throwable] = None
    val interruptee = new Thread(new Runnable {
      def run {
        try { operation } catch { case e: Exception => exception = Some(e) }
      }
    })
    interruptee.start
    Thread.sleep(10) // give the thread a chance to make some progress before we interrupt it
    interruptee.interrupt
    interruptee.join
    exception
  }

  @Test def testThreadInterruptInRetryLoop {
    val strategy = new ExponentialSleepStrategy
    var iterations = 0
    var loopObject: RetryLoop = null
    val exception = interruptedThread {
      strategy.run(
        loop => { iterations += 1; loopObject = loop },
        (exception, loop) => throw exception
      )
    }
    assertEquals(classOf[InterruptedException], exception.get.getClass)
  }

  @Test def testThreadInterruptInOperationSleep {
    val strategy = new ExponentialSleepStrategy
    var iterations = 0
    var loopObject: RetryLoop = null
    val exception = interruptedThread {
      strategy.run(
        loop => { iterations += 1; loopObject = loop; Thread.sleep(1000) },
        (exception, loop) => throw exception
      )
    }
    assertEquals(classOf[InterruptedException], exception.get.getClass)
  }
}
