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

import java.nio.channels.ClosedByInterruptException
import org.apache.samza.util.ExponentialSleepStrategy.RetryLoop

/**
 * Encapsulates the pattern of retrying an operation until it succeeds.
 * Before every retry there is a delay, which starts short and gets exponentially
 * longer on each retry, up to a configurable maximum. There is no limit to the
 * number of retries.
 *
 * @param backOffMultiplier The factor by which the delay increases on each retry.
 * @param initialDelayMs Time in milliseconds to wait after the first attempt failed.
 * @param maximumDelayMs Cap up to which we will increase the delay.
 */
class ExponentialSleepStrategy(
    backOffMultiplier: Double = 2.0,
    initialDelayMs: Long = 100,
    maximumDelayMs: Long = 10000) {

  require(backOffMultiplier > 1.0, "backOffMultiplier must be greater than 1")
  require(initialDelayMs > 0, "initialDelayMs must be positive")
  require(maximumDelayMs >= initialDelayMs, "maximumDelayMs must be >= initialDelayMs")

  /**
   * Given the delay before the last retry, calculate what the delay before the
   * next retry should be.
   */
  def getNextDelay(previousDelay: Long): Long = {
    val nextDelay = (previousDelay * backOffMultiplier).asInstanceOf[Long]
    math.min(math.max(initialDelayMs, nextDelay), maximumDelayMs)
  }

  /** Can be overridden by subclasses to customize looping behavior. */
  def startLoop: RetryLoop = new RetryLoopState

  /**
   * Starts a retryable operation with the delay properties that were configured
   * when the object was created. Every call to run is independent, so the same
   * ExponentialSleepStrategy object can be used for several different retry loops.
   *
   * loopOperation is called on every attempt, and given as parameter a RetryLoop
   * object. By default it is assumed that the operation failed. If the operation
   * succeeded, you must call <code>done</code> on the RetryLoop object to indicate
   * success. This method returns the return value of the successful loopOperation.
   *
   * If an exception is thrown during the execution of loopOperation, the onException
   * handler is called. You can choose to re-throw the exception (so that it aborts
   * the run loop and bubbles up), or ignore it (the operation will be retried),
   * or call <code>done</code> (give up, don't retry).
   *
   * @param loopOperation The operation that should be attempted and may fail.
   * @param onException Handler function that determines what to do with an exception.
   * @return If loopOperation succeeded, an option containing the return value of
   *         the last invocation. If done was called in the exception hander, None.
   */
  def run[A](loopOperation: RetryLoop => A, onException: (Exception, RetryLoop) => Unit): Option[A] = {
    val loop = startLoop
    while (!loop.isDone && !Thread.currentThread.isInterrupted) {
      try {
        val result = loopOperation(loop)
        if (loop.isDone) return Some(result)
      } catch {
        case e: InterruptedException       => throw e
        case e: ClosedByInterruptException => throw e
        case e: OutOfMemoryError           => throw e
        case e: StackOverflowError         => throw e
        case e: Exception                  => onException(e, loop)
      }
      if (!loop.isDone && !Thread.currentThread.isInterrupted) loop.sleep
    }
    None
  }

  private[util] class RetryLoopState extends RetryLoop {
    var previousDelay = 0L
    var isDone = false
    var sleepCount = 0

    def sleep {
      sleepCount += 1
      val nextDelay = getNextDelay(previousDelay)
      previousDelay = nextDelay
      Thread.sleep(nextDelay)
    }

    def reset {
      previousDelay = 0
      isDone = false
    }

    def done {
      isDone = true
    }
  }
}

object ExponentialSleepStrategy {
  /**
   * State of the retry loop, passed to every invocation of the loopOperation
   * or the exception handler.
   */
  trait RetryLoop {
    /** Let the current thread sleep for the backoff time (called by run method). */
    def sleep

    /** Tell the retry loop to revert to initialDelayMs for the next retry. */
    def reset

    /** Tell the retry loop to stop trying (success or giving up). */
    def done

    /** Check whether <code>done</code> was called (used by the run method). */
    def isDone: Boolean

    /** Returns the number of times that the retry loop has called <code>sleep</code>. */
    def sleepCount: Int
  }

  /** For tests using ExponentialSleepStrategy.Mock */
  class CallLimitReached extends Exception

  /**
   * For writing tests of retryable code. Doesn't actually sleep, so that tests
   * are quick to run.
   *
   * @param maxCalls The maximum number of retries to allow before throwing CallLimitReached.
   */
  class Mock(maxCalls: Int) extends ExponentialSleepStrategy {
    override def startLoop = new MockRetryLoop

    class MockRetryLoop extends RetryLoop {
      var isDone = false
      var sleepCount = 0
      def sleep { sleepCount += 1; if (sleepCount > maxCalls) throw new CallLimitReached }
      def reset { isDone = false }
      def done  { isDone = true  }
    }
  }
}
