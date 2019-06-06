/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License") you may not use this file except in compliance
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
package org.apache.samza.container

import com.google.common.collect.ImmutableMap
import org.apache.samza.config.{Config, MapConfig, TaskConfig}
import org.apache.samza.metrics.{Counter, MetricsHelper}
import org.junit.{Before, Test}
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar

class TestTaskInstanceExceptionHandler extends AssertionsForJUnit with MockitoSugar {
  @Mock
  private var metrics: MetricsHelper = null
  @Mock
  private var troublesomeExceptionCounter: Counter = null
  @Mock
  private var nonFatalExceptionCounter: Counter = null
  @Mock
  private var fatalExceptionCounter: Counter = null

  @Before
  def setup() {
    MockitoAnnotations.initMocks(this)
    when(this.metrics.newCounter("exception-ignored-" + classOf[TroublesomeException].getName)).thenReturn(
        this.troublesomeExceptionCounter)
    when(this.metrics.newCounter("exception-ignored-" + classOf[NonFatalException].getName)).thenReturn(
        this.nonFatalExceptionCounter)
    when(this.metrics.newCounter("exception-ignored-" + classOf[FatalException].getName)).thenReturn(
        this.fatalExceptionCounter)
  }

  /**
   * Given that no exceptions are ignored, any exception should get propogated up.
   */
  @Test
  def testHandleIgnoreNone() {
    val handler = build(new MapConfig())
    intercept[TroublesomeException] {
      handler.maybeHandle(() -> {
        throw new TroublesomeException()
      })
    }
    verifyZeroInteractions(this.metrics, this.troublesomeExceptionCounter, this.nonFatalExceptionCounter,
        this.fatalExceptionCounter)
  }

  /**
   * Given that some exceptions are ignored, the ignored exceptions should not be thrown and should increment the proper
   * metrics, and any other exception should get propagated up.
   */
  @Test
  def testHandleIgnoreSome() {
    val config = new MapConfig(ImmutableMap.of(TaskConfig.IGNORED_EXCEPTIONS,
        String.join(",", classOf[TroublesomeException].getName, classOf[NonFatalException].getName)))
    val handler = build(config)
    handler.maybeHandle(() -> {
      throw new TroublesomeException()
    })
    handler.maybeHandle(() -> {
      throw new NonFatalException()
    })
    intercept[FatalException] {
      handler.maybeHandle(() -> {
        throw new FatalException()
      })
    }
    handler.maybeHandle(() -> {
      throw new TroublesomeException()
    })
    verify(this.troublesomeExceptionCounter, times(2)).inc()
    // double check that the counter gets cached for multiple occurrences of the same exception type
    verify(this.metrics).newCounter("exception-ignored-" + classOf[TroublesomeException].getName)
    verify(this.nonFatalExceptionCounter).inc()
    verifyZeroInteractions(this.fatalExceptionCounter)
  }

  /**
   * Given that all exceptions are ignored, no exceptions should be thrown and the proper metrics should be incremented.
   */
  @Test
  def testHandleIgnoreAll() {
    val config = new MapConfig(ImmutableMap.of(TaskConfig.IGNORED_EXCEPTIONS, "*"))
    val handler = build(config)
    handler.maybeHandle(() -> {
      throw new TroublesomeException()
    })
    handler.maybeHandle(() -> {
      throw new TroublesomeException()
    })
    handler.maybeHandle(() -> {
      throw new NonFatalException()
    })
    handler.maybeHandle(() -> {
      throw new FatalException()
    })

    verify(this.troublesomeExceptionCounter, times(2)).inc()
    // double check that the counter gets cached for multiple occurrences of the same exception type
    verify(this.metrics).newCounter("exception-ignored-" + classOf[TroublesomeException].getName)
    verify(this.nonFatalExceptionCounter).inc()
    verify(this.fatalExceptionCounter).inc()
  }

  private def build(config: Config): TaskInstanceExceptionHandler = {
    TaskInstanceExceptionHandler.apply(this.metrics, new TaskConfig(config))
  }

  /**
   * Mock exception used to test exception counts metrics.
   */
  private class TroublesomeException extends RuntimeException {
  }

  /**
   * Mock exception used to test exception counts metrics.
   */
  private class NonFatalException extends RuntimeException {
  }

  /**
   * Mock exception used to test exception counts metrics.
   */
  private class FatalException extends RuntimeException {
  }
}
