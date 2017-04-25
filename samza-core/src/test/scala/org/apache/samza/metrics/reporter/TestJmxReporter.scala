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

package org.apache.samza.metrics.reporter

import javax.management.MBeanServer
import javax.management.ObjectName
import junit.framework.Assert.assertEquals
import org.apache.samza.metrics.JmxUtil
import org.apache.samza.metrics.MetricsRegistryMap
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.Mockito.reset

class TestJmxReporter {

  val REPORTER_SOURCE = "test"

  @Test
  def testJmxReporter {
    val metricGroup = "org.apache.samza.metrics.JvmMetrics"
    val metricName = "mem-non-heap-used-mb"
    val objectName: ObjectName = JmxUtil.getObjectName(metricGroup, metricName, REPORTER_SOURCE)

    val registry: MetricsRegistryMap = new MetricsRegistryMap
    val mBeanServerMock: MBeanServer = mock(classOf[MBeanServer])

    // Create dummy test metric.
    registry.newCounter(metricGroup, metricName)

    when(mBeanServerMock.isRegistered(objectName)).thenReturn(false)

    val reporter = new JmxReporter(mBeanServerMock)
    reporter.register(REPORTER_SOURCE, registry)
    reporter.start

    verify(mBeanServerMock, times(1)).registerMBean(Matchers.anyObject(), Matchers.eq(objectName))

    reset(mBeanServerMock)
    // Create dummy counter to test metrics reporting through listener.
    registry.newCounter(metricGroup, metricName)

    assertEquals(1, registry.listeners.size)
    verify(mBeanServerMock, times(1)).registerMBean(Matchers.anyObject(), Matchers.eq(objectName))

    reporter.stop
  }
}
