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

package org.apache.samza.metrics;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import org.apache.samza.container.SamzaContainerMetrics;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJmxMetricsAccessor {
  private JmxMetricsAccessor jmxMetricsAccessor;
  private Set<ObjectName> objectNames;
  private MBeanServerConnection conn;

  @Before
  public void setup() throws Exception {
    jmxMetricsAccessor = new JmxMetricsAccessor("dummyurl");
    JMXConnector jmxc = mock(JMXConnector.class);
    conn = mock(MBeanServerConnection.class);
    when(jmxc.getMBeanServerConnection()).thenReturn(conn);
    objectNames = new HashSet<>();
    when(conn.queryNames(any(ObjectName.class), any(QueryExp.class))).thenReturn(objectNames);
    Field jmxcField = JmxMetricsAccessor.class.getDeclaredField("jmxc");
    jmxcField.setAccessible(true);
    jmxcField.set(jmxMetricsAccessor, jmxc);
  }

  @Test
  public void testGetCounterValues() throws Exception {
    ObjectName counterObject = JmxUtil.getObjectName(SamzaContainerMetrics.class.getName(), "commit-calls", "samza-container-0");
    objectNames.add(counterObject);
    Long commitCalls = 100L;
    when(conn.getAttribute(counterObject, "Count")).thenReturn(commitCalls);

    Map<String, Long> result = jmxMetricsAccessor.getCounterValues(SamzaContainerMetrics.class.getName(),
        "commit-calls");
    assertTrue(result.size() == 1);
    assertTrue(result.get("samza-container-0").equals(commitCalls));
  }

  @Test
  public void testGetGaugeValues() throws Exception {
    ObjectName gaugeObject = JmxUtil.getObjectName(SamzaContainerMetrics.class.getName(), "event-loop-utilization", "samza-container-1");
    objectNames.add(gaugeObject);
    Double loopUtil = 0.8;
    when(conn.getAttribute(gaugeObject, "Value")).thenReturn(loopUtil);

    Map<String, Double> result = jmxMetricsAccessor.getGaugeValues(SamzaContainerMetrics.class.getName(), "event-loop-utilization");
    assertTrue(result.size() == 1);
    assertTrue(result.get("samza-container-1").equals(loopUtil));
  }

  @Test
  public void testGetTimerValues() throws Exception {
    ObjectName timerObject = JmxUtil.getObjectName(SamzaContainerMetrics.class.getName(), "choose-ns", "samza-container-2");
    objectNames.add(timerObject);
    Double time = 42.42;
    when(conn.getAttribute(timerObject, "AverageTime")).thenReturn(time);

    Map<String, Double> result = jmxMetricsAccessor.getTimerValues(SamzaContainerMetrics.class.getName(), "choose-ns");
    assertTrue(result.size() == 1);
    assertTrue(result.get("samza-container-2").equals(time));
  }
}
