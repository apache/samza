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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JMX metrics accessor.
 * It connects to a container JMX url,and get metrics values by querying the MBeans.
 */
public class JmxMetricsAccessor implements MetricsAccessor {
  private static final Logger log = LoggerFactory.getLogger(JmxMetricsAccessor.class);

  private final String url;
  private JMXConnector jmxc;

  public JmxMetricsAccessor(String url) {
    this.url = url;
  }

  public void connect() throws IOException {
    JMXServiceURL jmxUrl = new JMXServiceURL(url);
    jmxc = JMXConnectorFactory.connect(jmxUrl, null);
  }

  public void close() throws IOException {
    jmxc.close();
  }

  private <T> Map<String, T> getMetricValues(String group, String metric, String attribute) {
    try {
      StringBuilder nameBuilder = new StringBuilder();
      nameBuilder.append(JmxUtil.makeNameJmxSafe(group));
      nameBuilder.append(":type=*,name=");
      nameBuilder.append(JmxUtil.makeNameJmxSafe(metric));
      ObjectName query = new ObjectName(nameBuilder.toString());
      Map<String, T> values = new HashMap<>();
      MBeanServerConnection conn = jmxc.getMBeanServerConnection();
      for (ObjectName objName : conn.queryNames(query, null)) {
        String type = objName.getKeyProperty("type");
        T val = (T) conn.getAttribute(objName, attribute);
        values.put(type, val);
      }
      return values;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Collections.EMPTY_MAP;
    }
  }

  @Override
  public Map<String, Long> getCounterValues(String group, String counter) {
    return getMetricValues(group, counter, "Count");
  }

  @Override
  public <T> Map<String, T> getGaugeValues(String group, String gauge) {
    return getMetricValues(group, gauge, "Value");
  }

  @Override
  public Map<String, Double> getTimerValues(String group, String timer) {
    return getMetricValues(group, timer, "AverageTime");
  }
}