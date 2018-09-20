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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsReporterFactory;
import scala.collection.JavaConverters;

/**
 * Helper class that instantiates the MetricsReporter.
 */
public class MetricsReporterLoader {

  private MetricsReporterLoader() {
  }

  public static Map<String, MetricsReporter> getMetricsReporters(MetricsConfig config, String containerName) {
    Map<String, MetricsReporter> metricsReporters = new HashMap<>();

    for (String metricsReporterName : JavaConverters.seqAsJavaListConverter(config.getMetricReporterNames()).asJava()) {
      String metricsFactoryClassName = config.getMetricsFactoryClass(metricsReporterName).get();
      if (metricsFactoryClassName == null) {
        throw new SamzaException(String.format("Metrics reporter %s missing .class config", metricsReporterName));
      }
      MetricsReporterFactory metricsReporterFactory = Util.getObj(metricsFactoryClassName, MetricsReporterFactory.class);
      metricsReporters.put(metricsReporterName,
                           metricsReporterFactory.getMetricsReporter(metricsReporterName,
                                                                     containerName,
                                                                     config));
    }
    return metricsReporters;
  }
}
