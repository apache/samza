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

import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestSamzaHistogram {
  private static final String GROUP = "Group0";
  private static final String METRIC_NAME = "Metric1";

  private MetricsRegistry metricsRegistry;

  @Test
  public void testCreateHistogramGaugeNullCheck() {
    metricsRegistry = mock(MetricsRegistry.class);

    doAnswer((Answer<Gauge<Double>>) invocation -> {
      Object[] args = invocation.getArguments();
      return new Gauge<>((String) args[0], (Double) ((Gauge) args[1]).getValue());
    }).when(metricsRegistry).newGauge(anyString(), any(Gauge.class));

    SamzaHistogram histogram = new SamzaHistogram(metricsRegistry, GROUP, METRIC_NAME);
    assertNotNull(histogram);
  }
}