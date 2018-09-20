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
package org.apache.samza.logging.log4j;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.MetricsRegistry;


public class StreamAppenderMetrics extends MetricsBase {
  /** The percentage of the log queue capacity that is currently filled with messages from 0 to 100. */
  public final Gauge<Integer> bufferFillPct;

  /** The number of recursive calls to the StreamAppender. These events will not be logged. */
  public final Counter recursiveCalls;

  /** The number of log messages dropped e.g. because of buffer overflow. Does not include recursive calls. */
  public final Counter logMessagesDropped;

  public StreamAppenderMetrics(String prefix, MetricsRegistry registry) {
    super(prefix, registry);
    bufferFillPct = newGauge("buffer-fill-percent", 0);
    recursiveCalls = newCounter("recursive-calls");
    logMessagesDropped = newCounter("log-messages-dropped");
  }
}
