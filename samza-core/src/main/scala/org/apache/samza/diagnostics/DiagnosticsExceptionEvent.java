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
package org.apache.samza.diagnostics;

import java.util.Map;


/**
 * This class encapsulates information related to an exception event that is useful for diagnostics.
 * It used to define container, task, and other metrics as
 * {@link org.apache.samza.metrics.ListGauge} of type {@link DiagnosticsExceptionEvent}.
 */
public class DiagnosticsExceptionEvent {

  private long timestamp; // the timestamp associated with this exception
  private Throwable throwable;
  private Map mdcMap;
      // the MDC map associated with this exception, used to store/obtain any context associated with the throwable

  public DiagnosticsExceptionEvent(long timestampMillis, Throwable throwable, Map mdcMap) {
    this.throwable = throwable;
    this.timestamp = timestampMillis;
    this.mdcMap = mdcMap;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Throwable getThrowable() {
    return this.throwable;
  }

  public Map getMdcMap() {
    return mdcMap;
  }
}