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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.exception.ExceptionUtils;


/**
 * This class encapsulates information related to an exception event that is useful for diagnostics.
 * It used to define container, task, and other metrics as
 * {@link org.apache.samza.metrics.ListGauge} of type {@link DiagnosticsExceptionEvent}.
 */
public class DiagnosticsExceptionEvent {

  private long timestamp; // the timestamp associated with this exception
  private Class exceptionType; // store the exception type separately
  private String exceptionMessage; // the exception message
  private String compactExceptionStackTrace; // a compact representation of the exception's stacktrace
  private Map mdcMap;
  // the MDC map associated with this exception, used to store/obtain any context associated with the throwable

  public DiagnosticsExceptionEvent() {
  }

  public DiagnosticsExceptionEvent(long timestampMillis, Throwable throwable, Map mdcMap) {
    this.exceptionType = throwable.getClass();
    this.exceptionMessage = throwable.getMessage();
    this.compactExceptionStackTrace = ExceptionUtils.getStackTrace(throwable);
    this.timestamp = timestampMillis;
    this.mdcMap = new HashMap(mdcMap);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Class getExceptionType() {
    return this.exceptionType;
  }

  public Map getMdcMap() {
    return mdcMap;
  }

  public String getExceptionMessage() {
    return exceptionMessage;
  }

  public String getCompactExceptionStackTrace() {
    return compactExceptionStackTrace;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DiagnosticsExceptionEvent that = (DiagnosticsExceptionEvent) o;
    return timestamp == that.timestamp && Objects.equals(exceptionType, that.exceptionType) && Objects.equals(
        exceptionMessage, that.exceptionMessage) && Objects.equals(compactExceptionStackTrace,
        that.compactExceptionStackTrace) && Objects.equals(mdcMap, that.mdcMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, exceptionType, exceptionMessage, compactExceptionStackTrace, mdcMap);
  }
}