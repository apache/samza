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

/**
 * This class encapsulates information related to an exception event that is useful for diagnostics.
 * It used to define container, task, and other metrics as
 * {@link org.apache.samza.metrics.ListGauge} of type {@link DiagnosticsExceptionEvent}.
 */
public class DiagnosticsExceptionEvent {

  private long timestamp; // the timestamp associated with this exception
  private String message;   // the string message associated with this exception
  private String threadName; // the name of the thread on which this exception occurred

  // a compact string representation of this exception, to avoid serializing the entire stack trace
  private String compactStackTrace;

  // a unique identifier computed to identify this stack trace
  private Object stackTraceIdentifier;

  public DiagnosticsExceptionEvent(long timestampMillis, String message, String threadName, String compactStackTrace,
      Object stackTraceIdentifier) {
    this.message = message;
    this.timestamp = timestampMillis;
    this.threadName = threadName;
    this.compactStackTrace = compactStackTrace;
    this.stackTraceIdentifier = stackTraceIdentifier;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getThreadName() {
    return threadName;
  }

  public void setThreadName(String threadName) {
    this.threadName = threadName;
  }

  public String getCompactStackTrace() {
    return compactStackTrace;
  }

  public void setCompactStackTrace(String compactStackTrace) {
    this.compactStackTrace = compactStackTrace;
  }

  public Object getStackTraceIdentifier() {
    return stackTraceIdentifier;
  }

  public void setStackTraceIdentifier(Object stackTraceIdentifier) {
    this.stackTraceIdentifier = stackTraceIdentifier;
  }
}