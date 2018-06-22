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

import org.apache.log4j.spi.ThrowableInformation;


/**
 * This class encapsulates information related to an exception event that is useful for diagnostics.
 * It used to define container, task, and other metrics as
 * {@link org.apache.samza.metrics.ListGauge} of type {@link DiagnosticsExceptionEvent}.
 */
public class DiagnosticsExceptionEvent {

  private long timestamp; // the timestamp associated with this exception
  private ThrowableInformation throwableInformation;

  public DiagnosticsExceptionEvent(long timestampMillis, ThrowableInformation throwableInformation) {
    this.throwableInformation = throwableInformation;
    this.timestamp = timestampMillis;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public ThrowableInformation getThrowableInformation() {
    return this.throwableInformation;
  }
}