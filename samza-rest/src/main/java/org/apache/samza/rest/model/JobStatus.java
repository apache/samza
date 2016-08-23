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
package org.apache.samza.rest.model;

/**
 * The abstract status of the job, irrespective of the status in any underlying cluster management system (e.g. YARN).
 * This status is the client view of the job status.
 */
public enum JobStatus {

    /** Job is in the process of starting but is not yet running. */
    STARTING("starting"),

    /** Job has been started. */
    STARTED("started"),

    /** Job has been stopped. */
    STOPPED("stopped"),

    /** Job status is unknown. */
    UNKNOWN("unknown");

  private final String stringVal;

  JobStatus(final String stringVal) {
    this.stringVal = stringVal;
  }

  @Override
  public String toString() {
    return stringVal;
  }

  public boolean hasBeenStarted() {
    return !(this == STOPPED || this == UNKNOWN);
  }
}
