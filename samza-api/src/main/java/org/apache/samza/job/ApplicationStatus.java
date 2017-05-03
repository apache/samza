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

package org.apache.samza.job;

/**
 * Status of a {@link org.apache.samza.job.StreamJob} during and after its run.
 */
public class ApplicationStatus {
  public static final ApplicationStatus New = new ApplicationStatus(StatusCode.New, null);
  public static final ApplicationStatus Running = new ApplicationStatus(StatusCode.Running, null);
  public static final ApplicationStatus SuccessfulFinish = new ApplicationStatus(StatusCode.SuccessfulFinish, null);
  public static final ApplicationStatus UnsuccessfulFinish = new ApplicationStatus(StatusCode.UnsuccessfulFinish, null);

  public enum StatusCode {
    New,
    Running,
    SuccessfulFinish,
    UnsuccessfulFinish
  }

  private final StatusCode statusCode;
  private final Throwable throwable;

  private ApplicationStatus(StatusCode code, Throwable t) {
    this.statusCode = code;
    this.throwable = t;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public Throwable getThrowable() {
    return throwable;
  }

  @Override
  public String toString() {
    return statusCode.name();
  }


  public static ApplicationStatus unsuccessfulFinish(Throwable t) {
    return new ApplicationStatus(StatusCode.UnsuccessfulFinish, t);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }

    ApplicationStatus rhs = (ApplicationStatus) obj;
    return statusCode.equals(rhs.statusCode);
  }

  @Override
  public int hashCode() {
    return statusCode.hashCode();
  }
}
