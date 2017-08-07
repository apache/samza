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

package org.apache.samza;

import org.apache.samza.job.model.JobModel;


/**
 * Bundle class for current and previous - job model and job model version.
 * Used for publishing updated data to the blob in one go.
 */
public class JobModelBundle {

  private JobModel prevJobModel;
  private JobModel currJobModel;
  private String prevJobModelVersion;
  private String currJobModelVersion;

  public JobModelBundle() {}

  public JobModelBundle(JobModel prevJM, JobModel currJM, String prevJMV, String currJMV) {
    prevJobModel = prevJM;
    currJobModel = currJM;
    prevJobModelVersion = prevJMV;
    currJobModelVersion = currJMV;
  }

  public JobModel getCurrJobModel() {
    return currJobModel;
  }

  public JobModel getPrevJobModel() {
    return prevJobModel;
  }

  public String getCurrJobModelVersion() {
    return currJobModelVersion;
  }

  public String getPrevJobModelVersion() {
    return prevJobModelVersion;
  }

}
