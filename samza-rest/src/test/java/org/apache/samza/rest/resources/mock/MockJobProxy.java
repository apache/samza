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
package org.apache.samza.rest.resources.mock;

import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.samza.rest.proxy.job.AbstractJobProxy;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.proxy.job.JobStatusProvider;
import org.apache.samza.rest.resources.JobsResourceConfig;


public class MockJobProxy extends AbstractJobProxy {

  public static final String JOB_INSTANCE_1_NAME = "Job1";
  public static final String JOB_INSTANCE_1_ID = "i001";

  public static final String JOB_INSTANCE_2_NAME = "Job1";
  public static final String JOB_INSTANCE_2_ID = "i002";

  public static final String JOB_INSTANCE_3_NAME = "Job2";
  public static final String JOB_INSTANCE_3_ID = "i001";

  public static final String JOB_INSTANCE_4_NAME = "Job3";
  public static final String JOB_INSTANCE_4_ID = "1";
  /**
   * Required constructor.
   *

   * @param config  the config containing the installations path.
   */
  public MockJobProxy(JobsResourceConfig config) {
    super(config);
  }

  @Override
  protected JobStatusProvider getJobStatusProvider() {
    return new MockJobStatusProvider();
  }

  @Override
  protected Set<JobInstance> getAllJobInstances() {
    Set<JobInstance> validatedInstallations = new LinkedHashSet<>();

    validatedInstallations.add(new JobInstance(JOB_INSTANCE_1_NAME, JOB_INSTANCE_1_ID));
    validatedInstallations.add(new JobInstance(JOB_INSTANCE_2_NAME, JOB_INSTANCE_2_ID));
    validatedInstallations.add(new JobInstance(JOB_INSTANCE_3_NAME, JOB_INSTANCE_3_ID));

    validatedInstallations.add(new JobInstance(JOB_INSTANCE_4_NAME, JOB_INSTANCE_4_ID));

    return validatedInstallations;
  }

  @Override
  public void start(JobInstance jobInstance)
      throws Exception {

  }

  @Override
  public void stop(JobInstance jobInstance)
      throws Exception {

  }
}
