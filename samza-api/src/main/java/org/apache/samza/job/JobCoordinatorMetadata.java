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

import java.util.Objects;
import org.apache.samza.annotation.InterfaceStability;

/**
 * A data model to represent the metadata of the job coordinator. The metadata refers to attributes of job coordinator
 * scoped to attempt within a deployment. For the purpose of this data model, deployment and attempt are defined
 * as follows
 *
 * Deployment - Set of actions to stop an existing application, install new binaries and submit a request to run the new binaries
 * Attempt    - Incarnations of application within a deployment for fault tolerance; e.g. Job coordinator failures or
 *              job model changes detected by partition count monitoring or regex monitor.
 *
 * Metadata generation may require underlying cluster manager's interaction. The following describes the properties
 * of the attributes to provide guidelines for implementors of contracts related to metadata generation.
 *
 * Epoch ID - An identifier to associated with the job coordinator's lifecycle within the scope of a single deployment.
 * The properties of the epoch identifier are as follows
 *    1. Unique across applications in the cluster
 *    2. Remains unchanged within a single deployment lifecycle
 *    3. Remains unchanged across application attempt within a single deployment lifecycle
 *    4. Changes across deployment lifecycle
 *
 * Config ID - An identifier associated with a subset of configuration snapshot used by the job in an application attempt.
 * Current prefixes that impacts the identifier are job.autosizing.*
 * The properties of the config identifier are as follows
 *    1. Unique and Reproducible
 *    2. Remains unchanged across application attempts / deployments as long as the subset of configuration remains unchanged.
 *
 * Job Model ID - An identifier associated with the JobModel used by the job in an application attempt. JobModel
 * has both configurations and list of container model. We don't account for changes in the configuration as part of this
 * identifier since it is separately tracked and handled by Config ID.
 * The properties of the job model identifier are as follows
 *    1. Unique and Reproducible
 *    2. Remains unchanged across application attempts / deployments as long as the work assignment remains unchanged
 *
 * Notes on interface stability - It is used internally by Samza for job coordinator high availability in YARN
 * deployment offering. It may evolve depending on expanding the scope beyond YARN and hence unstable.
 *
 */
@InterfaceStability.Unstable
public class JobCoordinatorMetadata {
  private final String configId;
  private final String epochId;
  private final String jobModelId;

  public JobCoordinatorMetadata(String epochId, String configId, String jobModelId) {
    this.configId = configId;
    this.epochId = epochId;
    this.jobModelId = jobModelId;
  }

  public String getConfigId() {
    return configId;
  }

  public String getEpochId() {
    return this.epochId;
  }

  public String getJobModelId() {
    return jobModelId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobCoordinatorMetadata)) {
      return false;
    }
    JobCoordinatorMetadata metadata = (JobCoordinatorMetadata) o;
    return Objects.equals(configId, metadata.configId) && Objects.equals(epochId, metadata.epochId)
        && Objects.equals(jobModelId, metadata.jobModelId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configId, epochId, jobModelId);
  }

  @Override
  public String toString() {
    return "JobCoordinatorMetadata{" + "configId='" + configId + '\'' + ", epochId='" + epochId + '\''
        + ", jobModelId='" + jobModelId + '\'' + '}';
  }
}
