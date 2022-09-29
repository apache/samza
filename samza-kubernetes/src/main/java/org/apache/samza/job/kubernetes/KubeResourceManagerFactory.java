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
package org.apache.samza.job.kubernetes;

import org.apache.samza.clustermanager.ClusterResourceManager;
import org.apache.samza.clustermanager.ResourceManagerFactory;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobModelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A KubeResourceManagerFactory returns an implementation of a {@link ClusterResourceManager} for Kubernetes.
 */
public class KubeResourceManagerFactory implements ResourceManagerFactory {
  private static Logger log = LoggerFactory.getLogger(KubeResourceManagerFactory.class);

  @Override
  public ClusterResourceManager getClusterResourceManager(ClusterResourceManager.Callback callback, SamzaApplicationState state) {
    JobModelManager jobModelManager = state.jobModelManager;
    Config config = jobModelManager.jobModel().getConfig();
    KubeClusterResourceManager manager = new KubeClusterResourceManager(config, jobModelManager, callback);
    log.info("KubeClusterResourceManager created");
    return manager;
  }
}
