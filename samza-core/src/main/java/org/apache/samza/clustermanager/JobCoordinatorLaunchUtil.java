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
package org.apache.samza.clustermanager;

import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.execution.RemoteJobPlanner;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.DiagnosticsUtil;


public class JobCoordinatorLaunchUtil {
  /**
   * Run {@link ClusterBasedJobCoordinator} with full job config.
   *
   * @param app SamzaApplication to run.
   * @param config full job config.
   */
  @SuppressWarnings("rawtypes")
  public static void run(SamzaApplication app, Config config) {
    // Execute planning
    ApplicationDescriptorImpl<? extends ApplicationDescriptor>
        appDesc = ApplicationDescriptorUtil.getAppDescriptor(app, config);
    RemoteJobPlanner planner = new RemoteJobPlanner(appDesc);
    List<JobConfig> jobConfigs = planner.prepareJobs();

    if (jobConfigs.size() != 1) {
      throw new SamzaException("Only support single remote job is supported.");
    }

    Config finalConfig = jobConfigs.get(0);

    // This needs to be consistent with RemoteApplicationRunner#run where JobRunner#submit to be called instead of JobRunner#run
    CoordinatorStreamUtil.writeConfigToCoordinatorStream(finalConfig, true);
    DiagnosticsUtil.createDiagnosticsStream(finalConfig);
    MetricsRegistryMap metrics = new MetricsRegistryMap();
    MetadataStore
        metadataStore = new CoordinatorStreamStore(CoordinatorStreamUtil.buildCoordinatorStreamConfig(finalConfig), metrics);
    metadataStore.init();

    ClusterBasedJobCoordinator jc = new ClusterBasedJobCoordinator(
        metrics,
        metadataStore,
        finalConfig);
    jc.run();
  }

  private JobCoordinatorLaunchUtil() {}
}
