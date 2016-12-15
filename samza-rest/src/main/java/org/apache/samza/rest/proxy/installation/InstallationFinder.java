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
package org.apache.samza.rest.proxy.installation;

import java.util.Map;
import org.apache.samza.rest.proxy.job.JobInstance;


/**
 * Finds all the installed jobs. For example, one implementation may take an installation root directory
 * and scan all subdirectories for job installations.
 *
 * Provides a map from a {@link JobInstance} to its {@link InstallationRecord} based on the structure within the
 * installation directory. Implementations of this interface should encapsulate any custom installation
 * structure such that the resulting {@link InstallationRecord} simply contains the locations of the files
 * needed to control the job.
 */
public interface InstallationFinder {

  /**
   * @param jobInstance the job to check.
   * @return            <code>true</code> if a job with the specified name and id is installed on the local host.
   */
  boolean isInstalled(JobInstance jobInstance);

  /**
   * @return  a map from each {@link JobInstance} to the corresponding {@link InstallationRecord}
   *          for each Samza installation found in the installRoot.
   */
  Map<JobInstance, InstallationRecord> getAllInstalledJobs();
}
