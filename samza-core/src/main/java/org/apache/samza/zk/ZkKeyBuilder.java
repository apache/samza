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

package org.apache.samza.zk;

import org.apache.samza.SamzaException;


public class ZkKeyBuilder {
  private final String pathPrefix;
  public static final String PROCESSORS_PATH = "processors";
  public static final String PROCESSOR_ID_PREFIX = "processor-";

  public static final String JOBMODEL_VERSION_PATH = "jobModelVersion";

  public ZkKeyBuilder() {
    this("");
  }

  public ZkKeyBuilder(String pathPrefix) {
    this.pathPrefix = pathPrefix;
  }

  public String getProcessorsPath() {
    return String.format("/%s/%s", pathPrefix, PROCESSORS_PATH);
  }

  public static String parseIdFromPath(String path) {
    if (path != null)
      return path.substring(path.indexOf(PROCESSOR_ID_PREFIX));
    return null;
  }

  public static String parseContainerIdFromProcessorId(String prId) {
    if (prId == null)
      throw new SamzaException("processor id is null");

    return prId.substring(prId.indexOf(PROCESSOR_ID_PREFIX) + PROCESSOR_ID_PREFIX.length());
  }

  public String getJobModelVersionPath() {
    return String.format("/%s/%s", pathPrefix, JOBMODEL_VERSION_PATH);
  }

  public String getJobModelPathPrefix() {
    return String.format("/%s/jobModels", pathPrefix);
  }

  public String getJobModelPath(String jobModelVersion) {
    return String.format("%s/%s", getJobModelPathPrefix(), jobModelVersion);
  }

  public String getJobModelVersionBarrierPrefix() {
    return String.format("/%s/versionBarriers", pathPrefix);
  }

  public String getRootPath() {
    return "/" + pathPrefix;
  }
}
