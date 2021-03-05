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

package org.apache.samza.config;

/**
 * Kubernetes related configurations
 */
public class KubeConfig {

  // the image name of samza
  public static final String APP_IMAGE = "kube.app.image";
  public static final String DEFAULT_IMAGE = "samza/samza:v0";

  // the default working directory
  public static final String DEFAULT_DIRECTORY = "/opt/samza/";

  // the memory and the cpu cores of container
  public static final int DEFAULT_CLUSTER_MANAGER_CONTAINER_MEM_SIZE = 1024;
  public static final int DEFAULT_CLUSTER_MANAGER_CONTAINER_CPU_CORE_NUM = 1;

  // The directory path inside which the log will be stored.
  public static final String SAMZA_MOUNT_DIR = "kube.app.pod.mnt.path";
  public static final String DEFAULT_SAMZA_MOUNT_DIR = "/tmp/mnt";
  public static final String K8S_API_NAMESPACE = "kube.app.namespace";
  public static final String STREAM_PROCESSOR_CONTAINER_NAME_PREFIX = "sp";
  public static final String JC_CONTAINER_NAME_PREFIX = "jc";
  public static final String POD_RESTART_POLICY = "OnFailure";
  public static final String JC_POD_NAME_FORMAT = "%s-%s-%s"; // jc-appName-appId
  public static final String TASK_POD_NAME_FORMAT = "%s-%s-%s-%s"; // sp-appName-appId-containerId

  // Environment variable
  public static final String COORDINATOR_POD_NAME = "COORDINATOR_POD_NAME";
  public static final String AZURE_REMOTE_VOLUME_ENABLED = "kube.app.volume.azure.file-share.enabled";
  public static final String AZURE_SECRET = "kube.app.volume.azure-secret";
  public static final String DEFAULT_AZURE_SECRET = "azure-secret";
  public static final String AZURE_FILESHARE = "kube.app.volume.azure.file-share";
  public static final String DEFAULT_AZURE_FILESHARE = "aksshare";

  private Config config;

  public KubeConfig(Config config) {
    this.config = config;
  }

  public static KubeConfig validate(Config config) throws ConfigException {
    KubeConfig kc = new KubeConfig(config);
    kc.validate();
    return kc;
  }

  // TODO: SAMZA-2365: validate KubeConfig before starting the job
  private void validate() throws ConfigException {

  }
}
