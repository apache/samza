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

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import java.net.URL;
import java.util.*;
import org.apache.samza.clustermanager.ClusterResourceManager;
import org.apache.samza.clustermanager.ResourceRequestState;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.SamzaResourceRequest;
import org.apache.samza.clustermanager.SamzaResource;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.server.LocalityServlet;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.ShellCommandBuilder;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.config.ApplicationConfig.*;
import static org.apache.samza.config.KubeConfig.*;

/**
 * An {@link KubeClusterResourceManager} implements a ClusterResourceManager using Kubernetes as the underlying
 * resource manager.
 */
public class KubeClusterResourceManager extends ClusterResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(KubeClusterResourceManager.class);
  private final Map<String, String> podLabels = new HashMap<>();
  private KubernetesClient client;
  private String appId;
  private String appName;
  private String image;
  private String namespace;
  private OwnerReference ownerReference;
  private JobModelManager jobModelManager;
  private boolean hostAffinityEnabled;
  private Config config;
  private String jcPodName;

  KubeClusterResourceManager(Config config, JobModelManager jobModelManager, ClusterResourceManager.Callback callback) {
    super(callback);
    this.config = config;
    this.client = KubeClientFactory.create();
    this.jobModelManager = jobModelManager;
    this.image = config.get(APP_IMAGE, DEFAULT_IMAGE);
    this.namespace = config.get(K8S_API_NAMESPACE, "default");
    this.appId = config.get(APP_ID, "001");
    this.appName = config.get(APP_NAME, "samza");
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();
    createOwnerReferences();
  }

  @Override
  public void start() {
    LOG.info("Kubernetes Cluster ResourceManager started, starting watcher");
    startPodWatcher();
    jobModelManager.start();
  }

  // Create the owner reference for the samza-job-coordinator pod
  private void createOwnerReferences() {
    this.jcPodName = System.getenv(COORDINATOR_POD_NAME);
    LOG.info("job coordinator pod name is: {}, namespace is: {}", jcPodName, namespace);
    Pod pod = client.pods().inNamespace(namespace).withName(jcPodName).get();
    ownerReference = new OwnerReferenceBuilder()
        .withName(pod.getMetadata().getName())
        .withApiVersion(pod.getApiVersion())
        .withUid(pod.getMetadata().getUid())
        .withKind(pod.getKind())
        .withController(true).build();
    podLabels.put("jc-pod-name", jcPodName);
  }

  public void startPodWatcher() {
    Watcher watcher = new Watcher<Pod>() {
      @Override
      public void eventReceived(Action action, Pod pod) {
        if (!pod.getMetadata().getLabels().get("jc-pod-name").equals(jcPodName)) {
          LOG.warn("This JC pod name is " + jcPodName + ", received pods for a different JC "
              + pod.getMetadata().getLabels().get("jc-pod-name"));
          return;
        }
        LOG.info("Pod watcher received action " + action + " for pod " + pod.getMetadata().getName());
        switch (action) {
          case ADDED:
            LOG.info("Pod " + pod.getMetadata().getName() + " is added.");
            break;
          case MODIFIED:
            LOG.info("Pod " + pod.getMetadata().getName() + " is modified.");
            if (isPodFailed(pod)) {
              deletePod(pod);
            }
            break;
          case ERROR:
            LOG.info("Pod " + pod.getMetadata().getName() + " received error.");
            if (isPodFailed(pod)) {
              deletePod(pod);
            }
            break;
          case DELETED:
            LOG.info("Pod " + pod.getMetadata().getName() + " is deleted.");
            createNewStreamProcessor(pod);
            break;
        }
      }
      @Override
      public void onClose(KubernetesClientException e) {
        LOG.error("Pod watcher closed", e);
      }
    };

    // TODO: SAMZA-2367: "podLabels" is empty. Need to add labels when creating Pod
    client.pods().withLabels(podLabels).watch(watcher);
  }

  private boolean isPodFailed(Pod pod) {
    return pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed");
  }

  private void deletePod(Pod pod) {
    boolean deleted = client.pods().delete(pod);
    if (deleted) {
      LOG.info("Deleted pod " + pod.getMetadata().getName());
    } else {
      LOG.info("Failed to deleted pod " + pod.getMetadata().getName());
    }
  }
  private void createNewStreamProcessor(Pod pod) {
    int memory = Integer.parseInt(pod.getSpec().getContainers().get(0).getResources().getRequests().get("memory").getAmount());
    int cpu = Integer.parseInt(pod.getSpec().getContainers().get(0).getResources().getRequests().get("cpu").getAmount());

    String containerId = KubeUtils.getSamzaContainerNameFromPodName(pod.getMetadata().getName());

    // Find out previously running container location
    // TODO: need to get the locality information. The logic below works for samza 1.3 or earlier version only.
    /* String lastSeenOn = jobModelManager.jobModel().getContainerToHostValue(containerId, SetContainerHostMapping.HOST_KEY);
       if (!hostAffinityEnabled || lastSeenOn == null) {
          lastSeenOn = ResourceRequestState.ANY_HOST;
       }  */
    String lastSeenOn = ResourceRequestState.ANY_HOST;
    SamzaResourceRequest request = new SamzaResourceRequest(cpu, memory, lastSeenOn, containerId);
    requestResources(request);
  }

  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    String samzaContainerId = resourceRequest.getProcessorId();
    LOG.info("Requesting resources on " + resourceRequest.getPreferredHost() + " for container " + samzaContainerId);
    CommandBuilder builder = getCommandBuilder(samzaContainerId);
    String command = buildCmd(builder);
    LOG.info("Container ID {} using command {}", samzaContainerId, command);
    Container container = KubeUtils.createContainer(STREAM_PROCESSOR_CONTAINER_NAME_PREFIX, image, resourceRequest, command);
    container.setEnv(getEnvs(builder));
    String podName = String.format(TASK_POD_NAME_FORMAT, STREAM_PROCESSOR_CONTAINER_NAME_PREFIX, appName, appId, samzaContainerId);

    PodBuilder podBuilder;
    if (config.getBoolean(AZURE_REMOTE_VOLUME_ENABLED)) {
      AzureFileVolumeSource azureFileVolumeSource = new AzureFileVolumeSource(false,
          config.get(AZURE_SECRET, DEFAULT_AZURE_SECRET), config.get(AZURE_FILESHARE, DEFAULT_AZURE_FILESHARE));
      Volume volume = new Volume();
      volume.setAzureFile(azureFileVolumeSource);
      volume.setName("azure");
      VolumeMount volumeMount = new VolumeMount();
      volumeMount.setMountPath(config.get(SAMZA_MOUNT_DIR, DEFAULT_SAMZA_MOUNT_DIR));
      volumeMount.setName("azure");
      volumeMount.setSubPath(podName);
      LOG.info("Set subpath to " + podName + ", mountpath to " + config.get(SAMZA_MOUNT_DIR, DEFAULT_SAMZA_MOUNT_DIR));
      container.setVolumeMounts(Collections.singletonList(volumeMount));
      podBuilder = new PodBuilder().editOrNewMetadata()
              .withName(podName)
              .withOwnerReferences(ownerReference)
              .addToLabels(podLabels).endMetadata()
              .editOrNewSpec()
              .withRestartPolicy(POD_RESTART_POLICY)
              .withVolumes(volume).addToContainers(container).endSpec();
    } else {
      podBuilder = new PodBuilder().editOrNewMetadata()
              .withName(podName)
              .withOwnerReferences(ownerReference)
              .addToLabels(podLabels).endMetadata()
              .editOrNewSpec()
              .withRestartPolicy(POD_RESTART_POLICY)
              .addToContainers(container).endSpec();
    }

    String preferredHost = resourceRequest.getPreferredHost();
    Pod pod;
    if (preferredHost.equals("ANY_HOST")) {
      // Create a pod with only one container in anywhere
      pod = podBuilder.build();
    } else {
      LOG.info("Making a preferred host request on " + preferredHost);
      pod = podBuilder.editOrNewSpec().editOrNewAffinity().editOrNewNodeAffinity()
          .addNewPreferredDuringSchedulingIgnoredDuringExecution().withNewPreference()
          .addNewMatchExpression()
          .withKey("kubernetes.io/hostname")
          .withOperator("Equal")
          .withValues(preferredHost).endMatchExpression()
          .endPreference().endPreferredDuringSchedulingIgnoredDuringExecution().endNodeAffinity().endAffinity().endSpec().build();
    }
    client.pods().inNamespace(namespace).create(pod);
    LOG.info("Created a pod " + pod.getMetadata().getName() + " on " + preferredHost);
  }

  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {
    // no need to implement
  }

  @Override
  public void releaseResources(SamzaResource resource) {
    // no need to implement
  }

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) {
    // no need to implement
  }

  @Override
  public void stopStreamProcessor(SamzaResource resource) {
    client.pods().withName(resource.getContainerId()).delete();
  }

  @Override
  public void stop(SamzaApplicationState.SamzaAppStatus status) {
    LOG.info("Kubernetes Cluster ResourceManager stopped");
    jobModelManager.stop();
  }

  private String buildCmd(CommandBuilder cmdBuilder) {
    cmdBuilder.setCommandPath(DEFAULT_DIRECTORY);
    return cmdBuilder.buildCommand();
  }

  private CommandBuilder getCommandBuilder(String containerId) {
    TaskConfig taskConfig = new TaskConfig(config);
    String cmdBuilderClassName = taskConfig.getCommandClass(ShellCommandBuilder.class.getName());
    CommandBuilder cmdBuilder = ReflectionUtil.getObj(cmdBuilderClassName, CommandBuilder.class);
    if (jobModelManager.server() == null) {
      LOG.error("HttpServer is null");
    }
    URL url = jobModelManager.server().getIpUrl();
    LOG.info("HttpServer URL: " + url);
    cmdBuilder.setConfig(config).setId(containerId).setUrl(url);

    return cmdBuilder;
  }

  // Construct the envs for the task container pod
  private List<EnvVar> getEnvs(CommandBuilder cmdBuilder) {
    // for logging
    StringBuilder sb = new StringBuilder();

    List<EnvVar> envList = new ArrayList<>();
    for (Map.Entry<String, String> entry : cmdBuilder.buildEnvironment().entrySet()) {
      envList.add(new EnvVar(entry.getKey(), entry.getValue(), null));
      sb.append(String.format("\n%s=%s", entry.getKey(), entry.getValue())); //logging
    }

    // TODO: SAMZA-2366: make the container ID as an execution environment and pass it to the container.
    //  Seems there is no such id (K8s container id)?
    // envList.add(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID(), container.getId().toString());
    // sb.append(String.format("\n%s=%s", ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID(), container.getId().toString()));

    envList.add(new EnvVar("LOGGED_STORE_BASE_DIR", config.get(SAMZA_MOUNT_DIR), null));
    envList.add(new EnvVar("EXECUTION_PLAN_DIR", config.get(SAMZA_MOUNT_DIR), null));
    envList.add(new EnvVar("SAMZA_LOG_DIR", config.get(SAMZA_MOUNT_DIR), null));

    LOG.info("Using environment variables: {}", cmdBuilder, sb.toString());

    return envList;
  }
}
