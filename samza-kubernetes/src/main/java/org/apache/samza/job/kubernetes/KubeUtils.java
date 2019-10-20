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
import org.apache.samza.clustermanager.SamzaResourceRequest;

public class KubeUtils {

  public static String getSamzaContainerNameFromPodName(String podName) {
    // stream-processor-appName-appId-containerId
    String[] splits = podName.split("-");
    return splits[splits.length - 1];
  }

  public static Pod createPod(String name, OwnerReference ownerReference, String restartPolicy, Container container) {
    return new PodBuilder().editOrNewMetadata().withName(name).withOwnerReferences(ownerReference).endMetadata()
            .editOrNewSpec().withRestartPolicy(restartPolicy).addToContainers(container).endSpec().build();
  }

  public static Pod createPod(String name, String restartPolicy, Container container, String namespace) {
    return new PodBuilder().editOrNewMetadata().withNamespace(namespace).withName(name).endMetadata()
            .editOrNewSpec().withRestartPolicy(restartPolicy).addToContainers(container).endSpec().build();
  }

  // for Samza operator
  public static Container createContainer(String containerId, String image, SamzaResourceRequest resourceRequest,
      String cmd) {
    Quantity memQuantity = new QuantityBuilder(false)
        .withAmount(String.valueOf(resourceRequest.getMemoryMB())).withFormat("Mi").build();
    Quantity cpuQuantity = new QuantityBuilder(false)
        .withAmount(String.valueOf(resourceRequest.getNumCores())).build();
    return new ContainerBuilder().withName(containerId).withImage(image).withImagePullPolicy("Always").withCommand(cmd).editOrNewResources()
      .addToRequests("memory", memQuantity).addToRequests("cpu", cpuQuantity).endResources().build();
  }

  // TODO: will add util methods describing details about Pod status and container status. Refer to Spark'KubernetesUtils.
  //       Then we can use them in logs and exception messages.
}
