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

# Configurations
- kube.app.image: the image name for the samza app
- kube.app.namespace: the namespace where the samza app runs
- kube.app.pod.mnt.path: the path where the remote volume is mounted into the pod, for both the job coordinator pod and stream processor pod
  the volume can be used for storing logs and local states.
- cluster-manager.container.memory.mb: the memory size for the samza stream processor
- cluster-manager.container.cpu.cores: the cpu cores for the samza stream processor
