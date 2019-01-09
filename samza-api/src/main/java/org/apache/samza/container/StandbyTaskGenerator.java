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
package org.apache.samza.container;

import java.util.Map;
import org.apache.samza.job.model.ContainerModel;


/**
 *  Given a ContainerModel map (generated after applying the SSPGrouper and the TaskNameGrouper), a StandbyTaskGenerator generates
 *  StandbyTasks and adds them to the existing ContainerModels.
 *  It adds r StandbyTask for each TaskModel in the given ContainerModel map, where r is the replicationFactor.
 *
 *  Note that, the StandbyTaskGenerator implementations for YARN (BuddyContainerBasedStandbyTaskGenerator) and
 *  Standalone are different.
 */
public interface StandbyTaskGenerator {
  Map<String, ContainerModel> generateStandbyTasks(Map<String, ContainerModel> containerModels, int replicationFactor);
}
