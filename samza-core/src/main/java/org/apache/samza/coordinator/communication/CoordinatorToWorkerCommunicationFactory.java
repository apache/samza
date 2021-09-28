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
package org.apache.samza.coordinator.communication;

/**
 * Factory for generating components for coordinator-to-work communication.
 *
 * SAMZA-2673: There should be a "WorkerCommunication" interface which pairs with the {@link CoordinatorCommunication},
 * but that abstraction layer is not implemented yet. One communication path to be handled by WorkerCommunication is the
 * query to get the job model.
 */
public interface CoordinatorToWorkerCommunicationFactory {
  /**
   * Build a {@link CoordinatorCommunication} to handle communication with the workers for the job.
   */
  CoordinatorCommunication coordinatorCommunication(CoordinatorCommunicationContext context);

  // SAMZA-2673: an opportunity for improvement here is to add the WorkerCommunication layer
}
