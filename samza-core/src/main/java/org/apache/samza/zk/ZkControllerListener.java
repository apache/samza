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

import java.util.List;

/**
 * Interface to listen for notifications from the {@link ZkController}
 */
public interface ZkControllerListener {
  /**
   * ZkController observes the ZkTree for changes to group membership of processors and notifies the listener
   *
   * @param processorIds List of current znodes that are in the processing group
   */
  void onProcessorChange(List<String> processorIds);

  void onNewJobModelAvailable(String version); // start job model update (stop current work)
  void onNewJobModelConfirmed(String version); // start new work according to the new model
}
