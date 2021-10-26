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
package org.apache.samza.coordinator.lifecycle;

/**
 * Interface for defining how to trigger a restart of a Samza job. This will be called when the Samza
 * framework determines that a job restart is needed. For example, if the partition count of an input stream
 * changes, then that means the job model needs to change, and restarting the job will update the job model.
 */
public interface JobRestartSignal {
  /**
   * Trigger a restart of the Samza job. This method should trigger the restart asynchronously, because the
   * caller of this method is part of the Samza job which is going to be restarted. It is not necessary that
   * the restart needs to actually happen immediately, as the job will continue to run until the restart
   * actually happens.
   */
  void restartJob();
}
