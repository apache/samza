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
package org.apache.samza.rest.proxy.task;

import java.io.IOException;
import java.util.List;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.proxy.job.JobProxy;
import org.apache.samza.rest.resources.JobsResourceConfig;


/**
 * TaskProxy is the primary abstraction that will be used by Rest API's to interact with tasks.
 */
public interface TaskProxy {

  /**
   * @param jobInstance the job instance to get the tasks for.
   * @return a list of all the {@link Task} tasks that belongs to the {@link JobInstance}.
   *         Each task will have a preferred host and stream partitions assigned to it by
   *         the samza job coordinator.
   * @throws IOException if there was a problem executing the command to get the tasks.
   * @throws InterruptedException if the thread was interrupted while waiting for the result.
   */
  List<Task> getTasks(JobInstance jobInstance)
      throws IOException, InterruptedException;
}
