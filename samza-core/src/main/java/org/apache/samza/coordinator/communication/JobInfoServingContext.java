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

import java.util.Optional;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.samza.SamzaException;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;


/**
 * Provides a way to access and set the job model.
 * The {@link JobInfoProvider} part of this class is used by a {@link CoordinatorCommunication} implementation, and the
 * "set job model" part is called by the job coordinator when there is a new job model.
 */
public class JobInfoServingContext implements JobInfoProvider {
  private volatile byte[] serializedJobModel = null;

  @Override
  public Optional<byte[]> getSerializedJobModel() {
    return Optional.ofNullable(this.serializedJobModel);
  }

  public void setJobModel(JobModel jobModel) {
    try {
      this.serializedJobModel = SamzaObjectMapper.getObjectMapper().writeValueAsBytes(jobModel);
    } catch (JsonProcessingException e) {
      throw new SamzaException("Failed to serialize job model", e);
    }
  }
}
