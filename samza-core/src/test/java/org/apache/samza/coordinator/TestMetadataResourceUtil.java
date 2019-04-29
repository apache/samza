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

package org.apache.samza.coordinator;

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.job.model.JobModel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TestMetadataResourceUtil {
  private CheckpointManager checkpointManager;
  private JobModel jobModel;

  @Before
  public void setUp() {
    checkpointManager = Mockito.mock(CheckpointManager.class);
    jobModel = Mockito.mock(JobModel.class);
  }

  @Test
  public void testLoad() {
    MetadataResourceUtil metadataResourceUtil = Mockito.spy(new MetadataResourceUtil(checkpointManager, jobModel));
    Mockito.doNothing().when(metadataResourceUtil).createChangelogStreams();

    metadataResourceUtil.createResources();
    Mockito.verify(checkpointManager).createResources();
    Mockito.verify(metadataResourceUtil).createChangelogStreams();
  }
}