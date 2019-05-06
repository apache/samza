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

import org.apache.samza.metadatastore.MetadataStore;
import org.junit.Test;
import org.mockito.Mockito;


import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestRunIdGenerator {

  private static final String FAKE_RUNID = "FAKE_RUNID";
  private RunIdGenerator runIdGenerator;
  private CoordinationUtils coordinationUtils;
  private DistributedLock distributedLock;
  private ClusterMembership membership;
  private MetadataStore metadataStore;

  @Test
  public void testSingleProcessorWriteRunId() throws Exception {
    // When there is a single processor registered with ClusterMembership
    // RunIdGenerator should write a new run id to the MetadataStore

    prepareRunIdGenerator(1);

    runIdGenerator.getRunId();

    verify(coordinationUtils, Mockito.times(1)).getClusterMembership();
    verify(coordinationUtils, Mockito.times(1)).getLock(anyString());
    verify(distributedLock, Mockito.times(1)).lock(anyObject());
    verify(distributedLock, Mockito.times(1)).unlock();
    verify(membership, Mockito.times(1)).registerProcessor();
    verify(membership, Mockito.times(1)).getNumberOfProcessors();
    verify(metadataStore, Mockito.times(1)).put(eq(CoordinationConstants.RUNID_STORE_KEY), any(byte[].class));
  }

  @Test
  public void testTwoProcessorsReadRunId() throws Exception {
    // When there are two processors registered with ClusterMembership
    // RunIdGenerator should read run id from the MetadataStore

    prepareRunIdGenerator(2);

    String runId = runIdGenerator.getRunId().get();

    assertEquals("Runid was not read from store", runId, FAKE_RUNID);

    verify(coordinationUtils, Mockito.times(1)).getClusterMembership();
    verify(coordinationUtils, Mockito.times(1)).getLock(anyString());
    verify(distributedLock, Mockito.times(1)).lock(anyObject());
    verify(distributedLock, Mockito.times(1)).unlock();
    verify(membership, Mockito.times(1)).registerProcessor();
    verify(membership, Mockito.times(1)).getNumberOfProcessors();
    verify(metadataStore, Mockito.times(1)).get(CoordinationConstants.RUNID_STORE_KEY);
  }

  private void prepareRunIdGenerator(int numberOfProcessors) throws Exception {

    coordinationUtils = mock(CoordinationUtils.class);

    distributedLock = mock(DistributedLock.class);
    when(distributedLock.lock(anyObject())).thenReturn(true);
    when(coordinationUtils.getLock(anyString())).thenReturn(distributedLock);

    membership = mock(ClusterMembership.class);
    when(membership.getNumberOfProcessors()).thenReturn(numberOfProcessors);
    when(coordinationUtils.getClusterMembership()).thenReturn(membership);

    metadataStore = mock(MetadataStore.class);
    when(metadataStore.get(CoordinationConstants.RUNID_STORE_KEY)).thenReturn(FAKE_RUNID.getBytes("UTF-8"));

    runIdGenerator = spy(new RunIdGenerator(coordinationUtils, metadataStore));
  }
}
