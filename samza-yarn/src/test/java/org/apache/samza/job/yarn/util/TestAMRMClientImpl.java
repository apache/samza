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
package org.apache.samza.job.yarn.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TestAMRMClientImpl extends AMRMClientImpl<ContainerRequest> {

  private final AllocateResponse response;
  public List<ContainerRequest> requests = new ArrayList<ContainerRequest>();

  public TestAMRMClientImpl(AllocateResponse response) {
    this.response = response;
  }

  public Set<ContainerId> getRelease() {
    return release;
  }

  public void resetRelease() {
    release.clear();
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(String appHostName,
                                                                     int appHostPort,
                                                                     String appTrackingUrl) {
    return null;
  }

  @Override
  public AllocateResponse allocate(float progressIndicator) throws YarnException, IOException {
    response.getAMCommand();
    return response;
  }

  @Override
  public void unregisterApplicationMaster(FinalApplicationStatus appStatus, String appMessage, String appTrackingUrl)
      throws YarnException, IOException { }

  @Override
  public synchronized void addContainerRequest(ContainerRequest req) {
    requests.add(req);
  }

  @Override
  public synchronized void removeContainerRequest(ContainerRequest req) {
  }

  @Override
  public synchronized int getClusterNodeCount() {
    return 1;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception { }

  @Override
  protected void serviceStart() throws Exception {  }

  @Override
  protected void serviceStop() throws Exception { }
}
