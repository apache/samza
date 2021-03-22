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
package org.apache.samza.coordinator.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Optional;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;


/**
 * A servlet for locality information of a job. The servlet is hosted alongside of the {@link JobServlet} which hosts
 * job model and configuration. Historically, locality information was part of job model but we extracted the locality
 * as job model is static within the lifecycle of an application attempt while locality changes in the event of container
 * movements. The locality information is served under
 * {@link org.apache.samza.coordinator.JobModelManager#server()}/locality. The server and the port information are
 * dynamic and is determined at the start AM. YARN dashboard or job-coordinator logs contains the server
 * and the port information.
 *
 * This separation enables us to achieve performance benefits by caching job model when it is served by the AM as it
 * can incur significant penalty in the job start time for jobs with large number of containers.
 */
public class LocalityServlet extends HttpServlet {
  private static final String PROCESSOR_ID_PARAM = "processorId";
  private final ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();
  private final LocalityManager localityManager;

  public LocalityServlet(LocalityManager localityManager) {
    this.localityManager = localityManager;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    LocalityModel localityModel = localityManager.readLocality();

    if (request.getParameterMap().size() == 1) {
      String processorId = request.getParameter(PROCESSOR_ID_PARAM);
      ProcessorLocality processorLocality = Optional.ofNullable(localityModel.getProcessorLocality(processorId))
          .orElse(new ProcessorLocality(processorId, ""));
      mapper.writeValue(response.getWriter(), processorLocality);
    } else {
      mapper.writeValue(response.getWriter(), localityModel);
    }
  }
}
