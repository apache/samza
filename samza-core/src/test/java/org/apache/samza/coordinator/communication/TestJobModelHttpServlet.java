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

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestJobModelHttpServlet {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Mock
  private JobInfoProvider jobInfoProvider;
  @Mock
  private HttpServletResponse httpServletResponse;
  @Mock
  private ServletOutputStream servletOutputStream;

  private JobModelHttpServlet.Metrics metrics;
  private JobModelHttpServlet jobModelHttpServlet;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.metrics = new JobModelHttpServlet.Metrics(new MetricsRegistryMap());
    this.jobModelHttpServlet = new JobModelHttpServlet(this.jobInfoProvider, this.metrics);
  }

  @Test
  public void testDoGet() throws IOException {
    JobModel jobModel = jobModel();
    when(this.jobInfoProvider.getSerializedJobModel()).thenReturn(
        Optional.of(OBJECT_MAPPER.writeValueAsBytes(jobModel)));
    when(this.httpServletResponse.getOutputStream()).thenReturn(this.servletOutputStream);

    this.jobModelHttpServlet.doGet(mock(HttpServletRequest.class), this.httpServletResponse);

    verify(this.httpServletResponse).setContentType("application/json;charset=UTF-8");
    verify(this.httpServletResponse).setStatus(HttpServletResponse.SC_OK);
    verify(this.servletOutputStream).write(aryEq(OBJECT_MAPPER.writeValueAsBytes(jobModel)));
    assertEquals(1, this.metrics.incomingRequests.getCount());
    assertEquals(1, this.metrics.successfulResponses.getCount());
    assertEquals(0, this.metrics.failedResponses.getCount());
  }

  @Test
  public void testDoGetMissingJobModel() throws IOException {
    when(this.jobInfoProvider.getSerializedJobModel()).thenReturn(Optional.empty());

    this.jobModelHttpServlet.doGet(mock(HttpServletRequest.class), this.httpServletResponse);

    verify(this.httpServletResponse).setStatus(HttpServletResponse.SC_NOT_FOUND);
    verify(this.httpServletResponse, never()).setContentType(any());
    verify(this.httpServletResponse, never()).getOutputStream();
    assertEquals(1, this.metrics.incomingRequests.getCount());
    assertEquals(0, this.metrics.successfulResponses.getCount());
    assertEquals(1, this.metrics.failedResponses.getCount());
  }

  @Test
  public void testDoGetFailureToWriteResponse() throws IOException {
    when(this.jobInfoProvider.getSerializedJobModel()).thenReturn(
        Optional.of(OBJECT_MAPPER.writeValueAsBytes(jobModel())));
    when(this.httpServletResponse.getOutputStream()).thenReturn(this.servletOutputStream);
    doThrow(new IOException("failure to write to output stream")).when(this.servletOutputStream).write(any());

    this.jobModelHttpServlet.doGet(mock(HttpServletRequest.class), this.httpServletResponse);

    verify(this.httpServletResponse).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    verify(this.httpServletResponse, never()).setContentType(any());
    assertEquals(1, this.metrics.incomingRequests.getCount());
    assertEquals(0, this.metrics.successfulResponses.getCount());
    assertEquals(1, this.metrics.failedResponses.getCount());
  }

  private static JobModel jobModel() {
    Config config = new MapConfig(ImmutableMap.of("samza.user.config", "config-value"));
    Map<String, ContainerModel> containerModelMap = ImmutableMap.of("0", new ContainerModel("0",
        ImmutableMap.of(new TaskName("Partition 0"), new TaskModel(new TaskName("Partition 0"),
            ImmutableSet.of(new SystemStreamPartition("system", "stream", new Partition(0))), new Partition(0)))));
    return new JobModel(config, containerModelMap);
  }
}