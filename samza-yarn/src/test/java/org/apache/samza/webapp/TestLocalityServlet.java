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
package org.apache.samza.webapp;

import com.google.common.collect.ImmutableMap;
import java.net.URL;
import java.util.Collections;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.coordinator.server.LocalityServlet;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.HttpUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * A test class for {@link LocalityServlet}. It validates the servlet directly and Serde Mix-In of {@link ProcessorLocality}
 * indirectly.
 */
public class TestLocalityServlet {
  private static final String PROCESSOR_ID1 = "1";
  private static final String PROCESSOR_ID2 = "2";
  private static final String HOST1 = "host1";
  private static final String HOST2 = "host2";
  private static final String JMX_URL = "jmx";
  private static final String TUNNELING_URL = "tunneling";

  private static final ProcessorLocality PROCESSOR_1_LOCALITY =
      new ProcessorLocality(PROCESSOR_ID1, HOST1, JMX_URL, TUNNELING_URL);
  private static final ProcessorLocality PROCESSOR_2_LOCALITY =
      new ProcessorLocality("2", HOST2, JMX_URL, TUNNELING_URL);

  private final ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();
  private HttpServer webApp;
  private LocalityManager localityManager;



  @Before
  public void setup()
      throws Exception {
    localityManager = mock(LocalityManager.class);
    when(localityManager.readLocality())
        .thenReturn(new LocalityModel(ImmutableMap.of(PROCESSOR_ID1, PROCESSOR_1_LOCALITY, PROCESSOR_ID2, PROCESSOR_2_LOCALITY)));
    webApp = new HttpServer("/", 0, "", new ServletHolder(new DefaultServlet()));
    webApp.addServlet("/locality", new LocalityServlet(localityManager));
    webApp.start();
  }

  @After
  public void cleanup()
      throws Exception {
    webApp.stop();
  }

  @Test
  public void testReadContainerLocality() throws Exception {
    URL url = new URL(webApp.getUrl().toString() + "locality");

    String response = HttpUtil.read(url, 1000, new ExponentialSleepStrategy());
    LocalityModel locality = mapper.readValue(response, LocalityModel.class);

    assertEquals("Expected locality for two containers", 2, locality.getProcessorLocalities().size());
    assertEquals("Mismatch in locality for processor " + PROCESSOR_ID1,
        locality.getProcessorLocality(PROCESSOR_ID1), PROCESSOR_1_LOCALITY);
    assertEquals("Mismatch in locality for processor " + PROCESSOR_ID2,
        locality.getProcessorLocality(PROCESSOR_ID2), PROCESSOR_2_LOCALITY);
  }

  @Test
  public void testReadContainerLocalityWithNoLocality() throws Exception {
    final LocalityModel expectedLocality = new LocalityModel(Collections.emptyMap());
    URL url = new URL(webApp.getUrl().toString() + "locality");
    when(localityManager.readLocality()).thenReturn(new LocalityModel(ImmutableMap.of()));

    String response = HttpUtil.read(url, 1000, new ExponentialSleepStrategy());
    LocalityModel locality = mapper.readValue(response, LocalityModel.class);

    assertEquals("Expected empty response but got " + locality, locality, expectedLocality);
  }

  @Test
  public void testReadProcessorLocality() throws Exception {
    URL url = new URL(webApp.getUrl().toString() + "locality?processorId=" + PROCESSOR_ID1);
    String response = HttpUtil.read(url, 1000, new ExponentialSleepStrategy());

    assertEquals("Mismatch in the locality for processor " + PROCESSOR_ID1,
        mapper.readValue(response, ProcessorLocality.class), PROCESSOR_1_LOCALITY);
  }

  @Test
  public void testReadProcessorLocalityWithNoLocality() throws Exception {
    final ProcessorLocality expectedProcessorLocality = new ProcessorLocality(PROCESSOR_ID2, "");
    URL url = new URL(webApp.getUrl().toString() + "locality?processorId=" + PROCESSOR_ID2);
    when(localityManager.readLocality()).thenReturn(new LocalityModel(ImmutableMap.of()));

    String response = HttpUtil.read(url, 1000, new ExponentialSleepStrategy());
    ProcessorLocality processorLocality = mapper.readValue(response, ProcessorLocality.class);

    assertEquals("Expected empty response for processor locality " + PROCESSOR_ID2 + " but got " + processorLocality,
        processorLocality, expectedProcessorLocality);
  }
}
