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
package org.apache.samza.application.internal;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link StreamAppDescriptorImpl}
 */
public class TestStreamAppDescriptorImpl {
  private Config config = new MapConfig(new HashMap<String, String>() {
    {
      this.put("app.test.graph.class", TestStreamGraph.class.getName());
    }
  });

  @Test
  public void testConstructor() {
    StreamApplication mockApp = mock(StreamApplication.class);
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(mockApp, config);
    verify(mockApp, times(1)).describe(appDesc);
    assertEquals(config, appDesc.config);
    assertTrue(appDesc.graph instanceof TestStreamGraph);
  }

  @Test
  public void testGetInputStream() {
    Serde testSerde = new StringSerde();
    StreamApplication testApp = appDesc -> {
      appDesc.getInputStream("myinput1");
      appDesc.getInputStream("myinput2", testSerde);
    };
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(testApp, config);
    assertEquals(((TestStreamGraph) appDesc.graph).inputStreams,
        new ArrayList<String>() { { this.add("myinput1"); this.add("myinput2"); } });
    assertEquals(((TestStreamGraph) appDesc.graph).inputSerdes.get("myinput2"), testSerde);
  }

  @Test
  public void testGetOutputStream() {
    Serde testSerde = new StringSerde();
    StreamApplication testApp = appDesc -> {
      appDesc.getOutputStream("myoutput1");
      appDesc.getOutputStream("myoutput2", testSerde);
    };
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(testApp, config);
    assertEquals(((TestStreamGraph) appDesc.graph).outputStreams,
        new ArrayList<String>() { { this.add("myoutput1"); this.add("myoutput2"); } });
    assertEquals(((TestStreamGraph) appDesc.graph).outputSerdes.get("myoutput2"), testSerde);
  }

  @Test
  public void testGetTable() {
    TableDescriptor mockTd = mock(TableDescriptor.class);
    StreamApplication testApp = appDesc -> {
      appDesc.getTable(mockTd);
    };
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(testApp, config);
    assertEquals(((TestStreamGraph) appDesc.graph).tables,
        new ArrayList<TableDescriptor>() { { this.add(mockTd); } });
  }

  @Test
  public void testSetDefaultSerde() {
    Serde testSerde = new StringSerde();
    StreamApplication testApp = appDesc -> {
      appDesc.setDefaultSerde(testSerde);
    };
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(testApp, config);
    assertEquals(((TestStreamGraph) appDesc.graph).defaultSerde, testSerde);
  }

  @Test
  public void testContextManager() {
    ContextManager cntxMan = mock(ContextManager.class);
    StreamApplication testApp = appDesc -> {
      appDesc.withContextManager(cntxMan);
    };
    StreamAppDescriptorImpl appSpec = new StreamAppDescriptorImpl(testApp, config);
    assertEquals(appSpec.getContextManager(), cntxMan);
  }

  @Test
  public void testProcessorLifecycleListenerFactory() {
    ProcessorLifecycleListenerFactory mockFactory = mock(ProcessorLifecycleListenerFactory.class);
    StreamApplication testApp = appSpec -> {
      appSpec.withProcessorLifecycleListenerFactory(mockFactory);
    };
    StreamAppDescriptorImpl appDesc = new StreamAppDescriptorImpl(testApp, config);
    assertEquals(appDesc.getProcessorLifecycleListenerFactory(), mockFactory);
  }
}
