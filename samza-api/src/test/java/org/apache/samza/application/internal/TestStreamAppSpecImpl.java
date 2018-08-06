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
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Unit test for {@link StreamAppSpecImpl}
 */
public class TestStreamAppSpecImpl extends AppSpecImplTestBase {
  @Test
  public void testConstructor() {
    StreamApplication mockApp = mock(StreamApplication.class);
    Config mockConf = mock(Config.class);
    StreamAppSpecImpl appSpec = new StreamAppSpecImpl(mockApp, mockConf);
    verify(mockApp, times(1)).describe(appSpec);
    assertEquals(mockConf, appSpec.config);
    assertTrue(appSpec.graph instanceof StreamGraphSpec);
  }

  @Test
  public void testGetInputStream() {
    Serde testSerde = new StringSerde();
    Config mockConf = mock(Config.class);
    StreamApplication testApp = appSpec -> {
      appSpec.getInputStream("myinput1");
      appSpec.getInputStream("myinput2", testSerde);
    };
    StreamAppSpecImpl appSpec = new StreamAppSpecImpl(testApp, mockConf);
    assertEquals(((StreamGraphSpec) appSpec.graph).inputStreams,
        new ArrayList<String>() { { this.add("myinput1"); this.add("myinput2"); } });
    assertEquals(((StreamGraphSpec) appSpec.graph).inputSerdes.get("myinput2"), testSerde);
  }

  @Test
  public void testGetOutputStream() {
    Serde testSerde = new StringSerde();
    Config mockConf = mock(Config.class);
    StreamApplication testApp = appSpec -> {
      appSpec.getOutputStream("myoutput1");
      appSpec.getOutputStream("myoutput2", testSerde);
    };
    StreamAppSpecImpl appSpec = new StreamAppSpecImpl(testApp, mockConf);
    assertEquals(((StreamGraphSpec) appSpec.graph).outputStreams,
        new ArrayList<String>() { { this.add("myoutput1"); this.add("myoutput2"); } });
    assertEquals(((StreamGraphSpec) appSpec.graph).outputSerdes.get("myoutput2"), testSerde);
  }

  @Test
  public void testGetTable() {
    TableDescriptor mockTd = mock(TableDescriptor.class);
    Config mockConf = mock(Config.class);
    StreamApplication testApp = appSpec -> {
      appSpec.getTable(mockTd);
    };
    StreamAppSpecImpl appSpec = new StreamAppSpecImpl(testApp, mockConf);
    assertEquals(((StreamGraphSpec) appSpec.graph).tables,
        new ArrayList<TableDescriptor>() { { this.add(mockTd); } });
  }

  @Test
  public void testSetDefaultSerde() {
    Serde testSerde = new StringSerde();
    Config mockConf = mock(Config.class);
    StreamApplication testApp = appSpec -> {
      appSpec.setDefaultSerde(testSerde);
    };
    StreamAppSpecImpl appSpec = new StreamAppSpecImpl(testApp, mockConf);
    assertEquals(((StreamGraphSpec) appSpec.graph).defaultSerde, testSerde);
  }
}
