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

import org.apache.samza.application.ProcessorLifecycleListener;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Base class for unit tests for {@link AppSpecImpl}
 */
public class AppSpecImplTestBase {
  @Test
  public void testContextManager() {
    ContextManager cntxMan = mock(ContextManager.class);
    Config mockConf = mock(Config.class);
    StreamApplication testApp = appSpec -> {
      appSpec.withContextManager(cntxMan);
    };
    StreamAppSpecImpl appSpec = new StreamAppSpecImpl(testApp, mockConf);
    assertEquals(appSpec.getContextManager(), cntxMan);
  }

  @Test
  public void testProcessorLifecycleListener() {
    ProcessorLifecycleListener listener = mock(ProcessorLifecycleListener.class);
    Config mockConf = mock(Config.class);
    StreamApplication testApp = appSpec -> {
      appSpec.withProcessorLifecycleListener(listener);
    };
    StreamAppSpecImpl appSpec = new StreamAppSpecImpl(testApp, mockConf);
    assertEquals(appSpec.getProcessorLifecycleListner(), listener);
  }
}
