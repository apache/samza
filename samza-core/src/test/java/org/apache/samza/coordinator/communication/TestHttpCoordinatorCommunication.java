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

import org.apache.samza.coordinator.server.HttpServer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


public class TestHttpCoordinatorCommunication {
  @Mock
  private HttpServer httpServer;

  private HttpCoordinatorCommunication httpCoordinatorCommunication;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.httpCoordinatorCommunication = new HttpCoordinatorCommunication(this.httpServer);
  }

  @Test
  public void testStartStop() {
    this.httpCoordinatorCommunication.start();
    verify(this.httpServer).start();
    this.httpCoordinatorCommunication.start();
    // consecutive stops should still only result in one start
    verify(this.httpServer).start();

    this.httpCoordinatorCommunication.stop();
    verify(this.httpServer).stop();
    this.httpCoordinatorCommunication.stop();
    // consecutive stops should still only result in one stop
    verify(this.httpServer).stop();
  }

  @Test
  public void testStopOnly() {
    this.httpCoordinatorCommunication.stop();
    verify(this.httpServer, never()).stop();
  }
}