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

package org.apache.samza.container;

import java.io.IOException;
import java.net.URL;
import junit.framework.Assert;
import org.junit.Test;

public class TestContainerHeartbeatClient {
  private MockContainerHeartbeatClient client =
      new MockContainerHeartbeatClient("http://fake-endpoint/", "FAKE_CONTAINER_ID");

  @Test
  public void testClientResponseForHeartbeatAlive()
      throws IOException {
    client.setHttpOutput("{\"alive\": true}");
    ContainerHeartbeatResponse response = client.requestHeartbeat();
    Assert.assertTrue(response.isAlive());
  }

  @Test
  public void testClientResponseForHeartbeatDead()
      throws IOException {
    client.setHttpOutput("{\"alive\": false}");
    ContainerHeartbeatResponse response = client.requestHeartbeat();
    Assert.assertFalse(response.isAlive());
  }

  @Test
  public void testClientResponseOnBadRequest()
      throws IOException {
    client.shouldThrowException(true);
    ContainerHeartbeatResponse response = client.requestHeartbeat();
    Assert.assertFalse(response.isAlive());
  }

  private class MockContainerHeartbeatClient extends ContainerHeartbeatClient {
    private String httpOutput;
    private boolean throwException = false;

    public void shouldThrowException(boolean throwException) {
      this.throwException = throwException;
    }

    public void setHttpOutput(String httpOutput) {
      this.httpOutput = httpOutput;
    }

    MockContainerHeartbeatClient(String coordinatorUrl, String executionEnvContainerId) {
      super(coordinatorUrl, executionEnvContainerId);
    }

    @Override
    String httpGet(URL url)
        throws IOException {
      if (!throwException) {
        return httpOutput;
      } else {
        throw new IOException("Exception thrown");
      }
    }
  }
}
