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

package org.apache.samza.coordinator.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.Delete;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

public class TestCoordinatorStreamSystemProducer {
  @Test
  public void testCoordinatorStreamSystemProducer() {
    String source = "source";
    SystemStream systemStream = new SystemStream("system", "stream");
    MockCoordinatorSystemProducer systemProducer = new MockCoordinatorSystemProducer(source);
    MockSystemAdmin systemAdmin = new MockSystemAdmin();
    CoordinatorStreamSystemProducer producer = new CoordinatorStreamSystemProducer(systemStream, systemProducer, systemAdmin);
    SetConfig setConfig1 = new SetConfig(source, "job.name", "my-job-name");
    SetConfig setConfig2 = new SetConfig(source, "job.id", "1234");
    Delete delete = new Delete(source, "job.name", SetConfig.TYPE);
    assertFalse(systemProducer.isRegistered());
    producer.register(source);
    assertTrue(systemProducer.isRegistered());
    assertFalse(systemProducer.isStarted());
    producer.start();
    assertTrue(systemProducer.isStarted());
    producer.send(setConfig1);
    producer.send(setConfig2);
    producer.send(delete);
    assertFalse(systemProducer.isStopped());
    producer.stop();
    assertTrue(systemProducer.isStopped());
    List<OutgoingMessageEnvelope> envelopes = systemProducer.getEnvelopes();
    OutgoingMessageEnvelope envelope0 = envelopes.get(0);
    OutgoingMessageEnvelope envelope1 = envelopes.get(1);
    OutgoingMessageEnvelope envelope2 = envelopes.get(2);
    TypeReference<Object[]> keyRef = new TypeReference<Object[]>() {
    };
    TypeReference<Map<String, Object>> msgRef = new TypeReference<Map<String, Object>>() {
    };
    assertEquals(3, envelopes.size());
    assertEquals(new CoordinatorStreamMessage(setConfig1), new CoordinatorStreamMessage(deserialize((byte[]) envelope0.getKey(), keyRef), deserialize((byte[]) envelope0.getMessage(), msgRef)));
    assertEquals(new CoordinatorStreamMessage(setConfig2), new CoordinatorStreamMessage(deserialize((byte[]) envelope1.getKey(), keyRef), deserialize((byte[]) envelope1.getMessage(), msgRef)));
    assertEquals(new CoordinatorStreamMessage(delete), new CoordinatorStreamMessage(deserialize((byte[]) envelope2.getKey(), keyRef), deserialize((byte[]) envelope2.getMessage(), msgRef)));
  }

  private <T> T deserialize(byte[] bytes, TypeReference<T> reference) {
    try {
      if (bytes != null) {
        String valueStr = new String((byte[]) bytes, "UTF-8");
        return SamzaObjectMapper.getObjectMapper().readValue(valueStr, reference);
      }
    } catch (Exception e) {
      throw new SamzaException(e);
    }

    return null;
  }

  private static class MockSystemAdmin extends SinglePartitionWithoutOffsetsSystemAdmin implements SystemAdmin {
    public void createCoordinatorStream(String streamName) {
      // Do nothing.
    }
  }

  private static class MockCoordinatorSystemProducer extends MockCoordinatorStreamSystemFactory.MockSystemProducer {

    public MockCoordinatorSystemProducer(String expectedSource) {
      super(expectedSource);
    }

    @Override
    public void register(String source) {
      assertEquals(super.getExpectedSource(), source);
      super.register(source);
    }

    @Override
    public void flush(String source) {
      assertEquals(super.getExpectedSource(), source);
      super.flush(source);
    }
  }
}
