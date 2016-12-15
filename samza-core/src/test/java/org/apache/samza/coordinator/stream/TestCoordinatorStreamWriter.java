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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * This class is a unit test for the CoordinatorStreamWriter class.
 */
public class TestCoordinatorStreamWriter {

  private CoordinatorStreamWriter coordinatorStreamWriter;
  private MockCoordinatorStreamSystemFactory.MockSystemProducer systemProducer;

  @Test
  public void testCoordinatorStream() {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("systems.coordinatorStreamWriter.samza.factory", "org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory");
    configMap.put("job.name", "coordinator-stream-writer-test");
    configMap.put("job.coordinator.system", "coordinatorStreamWriter");
    Config config = new MapConfig(configMap);
    coordinatorStreamWriter = new CoordinatorStreamWriter(config);
    boolean exceptionHappened = false;

    try {

      //get coordinator system producer
      Field coordinatorProducerField = coordinatorStreamWriter.getClass().getDeclaredField("coordinatorStreamSystemProducer");
      coordinatorProducerField.setAccessible(true);
      assertNotNull(coordinatorProducerField.get(coordinatorStreamWriter));
      CoordinatorStreamSystemProducer coordinatorStreamSystemProducer = (CoordinatorStreamSystemProducer) coordinatorProducerField.get(coordinatorStreamWriter);

      //get mock system producer
      Field systemProducerField = coordinatorStreamSystemProducer.getClass().getDeclaredField("systemProducer");
      systemProducerField.setAccessible(true);
      systemProducer = (MockCoordinatorStreamSystemFactory.MockSystemProducer) systemProducerField.get(coordinatorStreamSystemProducer);

      testStart();
      testSendMessage();
      testStop();


    } catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      e.printStackTrace();
      exceptionHappened = true;
    }

    assertFalse(exceptionHappened);


  }


  public void testStart() throws NoSuchFieldException, IllegalAccessException {

    //checks before starting
    assertFalse(systemProducer.isStarted());

    //start and check if start has been done successfully
    coordinatorStreamWriter.start();
    assertTrue(systemProducer.isStarted());

  }

  public void testStop() throws NoSuchFieldException, IllegalAccessException {

    //checks before stopping
    assertTrue(systemProducer.isStarted());

    //stop and check if stop has been done correctly
    coordinatorStreamWriter.stop();
    assertTrue(systemProducer.isStopped());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testSendMessage() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

    //check a correct message
    assertEquals(0, systemProducer.getEnvelopes().size());
    coordinatorStreamWriter.sendMessage("set-config", "key0", "value0");
    assertEquals(1, systemProducer.getEnvelopes().size());

    //check invalid input is handled
    boolean exceptionHappened = false;
    try {
      coordinatorStreamWriter.sendMessage("invalid-type", "key-invalid", "value-invalid");
    } catch (IllegalArgumentException e) {
      exceptionHappened = true;
    }
    assertTrue(exceptionHappened);
    assertEquals(1, systemProducer.getEnvelopes().size());


    //check sendSetConfigMessage method works correctly
    Class[] sendArgs = {String.class, String.class};
    Method sendSetConfigMethod = coordinatorStreamWriter.getClass().getDeclaredMethod("sendSetConfigMessage", sendArgs);
    sendSetConfigMethod.setAccessible(true);
    sendSetConfigMethod.invoke(coordinatorStreamWriter, "key1", "value1");
    assertEquals(2, systemProducer.getEnvelopes().size());


    //check the messages are correct
    List<OutgoingMessageEnvelope> envelopes = systemProducer.getEnvelopes();
    OutgoingMessageEnvelope envelope0 = envelopes.get(0);
    OutgoingMessageEnvelope envelope1 = envelopes.get(1);
    TypeReference<Object[]> keyRef = new TypeReference<Object[]>() {
    };
    TypeReference<Map<String, Object>> msgRef = new TypeReference<Map<String, Object>>() {
    };
    assertEquals(2, envelopes.size());

    assertEquals("key0", deserialize((byte[]) envelope0.getKey(), keyRef)[CoordinatorStreamMessage.KEY_INDEX]);
    Map<String, String> values = (Map<String, String>) deserialize((byte[]) envelope0.getMessage(), msgRef).get("values");
    assertEquals("value0", values.get("value"));

    assertEquals("key1", deserialize((byte[]) envelope1.getKey(), keyRef)[CoordinatorStreamMessage.KEY_INDEX]);
    values = (Map<String, String>) deserialize((byte[]) envelope1.getMessage(), msgRef).get("values");
    assertEquals("value1", values.get("value"));
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

}

