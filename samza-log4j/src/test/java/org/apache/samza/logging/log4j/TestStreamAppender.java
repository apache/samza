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

package org.apache.samza.logging.log4j;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

public class TestStreamAppender {

  static Logger log = Logger.getLogger(TestStreamAppender.class);

  @Test
  public void testSystemProducerAppender() {
    System.setProperty("samza.container.name", "samza-container-1");

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender();
    PatternLayout layout = new PatternLayout();
    layout.setConversionPattern("%m");
    systemProducerAppender.setLayout(layout);
    systemProducerAppender.activateOptions();
    log.addAppender(systemProducerAppender);
    log.info("testing");
    log.info("testing2");

    systemProducerAppender.flushSystemProducer();

    assertEquals(2, MockSystemProducer.messagesReceived.size());
    assertEquals("testing", new String((byte[])MockSystemProducer.messagesReceived.get(0)));
    assertEquals("testing2", new String((byte[])MockSystemProducer.messagesReceived.get(1)));
  }

  /**
   * a mock class which overrides the getConfig method in SystemProducerAppener
   * for testing purpose. Because the environment variable where the config
   * stays is difficult to test.
   */
  class MockSystemProducerAppender extends StreamAppender {
    @Override
    protected Config getConfig() {
      Map<String, String> map = new HashMap<String, String>();
      map.put("job.name", "log4jTest");
      map.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
      return new MapConfig(map);
    }
  }
}