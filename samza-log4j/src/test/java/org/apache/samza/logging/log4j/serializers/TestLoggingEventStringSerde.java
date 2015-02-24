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

package org.apache.samza.logging.log4j.serializers;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

public class TestLoggingEventStringSerde {

  @Test
  public void test() {
    String testLog = "testing";
    Logger logger = Logger.getLogger(TestLoggingEventStringSerde.class);
    LoggingEvent log = new LoggingEvent(logger.getName(), logger, logger.getLevel(), testLog, null);
    LoggingEventStringSerde loggingEventStringSerde = new LoggingEventStringSerde();

    assertNull(loggingEventStringSerde.fromBytes(null));
    assertNull(loggingEventStringSerde.toBytes(null));

    assertArrayEquals(testLog.getBytes(), loggingEventStringSerde.toBytes(log));
    // only the log messages are guaranteed to be equivalent 
    assertEquals(log.getMessage().toString(), loggingEventStringSerde.fromBytes(testLog.getBytes()).getMessage().toString());
  }
}
