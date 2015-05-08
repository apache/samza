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

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;

/**
 * A serializer for LoggingEvent. It provides two methods. {@link #toBytes(LoggingEvent object)} serializes
 * the {@link org.apache.log4j.spi.LoggingEvent}'s messages into bytes. {@link #fromBytes(byte[] bytes)} will creates a new
 * LoggingEvent based on the messages, which is deserialized from the bytes.
 */
public class LoggingEventStringSerde implements Serde<LoggingEvent> {
  private static final String ENCODING = "UTF-8";
  private final Logger logger = Logger.getLogger(LoggingEventStringSerde.class);

  @Override
  public byte[] toBytes(LoggingEvent object) {
    byte[] bytes = null;
    if (object != null) {
      try {
        bytes = object.getMessage().toString().getBytes(ENCODING);
      } catch (UnsupportedEncodingException e) {
        throw new SamzaException("can not be encoded to byte[]", e);
      }
    }
    return bytes;
  }

  /**
   * Convert bytes to a {@link org.apache.log4j.spi.LoggingEvent}. This LoggingEvent uses logging
   * information of the {@link LoggingEventStringSerde}, which includes log
   * name, log category and log level.
   *
   * @param bytes bytes for decoding
   * @return LoggingEvent a new LoggingEvent
   */
  @Override
  public LoggingEvent fromBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String log;
    try {
      log = new String(bytes, ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new SamzaException("can not decode to String", e);
    }
    return new LoggingEvent(logger.getName(), logger, logger.getLevel(), log, null);
  }
}