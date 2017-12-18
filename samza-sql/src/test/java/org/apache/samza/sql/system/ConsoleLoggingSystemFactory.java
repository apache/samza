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

package org.apache.samza.sql.system;

import org.apache.commons.lang.NotImplementedException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Console logging System factory that just writes the messages to the console output.
 * This system factory is useful when the user wants to print the output of the stream processing to console.
 */
public class ConsoleLoggingSystemFactory implements SystemFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConsoleLoggingSystemFactory.class);

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    throw new NotImplementedException();
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new LoggingSystemProducer();
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SimpleSystemAdmin(config);
  }

  private class LoggingSystemProducer implements SystemProducer {
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(String source) {
      LOG.info("Registering source" + source);
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
      String msg = String.format("OutputStream:%s Key:%s Value:%s", envelope.getSystemStream(), envelope.getKey(),
          envelope.getMessage());
      LOG.info(msg);
      System.out.println(msg);
    }

    @Override
    public void flush(String source) {
    }
  }
}
