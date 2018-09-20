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

package org.apache.samza.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang.NotImplementedException;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Console logging System factory that just writes the messages to the console output.
 * This system factory is useful when the user wants to print the output of the stream processing to console.
 */
public class ConsoleLoggingSystemFactory implements SystemFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConsoleLoggingSystemFactory.class);

  public static AtomicInteger messageCounter = new AtomicInteger();
  private static ObjectMapper mapper = new ObjectMapper();

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
          new String((byte[]) envelope.getMessage()));
      LOG.info(msg);

      System.out.println(String.format("Message %d :", messageCounter.incrementAndGet()));
      if (envelope.getKey() != null) {
        System.out.println(String.format("Key:%s Value:%s", envelope.getKey(), getFormattedValue(envelope)));
      } else {
        System.out.println(getFormattedValue(envelope));
      }
    }

    private String getFormattedValue(OutgoingMessageEnvelope envelope) {
      String value = new String((byte[]) envelope.getMessage());
      String formattedValue;

      try {
        Object json = mapper.readValue(value, Object.class);
        formattedValue = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
      } catch (IOException e) {
        formattedValue = value;
        LOG.error("Error while formatting json", e);
      }

      return formattedValue;
    }

    @Override
    public void flush(String source) {
    }
  }

  public static class SimpleSystemAdmin implements SystemAdmin {

    public SimpleSystemAdmin(Config config) {
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
      return offsets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, null));
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
      return streamNames.stream()
          .collect(Collectors.toMap(Function.identity(), streamName -> new SystemStreamMetadata(streamName,
              Collections.singletonMap(new Partition(0),
                  new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, null)))));
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
      if (offset1 == null) {
        return offset2 == null ? 0 : -1;
      } else if (offset2 == null) {
        return 1;
      }
      return offset1.compareTo(offset2);
    }
  }
}