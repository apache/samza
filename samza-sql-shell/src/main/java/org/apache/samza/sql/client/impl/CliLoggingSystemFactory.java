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

package org.apache.samza.sql.client.impl;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CliLoggingSystemFactory implements SystemFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CliLoggingSystemFactory.class);
    private static AtomicInteger messageCounter = new AtomicInteger(0);

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        return new CliLoggingSystemFactory.LoggingSystemProducer();
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new CliLoggingSystemFactory.SimpleSystemAdmin(config);
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
            LOG.info(String.format(String.format("Message %d :", messageCounter.incrementAndGet())));
            String msg = String.format("OutputStream:%s Key:%s Value:%s", envelope.getSystemStream(), envelope.getKey(),
                    new String((byte[]) envelope.getMessage()));
            LOG.info(msg);

            SamzaExecutor.saveOutputMessage(envelope);
        }

        @Override
        public void flush(String source) {
        }
    }


    private static class SimpleSystemAdmin implements SystemAdmin {

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