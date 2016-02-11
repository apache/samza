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

package org.apache.samza.system.kinesis;

import org.apache.samza.system.kinesis.consumer.KinesisSystemConsumer;
import org.apache.samza.system.kinesis.producer.KinesisSystemProducer;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

import java.util.HashMap;

/**
 * Kinesis System factory
 */
public class KinesisSystemFactory implements SystemFactory {

    private static final HashMap<String, KinesisSystemConsumer> consumers =
            new HashMap<String, KinesisSystemConsumer>();

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new KinesisSystemAdmin(systemName, config);
    }

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        KinesisSystemConsumer consumer = new KinesisSystemConsumer(systemName, config);
        consumers.put(systemName, consumer);
        return consumer;
    }

    //TODO check metrics
    //TODO serialization
    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        KinesisSystemProducer producer = new KinesisSystemProducer(systemName, config);
        return producer;
    }

    /**
     * Needed so that the CheckpointManager can find the SystemConsumer instance.
     * Package visibility since the rest of the world shouldn't have to worry
     * about this.
     */
    static KinesisSystemConsumer getConsumerBySystemName(String systemName) {
        return consumers.get(systemName);
    }
}
