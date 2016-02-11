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

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;

/**
 * Reference this factory class in your Samza job config to make sure that the
 * consumer's position in the input stream is correctly checkpointed. The Kinesis
 * client library actually writes checkpoints to DynamoDB, but we call it
 * KinesisCheckpointManager anyway, because it's meant to be used with Kinesis.<p>
 *
 * Note that this checkpoint manager only allows Kinesis input streams to be
 * checkpointed. If your job is also consuming from some other system, that system
 * will not be checkpointed.<p>
 *
 * Usage example:
 *
 * <pre>
 * task.checkpoint.factory=com.amazonaws.services.kinesis.samza.KinesisCheckpointManagerFactory
 * </pre>
 */
public class KinesisCheckpointManagerFactory implements CheckpointManagerFactory {

    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
        return new KinesisCheckpointManager(config);
    }
}
