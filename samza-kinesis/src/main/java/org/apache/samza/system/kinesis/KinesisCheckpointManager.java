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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KinesisCheckpointManager implements CheckpointManager {

    private static final Log LOG = LogFactory.getLog(KinesisCheckpointManager.class);

    public KinesisCheckpointManager(Config config) {
    }

    @Override
    public void start() {
    }

    @Override
    public void register(TaskName taskName) {
    }

    @Override
    public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
        Set<String> systemNames = new HashSet<String>();
        for (SystemStreamPartition ssp : checkpoint.getOffsets().keySet()) {
            systemNames.add(ssp.getSystem());
        }

//        try {
//            for (String systemName : systemNames) {
//                KinesisSystemFactory.getConsumerBySystemName(systemName).checkpoint(checkpoint);
//            }
//        } catch (Exception e) {
//            // TODO handle this properly
//            LOG.error("Cannot write checkpoint", e);
//        }
    }

    @Override
    public Checkpoint readLastCheckpoint(TaskName taskName) {
        return null;
    }

    @Override
    public void stop() {
    }
}
