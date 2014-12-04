---
layout: page
title: Upgrading from 0.7.0 to 0.8.0
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Samza's [checkpointing](../../documentation/{{site.version}}/container/checkpointing.html) implementation changed between Samza 0.7.0 and 0.8.0. If you are running a Samza job with 0.7.0, and upgrade to 0.8.0, your job's checkpoint offsets will be lost, and the job will start (by default) with the most recent message in its input streams. If this is undesirable, and a job needs to pick up where it left off, the following steps must be taken:

1. Shutdown your job.
2. Run the [CheckpointMigrationTool](https://git-wip-us.apache.org/repos/asf?p=incubator-samza.git;a=blob;f=samza-kafka/src/main/scala/org/apache/samza/util/CheckpointMigrationTool.scala;h=5c4b3c4c544ae4367377b1a84d9a85a3de671018;hb=0.8.0).
3. Start your job.

The CheckpointMigrationTool is responsible for migrating your checkpoint topic from the 0.7.0 style format to the 0.8.0 style format. This tool works only against Kafka, so you must be storing your checkpoints in Kafka with the [KafkaCheckpointManager](https://git-wip-us.apache.org/repos/asf?p=incubator-samza.git;a=blob;f=samza-kafka/src/main/scala/org/apache/samza/checkpoint/kafka/KafkaCheckpointManager.scala;h=1d5627d0c561a0be6b48ee307b755958e62b783e;hb=0.8.0).

### Running CheckpointMigrationTool

Checkout Samza 0.8.0:

    git clone http://git-wip-us.apache.org/repos/asf/incubator-samza.git
    cd incubator-samza
    git fetch origin 0.8.0
    git checkout 0.8.0

Run the checkpoint migration task:

    ./gradlew samza-shell:checkpointMigrationTool -PconfigPath=file:///path/to/job/config.properties

The configPath property should be pointed at the .properties file for the job you wish to migrate. The tool will use the job's properties file to connect to the Kafka cluster, and migrate the checkpointed offsets to the 0.8.0 format. Once the tool is complete, the job should be restarted so that it can pick up the migrated offsets.

_NOTE: The checkpointMigrationTool task must be run from a machine that can connect to the Kafka cluster._
