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

#!/bin/bash

###############################################################################################
## setup script for join test--delete all kafka and zk data and restart them in a fresh state
###############################################################################################

echo "Shutting down and cleaning up..."
pkill -f kafka.Kafka
pkill -f org.apache.zookeeper.server.quorum.QuorumPeerMain
sleep 5
rm -rf /tmp/kafka-logs /tmp/zookeeper
sleep 2

echo "Starting zk and kafka..."
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 5
bin/kafka-server-start.sh config/server.properties &
sleep 5

echo "Creating topics..."
bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic epoch
bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic emitter-state
bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic emitted
bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic joiner-state
bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic completed-keys
bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic checker-state

echo "all done"