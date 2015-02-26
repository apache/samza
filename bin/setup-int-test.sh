#!/bin/bash -e
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script will setup the environment for integration test and kick-start the samza jobs

if [ $# -eq 0 ] || [ -z "$1" ] ; then
    echo "Usage: ./bin/setup-int-test.sh <DEPLOY_DIR>"
    exit -1
fi

DEPLOY_ROOT_DIR=$1
KAFKA_DIR=$1/kafka
SAMZA_DIR=$1/samza

# Setup the deployment Grid
# $BASE_DIR/bin/grid.sh bootstrap

# sleep 10

# Setup the topics
$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic epoch
$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic emitter-state
$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic emitted
$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --create --topic joiner-state
$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic completed-keys
$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic checker-state

# Start the jobs
for job in checker joiner emitter watcher
do
    $SAMZA_DIR/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$SAMZA_DIR/config/join/common.properties --config-path=file://$SAMZA_DIR/config/join/$job.samza --config job.foo=$job
done


