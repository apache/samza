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

# This script will generate wikipedia-raw data to Kafka

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
ZOOKEEPER=localhost:2181
KAFKA_BROKER=localhost:9092

# overwritten options
while getopts "z:b:" option
do
  case ${option} in 
    z) ZOOKEEPER="${OPTARG}";;
    b) KAFKA_BROKER="${OPTARG}";;
  esac
done
echo "Using ${ZOOKEEPER} as the zookeeper. You can overwrite it with '-z yourlocation'"
echo "Using ${KAFKA_BROKER} as the kafka broker. You can overwrite it with '-b yourlocation'"

# check if the topic exists. if not, create the topic
EXIST=$($BASE_DIR/deploy/kafka/bin/kafka-topics.sh --describe --topic wikipedia-raw --zookeeper $ZOOKEEPER)
if [ -z "$EXIST" ]
  then
    $BASE_DIR/deploy/kafka/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --topic wikipedia-raw --partition 1 --replication-factor 1
fi

# produce raw data
while sleep 1
do 
  $BASE_DIR/deploy/kafka/bin/kafka-console-producer.sh < $BASE_DIR/wikipedia-raw.json --topic wikipedia-raw --broker $KAFKA_BROKER
done

