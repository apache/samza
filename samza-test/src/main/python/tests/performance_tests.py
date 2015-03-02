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

import util
import logging
import zopkio.runtime as runtime
from kafka import SimpleProducer, SimpleConsumer

logger = logging.getLogger(__name__)

PACKAGE_ID = 'tests'
KAFKA_JOB_ID = 'kafka-read-write-performance'
KAFKA_CONFIG_FILE = 'config/perf/kafka-read-write-performance.properties'
CONTAINER_JOB_ID = 'container-performance'
CONTAINER_CONFIG_FILE = 'config/perf/container-performance.properties'
TEST_INPUT_TOPIC = 'kafka-read-write-performance-input'
TEST_OUTPUT_TOPIC = 'kafka-read-write-performance-output'
NUM_MESSAGES = 1000000
MESSAGE = 'a' * 200

def test_kafka_read_write_performance():
  """
  Runs a Samza job that reads from Kafka, and writes back out to it. The 
  writes/sec for the job is logged to the job's container.
  """
  _load_data()
  util.start_job(PACKAGE_ID, KAFKA_JOB_ID, KAFKA_CONFIG_FILE)
  util.await_job(PACKAGE_ID, KAFKA_JOB_ID)

def validate_kafka_read_write_performance():
  """
  Validates that all messages were sent to the output topic.
  """
  logger.info('Running validate_kafka_read_write_performance')
  kafka = util.get_kafka_client()
  kafka.ensure_topic_exists(TEST_OUTPUT_TOPIC)
  consumer = SimpleConsumer(
    kafka, 
    'samza-test-group', 
    TEST_OUTPUT_TOPIC,
    fetch_size_bytes=1000000,
    buffer_size=32768,
    max_buffer_size=None)
  # wait 5 minutes to get all million messages
  messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=300)
  message_count = len(messages)
  assert NUM_MESSAGES == message_count, 'Expected {0} lines, but found {1}'.format(NUM_MESSAGES, message_count)
  kafka.close()

def test_container_performance():
  """
  Runs TestPerformanceTask with a MockSystem to test how fast the 
  SamzaContainer can go.
  """
  util.start_job(PACKAGE_ID, CONTAINER_JOB_ID, CONTAINER_CONFIG_FILE)
  util.await_job(PACKAGE_ID, CONTAINER_JOB_ID)

def validate_container_performance():
  pass

def _load_data():
  """
  Sends 10 million messages to kafka-read-write-performance-input.
  """
  logger.info('Running test_kafka_read_write_performance')
  kafka = util.get_kafka_client()
  kafka.ensure_topic_exists(TEST_INPUT_TOPIC)
  producer = SimpleProducer(
    kafka,
    req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT,
    ack_timeout=30000,
    batch_send=True,
    batch_send_every_n=200)
  logger.info('Loading {0} test messages.'.format(NUM_MESSAGES))
  for i in range(0, NUM_MESSAGES):
    if i % 100000 == 0:
      logger.info('Loaded {0} messages.'.format(i))
    producer.send_messages(TEST_INPUT_TOPIC, MESSAGE)
  kafka.close()
