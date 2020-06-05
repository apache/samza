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

DEPLOYER = 'samza_job_deployer'
JOB_ID = 'negate_number'
PACKAGE_ID = 'tests'
CONFIG_FILE = 'config/negate-number.properties'
TEST_INPUT_TOPIC = 'samza-test-topic'
TEST_OUTPUT_TOPIC = 'samza-test-topic-output'
NUM_MESSAGES = 50

def test_samza_job():
  """
  Runs a job that reads converts input strings to integers, negates the
  integer, and outputs to a Kafka topic.
  """
  _load_data()
  util.start_job(PACKAGE_ID, JOB_ID, CONFIG_FILE)
  util.await_job(PACKAGE_ID, JOB_ID)

def validate_samza_job():
  """
  Validates that negate-number negated all messages, and sent the output to
  samza-test-topic-output.
  """
  logger.info('Running validate_samza_job')
  kafka = util.get_kafka_client()
  kafka.ensure_topic_exists(TEST_OUTPUT_TOPIC)
  consumer = SimpleConsumer(kafka, 'samza-test-group', TEST_OUTPUT_TOPIC)
  messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=300)
  message_count = len(messages)
  assert NUM_MESSAGES == message_count, 'Expected {0} lines, but found {1}'.format(NUM_MESSAGES, message_count)
  for message in map(lambda m: m.message.value, messages):
    assert int(message) < 0 , 'Expected negative integer but received {0}'.format(message)
  kafka.close()

def _load_data():
  """
  Sends 50 messages (1 .. 50) to samza-test-topic.
  """
  logger.info('Running test_samza_job')
  kafka = util.get_kafka_client()
  kafka.ensure_topic_exists(TEST_INPUT_TOPIC)
  producer = SimpleProducer(
    kafka,
    async=False,
    req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT,
    ack_timeout=30000)
  for i in range(1, NUM_MESSAGES + 1):
    producer.send_messages(TEST_INPUT_TOPIC, str(i))
  kafka.close()
