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

import os
import time
import logging
import socket
import errno
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
import zopkio.runtime as runtime

logger = logging.getLogger(__name__)

CWD = os.path.dirname(os.path.abspath(__file__))
HOME_DIR = os.path.join(CWD, os.pardir)
DATA_DIR = os.path.join(HOME_DIR, 'data')
TEST_TOPIC = 'samza-test-topic'
TEST_OUTPUT_TOPIC = 'samza-test-topic-output'
NUM_MESSAGES = 50

def test_samza_job():
  """
  Sends 50 messages (1 .. 50) to samza-test-topic.
  """
  logger.info('Running test_samza_job')
  kafka = _get_kafka_client()
  kafka.ensure_topic_exists(TEST_TOPIC)
  producer = SimpleProducer(kafka,
    async=False,
    req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT,
    ack_timeout=30000)
  for i in range(1, NUM_MESSAGES + 1):
    producer.send_messages(TEST_TOPIC, str(i))
  kafka.close()

def validate_samza_job():
  """
  Validates that negate-number negated all messages, and sent the output to 
  samza-test-topic-output.
  """
  logger.info('Running validate_samza_job')
  kafka = _get_kafka_client()
  kafka.ensure_topic_exists(TEST_OUTPUT_TOPIC)
  consumer = SimpleConsumer(kafka, 'samza-test-group', TEST_OUTPUT_TOPIC)
  messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=60)
  message_count = len(messages)
  assert NUM_MESSAGES == message_count, 'Expected {0} lines, but found {1}'.format(NUM_MESSAGES, message_count)
  for message in map(lambda m: m.message.value, messages):
    assert int(message) < 0 , 'Expected negative integer but received {0}'.format(message)
  kafka.close()

def _get_kafka_client(num_retries=20, retry_sleep=1):
  """
  Returns a KafkaClient based off of the kafka_hosts and kafka_port configs set 
  in the active runtime.
  """
  kafka_hosts = runtime.get_active_config('kafka_hosts').values()
  kafka_port = runtime.get_active_config('kafka_port')
  assert len(kafka_hosts) > 0, 'Missing required configuration: kafka_hosts'
  connect_string = ','.join(map(lambda h: h + ':{0},'.format(kafka_port), kafka_hosts)).rstrip(',')
  # wait for at least one broker to come up
  if not _wait_for_server(kafka_hosts[0], kafka_port, 30):
    raise Exception('Unable to connect to Kafka broker: {0}:{1}'.format(kafka_hosts[0], kafka_port))
  return KafkaClient(connect_string)

def _wait_for_server(host, port, timeout=5, retries=12):
  """
  Keep trying to connect to a host port until the retry count has been reached.
  """
  s = socket.socket()

  for i in range(retries):
    try:
      s.settimeout(timeout)
      s.connect((host, port))
    except socket.timeout, err:
      # Exception occurs if timeout is set. Wait and retry.
      pass
    except socket.error, err:
      # Exception occurs if timeout > underlying network timeout. Wait and retry.
      if type(err.args) != tuple or err[0] != errno.ETIMEDOUT:
        raise
    else:
      s.close()
      return True
  return False

