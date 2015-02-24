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

import logging
import socket
import errno
import zopkio.runtime as runtime
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
from zopkio.runtime import get_active_config as c

logger = logging.getLogger(__name__)

DEPLOYER = 'samza_job_deployer'

def start_job(package_id, job_id, config_file):
  """
  Start a Samza job.
  """
  logger.info('Starting {0}.{1}'.format(package_id, job_id))
  samza_job_deployer = runtime.get_deployer(DEPLOYER)
  samza_job_deployer.start(job_id, {
    'package_id': package_id,
    'config_file': config_file,
  })

def await_job(package_id, job_id):
  """
  Wait for a Samza job to finish.
  """
  logger.info('Awaiting {0}.{1}'.format(package_id, job_id))
  samza_job_deployer = runtime.get_deployer(DEPLOYER)
  samza_job_deployer.await(job_id, {
    'package_id': package_id,
  })

def get_kafka_client(num_retries=20, retry_sleep=1):
  """
  Returns a KafkaClient based off of the kafka_hosts and kafka_port configs set 
  in the active runtime.
  """
  kafka_hosts = runtime.get_active_config('kafka_hosts').values()
  kafka_port = runtime.get_active_config('kafka_port')
  assert len(kafka_hosts) > 0, 'Missing required configuration: kafka_hosts'
  connect_string = ','.join(map(lambda h: h + ':{0},'.format(kafka_port), kafka_hosts)).rstrip(',')
  # wait for at least one broker to come up
  if not wait_for_server(kafka_hosts[0], kafka_port, 30):
    raise Exception('Unable to connect to Kafka broker: {0}:{1}'.format(kafka_hosts[0], kafka_port))
  return KafkaClient(connect_string)

def wait_for_server(host, port, timeout=5, retries=12):
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
