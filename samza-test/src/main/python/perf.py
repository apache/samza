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
from zopkio.runtime import get_active_config as c

LOGS_DIRECTORY = 'logs'
OUTPUT_DIRECTORY = 'output'

def machine_logs():
  log_paths = {}

  # Attach proper path to all logs.
  for config_prefix in ['zookeeper', 'yarn_rm', 'yarn_nm', 'kafka']:
    deployed_path = os.path.join(c('remote_install_path'), c(config_prefix + '_install_path'))
    relative_log_paths = c(config_prefix + '_logs')
    log_paths[config_prefix] = map(lambda l: os.path.join(deployed_path, l), relative_log_paths)

  return {
    'zookeeper_instance_0': log_paths['zookeeper'],
    'kafka_instance_0': log_paths['kafka'],
    'yarn_rm_instance_0': log_paths['yarn_rm'],
    'yarn_nm_instance_0': log_paths['yarn_nm'],
  }

def naarad_logs():
  return {
    'zookeeper_instance_0': [],
    'kafka_instance_0': [],
    'samza_instance_0': [],
    'yarn_rm_instance_0': [],
    'yarn_nm_instance_0': [],
  }

def naarad_config(config, test_name=None):
  return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naarad.cfg')
