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
import logging
import shutil
import urllib
import zopkio.runtime as runtime
import zopkio.adhoc_deployer as adhoc_deployer
from zopkio.runtime import get_active_config as c
from samza_job_yarn_deployer import SamzaJobYarnDeployer

logger = logging.getLogger(__name__)
deployers = None
samza_job_deployer = None
samza_install_path = None

def _download_packages():
  for url_key in ['url_hadoop', 'url_kafka', 'url_zookeeper']:
    logger.debug('Getting download URL for: {0}'.format(url_key))
    url = c(url_key)
    filename = os.path.basename(url)
    if os.path.exists(filename):
      logger.debug('Using cached file: {0}'.format(filename))
    else:
      logger.info('Downloading: {0}'.format(url))
      urllib.urlretrieve(url, filename)

def _new_ssh_deployer(config_prefix, name=None):
  deployer_name = config_prefix if name == None else name
  return adhoc_deployer.SSHDeployer(deployer_name, {
    'install_path': os.path.join(c('remote_install_path'), c(config_prefix + '_install_path')),
    'executable': c(config_prefix + '_executable'),
    'post_install_cmds': c(config_prefix + '_post_install_cmds', []),
    'start_command': c(config_prefix + '_start_cmd'),
    'stop_command': c(config_prefix + '_stop_cmd'),
    'extract': True,
    'sync': True,
  })

def setup_suite():
  global deployers, samza_job_deployer, samza_install_path
  logger.info('Current working directory: {0}'.format(os.getcwd()))
  samza_install_path = os.path.join(c('remote_install_path'), c('samza_install_path'))

  _download_packages()

  deployers = {
    'zookeeper': _new_ssh_deployer('zookeeper'),
    'yarn_rm': _new_ssh_deployer('yarn_rm'),
    'yarn_nm': _new_ssh_deployer('yarn_nm'),
    'kafka': _new_ssh_deployer('kafka'),
  }

  # Enforce install order through list.
  for name in ['zookeeper', 'yarn_rm', 'yarn_nm', 'kafka']:
    deployer = deployers[name]
    runtime.set_deployer(name, deployer)
    for instance, host in c(name + '_hosts').iteritems():
      logger.info('Deploying {0} on host: {1}'.format(instance, host))
      deployer.deploy(instance, {
        'hostname': host
      })

  # Setup Samza job deployer.
  samza_job_deployer = SamzaJobYarnDeployer({
    'config_factory': c('samza_config_factory'),
    'yarn_site_template': c('yarn_site_template'),
    'yarn_driver_configs': c('yarn_driver_configs'),
    'yarn_nm_hosts': c('yarn_nm_hosts').values(),
    'install_path': samza_install_path,
  })

  samza_job_deployer.install('tests', {
    'executable': c('samza_executable'),
  })

  runtime.set_deployer('samza_job_deployer', samza_job_deployer)

def teardown_suite():
  samza_job_deployer.uninstall('tests')

  # Undeploy everything.
  for name, deployer in deployers.iteritems():
    for instance, host in c(name + '_hosts').iteritems():
      deployer.undeploy(instance)
