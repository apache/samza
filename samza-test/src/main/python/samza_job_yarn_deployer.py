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
import re
import logging
import json
import requests
import shutil
import tarfile
import zopkio.constants as constants
import zopkio.runtime as runtime
import templates

from subprocess import PIPE, Popen
from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file

logger = logging.getLogger(__name__)

class SamzaJobYarnDeployer(Deployer):
  def __init__(self, configs={}):
    """
    Instantiates a Samza job deployer that uses run-job.sh and kill-yarn-job.sh 
    to start and stop Samza jobs in a YARN grid.

    param: configs -- Map of config key/values pairs. These configs will be used
    as a default whenever overrides are not provided in the methods (install, 
    start, stop, etc) below.
    """
    logging.getLogger("paramiko").setLevel(logging.ERROR)
    # map from job_id to app_id
    self.username = runtime.get_username()
    self.password = runtime.get_password()
    self.app_ids = {}
    self.default_configs = configs
    Deployer.__init__(self)

  def install(self, package_id, configs={}):
    """
    Installs a package (tarball, or zip) on to a list of remote hosts by 
    SFTP'ing the package to the remote install_path.

    param: package_id -- A unique ID used to identify an installed YARN package.
    param: configs -- Map of config key/values pairs. Valid keys include:

    yarn_site_template: Jinja2 yarn-site.xml template local path.
    yarn_driver_configs: Key/value pairs to be injected into the yarn-site.xml template.
    yarn_nm_hosts: A list of YARN NM hosts to install the package onto.
    install_path: An absolute path where the package will be installed.
    executable: A local path pointing to the location of the package that should be installed on remote hosts.
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['yarn_site_template', 'yarn_driver_configs', 'yarn_nm_hosts', 'install_path', 'executable'])

    # Get configs.
    nm_hosts = configs.get('yarn_nm_hosts')
    install_path = configs.get('install_path')
    executable = configs.get('executable')

    # FTP and decompress job tarball to all NMs.
    exec_file_location = os.path.join(install_path, self._get_package_tgz_name(package_id))
    exec_file_install_path = os.path.join(install_path, package_id)
    for host in nm_hosts:
      logger.info('Deploying {0} on host: {1}'.format(package_id, host))
      with get_ssh_client(host, self.username, self.password) as ssh:
        better_exec_command(ssh, "mkdir -p {0}".format(install_path), "Failed to create path: {0}".format(install_path))
      with get_sftp_client(host, self.username, self.password) as ftp:
        def progress(transferred_bytes, total_bytes_to_transfer):
          logger.debug("{0} of {1} bytes transferred.".format(transferred_bytes, total_bytes_to_transfer))
        ftp.put(executable, exec_file_location, callback=progress)

    # Extract archive locally so we can use run-job.sh.
    executable_tgz = tarfile.open(executable, 'r:gz')
    executable_tgz.extractall(package_id)

    # Generate yarn-site.xml install it in package's local 'config' directory.
    yarn_site_dir = self._get_yarn_conf_dir(package_id)
    yarn_site_path = os.path.join(yarn_site_dir, 'yarn-site.xml')
    logger.info("Installing yarn-site.xml to {0}".format(yarn_site_path))
    if not os.path.exists(yarn_site_dir):
      os.makedirs(yarn_site_dir)
    templates.render_config(configs.get('yarn_site_template'), yarn_site_path, configs.get('yarn_driver_configs'))

  def start(self, job_id, configs={}):
    """
    Starts a Samza job using the bin/run-job.sh script.

    param: job_id -- A unique ID used to idenitfy a Samza job. Job IDs are associated
    with a package_id, and a config file.
    param: configs -- Map of config key/values pairs. Valid keys include:

    package_id: The package_id for the package that contains the code for job_id. 
    Usually, the package_id refers to the .tgz job tarball that contains the 
    code necessary to run job_id.
    config_factory: The config factory to use to decode the config_file.
    config_file: Path to the config file for the job to be run.
    install_path: Path where the package for the job has been installed on remote NMs.
    properties: (optional) [(property-name,property-value)] Optional override 
    properties for the run-job.sh script. These properties override the 
    config_file's properties.
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['package_id', 'config_factory', 'config_file', 'install_path'])

    # Get configs.
    package_id = configs.get('package_id')
    config_factory = configs.get('config_factory')
    config_file = configs.get('config_file')
    install_path = configs.get('install_path')
    properties = configs.get('properties', {})
    properties['yarn.package.path'] = 'file:' + os.path.join(install_path, self._get_package_tgz_name(package_id))

    # Execute bin/run-job.sh locally from driver machine.
    command = "{0} --config-factory={1} --config-path={2}".format(os.path.join(package_id, "bin/run-job.sh"), config_factory, os.path.join(package_id, config_file))
    env = self._get_env_vars(package_id)
    for property_name, property_value in properties.iteritems():
      command += " --config {0}={1}".format(property_name, property_value)
    p = Popen(command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
    output, err = p.communicate()
    logger.debug("Output from run-job.sh:\nstdout: {0}\nstderr: {1}".format(output, err))
    assert p.returncode == 0, "Command ({0}) returned non-zero exit code ({1}).\nstdout: {2}\nstderr: {3}".format(command, p.returncode, output, err)

    # Save application_id for job_id so we can kill the job later.
    regex = r'.*Submitted application (\w*)'
    match = re.match(regex, output.replace("\n", ' '))
    assert match, "Job ({0}) appears not to have started. Expected to see a log line matching regex: {1}".format(job_id, regex)
    app_id = match.group(1)
    logger.debug("Got application_id {0} for job_id {1}.".format(app_id, job_id))
    self.app_ids[job_id] = app_id

  def stop(self, job_id, configs={}):
    """
    Stops a Samza job using the bin/kill-yarn-job.sh script.

    param: job_id -- A unique ID used to idenitfy a Samza job.
    param: configs -- Map of config key/values pairs. Valid keys include:

    package_id: The package_id for the package that contains the code for job_id.
    Usually, the package_id refers to the .tgz job tarball that contains the
    code necessary to run job_id.
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['package_id'])

    # Get configs.
    package_id = configs.get('package_id')

    # Get the application_id for the job.
    application_id = self.app_ids.get(job_id)

    # Kill the job, if it's been started, or WARN and return if it's hasn't.
    if not application_id:
      logger.warn("Can't stop a job that was never started: {0}".format(job_id))
    else:
      command = "{0} {1}".format(os.path.join(package_id, "bin/kill-yarn-job.sh"), application_id)
      env = self._get_env_vars(package_id)
      p = Popen(command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
      p.wait()
      assert p.returncode == 0, "Command returned non-zero exit code ({0}): {1}".format(p.returncode, command)

  def await(self, job_id, configs={}):
    """
    Waits for a Samza job to finish using bin/stat-yarn-job.sh. A job is 
    finished when its "Final State" is not "UNDEFINED".

    param: job_id -- A unique ID used to idenitfy a Samza job.
    param: configs -- Map of config key/values pairs. Valid keys include:

    package_id: The package_id for the package that contains the code for job_id.
    Usually, the package_id refers to the .tgz job tarball that contains the
    code necessary to run job_id.
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['package_id'])

    # Get configs.
    package_id = configs.get('package_id')

    # Get the application_id for the job.
    application_id = self.app_ids.get(job_id)

    # Stat the job, if it's been started, or WARN and return if it's hasn't.
    final_state = 'UNDEFINED'
    if not application_id:
      logger.warn("Can't stat a job that was never started: {0}".format(job_id))
    else:
      command = "{0} {1}".format(os.path.join(package_id, "bin/stat-yarn-job.sh"), application_id)
      env = self._get_env_vars(package_id)
      while final_state == 'UNDEFINED':
        p = Popen(command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
        output, err = p.communicate()
        logger.debug("Output from run-job.sh:\nstdout: {0}\nstderr: {1}".format(output, err))
        assert p.returncode == 0, "Command ({0}) returned non-zero exit code ({1}).\nstdout: {2}\nstderr: {3}".format(command, p.returncode, output, err)

        # Check the final state for the job.
        regex = r'.*Final.State . (\w*)'
        match = re.match(regex, output.replace("\n", ' '))
        final_state = match.group(1)
        logger.debug("Got final state {0} for job_id {1}.".format(final_state, job_id))
    return final_state

  def uninstall(self, package_id, configs={}):
    """
    Removes the install path for package_id from all remote hosts that it's been
    installed on.

    param: package_id -- A unique ID used to identify an installed YARN package.
    param: configs -- Map of config key/values pairs. Valid keys include:

    yarn_nm_hosts: A list of hosts that package was installed on.
    install_path: Path where the package for the job has been installed on remote NMs.
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['yarn_nm_hosts', 'install_path'])

    # Get configs.
    nm_hosts = configs.get('yarn_nm_hosts')
    install_path = configs.get('install_path')

    # Delete job package on all NMs.
    for host in nm_hosts:
      with get_ssh_client(host, self.username, self.password) as ssh:
        better_exec_command(ssh, "rm -rf {0}".format(install_path), "Failed to remove {0}".format(install_path))

    # Delete job pacakge directory from local driver box.
    shutil.rmtree(package_id)

  # TODO we should implement the below helper methods over time, as we need them.

  def get_processes(self):
    # TODO raise NotImplementedError
    return []

  def get_pid(self, container_id, configs={}):
    raise NotImplementedError

  def get_host(self, container_id):
    raise NotImplementedError

  def get_containers(self, job_id):
    raise NotImplementedError

  def get_jobs(self):
    raise NotImplementedError

  def sleep(self, container_id, delay, configs={}):
    raise NotImplementedError

  def pause(self, container_id, configs={}):
    raise NotImplementedError

  def resume(self, container_id, configs={}):
    raise NotImplementedError

  def kill(self, container_id, configs={}):
    raise NotImplementedError

  def terminate(self, container_id, configs={}):
    raise NotImplementedError

  def get_logs(self, container_id, logs, directory):
    raise NotImplementedError

  def kill_all_process(self):
    pass

  def _validate_configs(self, configs, config_keys):
    for required_config in config_keys:
      assert configs.get(required_config), 'Required config is undefined: {0}'.format(required_config)

  def _get_merged_configs(self, configs):
    tmp = self.default_configs.copy()
    tmp.update(configs)
    return tmp

  def _get_package_tgz_name(self, package_id):
    return '{0}.tgz'.format(package_id)

  def _get_yarn_home_dir(self, package_id):
    return os.path.abspath(package_id)

  def _get_yarn_conf_dir(self, package_id):
    return os.path.join(self._get_yarn_home_dir(package_id), 'config')

  def _get_env_vars(self, package_id):
    env = os.environ.copy()
    env['YARN_CONF_DIR'] = self._get_yarn_conf_dir(package_id)
    env['HADOOP_CONF_DIR'] = env['YARN_CONF_DIR']
    logger.debug('Built environment: {0}'.format(env))
    return env

