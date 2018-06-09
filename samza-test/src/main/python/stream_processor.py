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
import os
import time
import zopkio.adhoc_deployer as adhoc_deployer
from zopkio.remote_host_helper import get_ssh_client, exec_with_env
import zopkio.runtime as runtime

logger = logging.getLogger(__name__)

class StreamProcessor:
    """
    Represents a standalone StreamProcessor that uses zookeeper for coordination. Used in standalone failure tests to
    to manage the lifecycle of linux process(start, kill, pause) associated with the StreamProcessor.
    """

    def __init__(self, host_name, processor_id):
        """
        :param host_name: Represents the host name in which this StreamProcessor will run.
        :param processor_id: Represents the processor_id of StreamProcessor.
        """
        start_cmd = 'export SAMZA_LOG_DIR=\"deploy/{0}\"; export JAVA_OPTS=\"$JAVA_OPTS -Xmx2G\"; ./bin/run-class.sh  org.apache.samza.test.integration.LocalApplicationRunnerMain --config-path ./config/standalone.failure.test.properties --operation run --config processor.id={0} >> /tmp/{0}.log &'
        self.username = runtime.get_username()
        self.password = runtime.get_password()
        self.processor_id = processor_id
        self.host_name = host_name
        self.processor_start_command = start_cmd.format(self.processor_id)
        logger.info('Running processor start command: {0}'.format(self.processor_start_command))
        self.deployment_config = {
            'install_path': os.path.join(runtime.get_active_config('remote_install_path'), 'deploy/{0}'.format(self.processor_id)),
            'executable': 'samza-test_2.11-0.15.0-SNAPSHOT.tgz',
            'post_install_cmds': [],
            'start_command': self.processor_start_command,
            'stop_command': '',
            'extract': True,
            'sync': True,
        }
        self.deployer = adhoc_deployer.SSHDeployer(self.processor_id, self.deployment_config)

    def start(self):
        """
        Submits the StreamProcessor for execution on a host: host_name.
        """
        logger.info("Starting processor with id: {0}.".format(self.processor_id))
        self.deployer.start(self.processor_id, {'hostname': self.host_name})

    def get_processor_id(self):
        """
        Returns the processorId of the StreamProcessor.
        """
        return self.processor_id

    def kill(self):
        """
        Kills the StreamProcessor process through SIGKILL signal.
        """
        self.__send_signal_to_processor("SIGKILL")

    def pause(self):
        """
        Pauses the StreamProcessor process through SIGSTOP signal.
        """
        self.__send_signal_to_processor("SIGSTOP")

    def resume(self):
        """
        Resumes the stream processor process through SIGCONT signal.
        """
        self.__send_signal_to_processor("SIGCONT")

    def __send_signal_to_processor(self, signal):
        """
        Sends a signal(:param signal) to the linux process of the StreamProcessor.
        """
        linux_process_pids = self.__get_pid()
        for linux_process_pid in linux_process_pids:
            command = "kill -{0} {1}".format(signal, linux_process_pid)
            result = self.__execute_command(command)
            logger.info("Result of {0} is: {1}.".format(command, result))

    def __get_pid(self):
        """
        Determines the linux process id associated with this StreamProcessor.
        """
        ps_command = "ps aux | grep '{0}' | grep -v grep | tr -s ' ' | cut -d ' ' -f 2 | grep -Eo '[0-9]+'".format(self.processor_id)
        non_failing_command = "{0}; if [ $? -le 1 ]; then true;  else false; fi;".format(ps_command)
        logger.info("Executing command: {0}.".format(non_failing_command))
        full_output = self.__execute_command(non_failing_command)
        pids = []
        if len(full_output) > 0:
            pids = [int(pid_str) for pid_str in full_output.split('\n') if pid_str.isdigit()]
        return pids

    def __execute_command(self, command):
        """
        Executes the :param command on host: self.host_name.
        """
        with get_ssh_client(self.host_name, username=self.username, password=self.password) as ssh:
            chan = exec_with_env(ssh, command, msg="Failed to get PID", env={})
        execution_result = ''
        while True:
            result_buffer = chan.recv(16)
            if len(result_buffer) == 0:
                break
            execution_result += result_buffer
        return execution_result
