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
import zopkio.adhoc_deployer as adhoc_deployer
from zopkio.runtime import get_active_config as c
from subprocess import PIPE, Popen
import logging
import time
import urllib
import os

TEST_INPUT_TOPIC = 'standalone_integration_test_kafka_input_topic'
TEST_OUTPUT_TOPIC = 'standalone_integration_test_kafka_output_topic'

logger = logging.getLogger(__name__)
deployers = {}

def setup_suite():
    """
    Setup method that will be run once by zopkio test_runner before all the integration tests.
    """
    ## Download and deploy zk and kafka. Configuration for kafka, zookeeper are defined in kafka.json and zookeeper.json.
    _download_components(['zookeeper', 'kafka'])

    _deploy_components(['zookeeper', 'kafka'])

    ## Create input and output topics.
    for topic in [TEST_INPUT_TOPIC, TEST_OUTPUT_TOPIC]:
        logger.info("Deleting topic: {0}.".format(topic))
        _delete_kafka_topic('localhost:2181', topic)
        logger.info("Creating topic: {0}.".format(topic))
        _create_kafka_topic('localhost:2181', topic, 3, 1)

def _download_components(components):
    """
    Download the :param components if unavailable in deployment directory using url defined in config.
    """

    for component in components:
        url_key = 'url_{0}'.format(component)
        url = c(url_key)
        filename = os.path.basename(url)
        if os.path.exists(filename):
            logger.debug('Using cached file: {0}.'.format(filename))
        else:
            logger.info('Downloading {0} from {1}.'.format(component, url))
            urllib.urlretrieve(url, filename)

def _deploy_components(components):
    """
    Install and start all the :param components through binaries in deployment directory.
    """

    global deployers

    for component in components:
        config =  {
            'install_path': os.path.join(c('remote_install_path'), c(component + '_install_path')),
            'executable': c(component + '_executable'),
            'post_install_cmds': c(component + '_post_install_cmds', []),
            'start_command': c(component + '_start_cmd'),
            'stop_command': c(component + '_stop_cmd'),
            'extract': True,
            'sync': True,
        }
        deployer = adhoc_deployer.SSHDeployer(component, config)
        deployers[component] = deployer
        for instance, host in c(component + '_hosts').iteritems():
            logger.info('Deploying {0} on host: {1}'.format(instance, host))
            deployer.start(instance, {'hostname': host})
            time.sleep(5)

def _create_kafka_topic(zookeeper_servers, topic_name, partition_count, replication_factor):
    """
    :param zookeeper_servers: Comma separated list of zookeeper servers used for setting up kafka consumer connector.
    :param topic_name: name of kafka topic to create.
    :param partition_count: Number of partitions of the kafka topic.
    :param replication_factor: Replication factor of the kafka topic.
    """

    ### Using command line utility to create kafka topic since kafka python API doesn't support configuring partitionCount during topic creation.
    base_dir = os.getcwd()
    create_topic_command = 'sh {0}/deploy/kafka/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --create --zookeeper {1} --replication-factor {2} --partitions {3} --topic {4}'.format(base_dir, zookeeper_servers, replication_factor, partition_count, topic_name)
    p = Popen(create_topic_command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    logger.info("Output from create kafka topic: {0}\nstdout: {1}\nstderr: {2}".format(topic_name, output, err))

def _delete_kafka_topic(zookeeper_servers, topic_name):
    """
    Delete kafka topic defined by the method parameters.

    :param zookeeper_servers: Comma separated list of zookeeper servers used for setting up kafka consumer connector.
    :param topic_name: name of kafka topic to delete.
    """

    base_dir = os.getcwd()
    delete_topic_command = 'sh {0}/deploy/kafka/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --delete --zookeeper {1} --topic {2}'.format(base_dir, zookeeper_servers, topic_name)
    logger.info("Deleting topic: {0}.".format(topic_name))
    p = Popen(delete_topic_command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    logger.info("Output from delete kafka topic: {0}\nstdout: {1}\nstderr: {2}".format(topic_name, output, err))

def teardown_suite():
    """
    Teardown method that will be run once by zopkio test_runner after all the integration tests.
    """
    for component in ['kafka', 'zookeeper']:
        deployer = deployers[component]
        for instance, host in c(component + '_hosts').iteritems():
            deployer.undeploy(instance)
