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
import json
from kazoo.client import KazooClient
import logging
import sys
import traceback

logger = logging.getLogger(__name__)

class ZkClient:

    """
    Wrapper class over KazooClient. Provides utility methods for standalone failure tests to get details about
    processor group state stored in zookeeper.

    Instantiates a kazoo client to connect to zookeeper server at :param zookeeper_host::param zookeeper_port.
    """
    def __init__(self, zookeeper_host, zookeeper_port, app_name, app_id):
        self.kazoo_client = KazooClient(hosts='{0}:{1}'.format(zookeeper_host, zookeeper_port))
        self.zk_base_node = 'app-{0}-{1}/{2}-{3}-coordinationData'.format(app_name, app_id, app_name, app_id)

    def start(self):
        """
        Establishes connection with the zookeeper server at self.host_name:self.port.
        """
        self.kazoo_client.start()

    def stop(self):
        """
        Closes and releases the connection held with the zookeeper server.
        """
        self.kazoo_client.stop()

    def watch_job_model(self, watch_function):
        self.kazoo_client.ensure_path('{0}/JobModelGeneration/jobModels/'.format(self.zk_base_node))
        self.kazoo_client.get_children('{0}/JobModelGeneration/jobModels/'.format(self.zk_base_node), watch=watch_function)

    def get_latest_job_model(self):
        """
        Reads and returns the latest JobModel from zookeeper.
        """
        job_model_dict = {}
        try:
            childZkNodes = self.kazoo_client.get_children('{0}/JobModelGeneration/jobModels/'.format(self.zk_base_node))
            if len(childZkNodes) > 0:
                childZkNodes.sort()
                childZkNodes.reverse()

                job_model_generation_path = '{0}/JobModelGeneration/jobModels/{1}/'.format(self.zk_base_node, childZkNodes[0])
                job_model, _ = self.kazoo_client.get(job_model_generation_path)

                """
                ZkClient java library stores the data in the following format in zookeeper:
                        class_name, data_length, actual_data

                JobModel json manipulation: Delete all the characters before first occurrence of '{' in jobModel json string.

                Normal json deserialization without the above custom string massaging fails. This will be removed after SAMZA-1749.
                """

                first_curly_brace_index = job_model.find('{')
                job_model = job_model[first_curly_brace_index: ]
                job_model_dict = json.loads(job_model)
                logger.info("Recent JobModel in zookeeper: {0}".format(job_model_dict))
        except:
            logger.error(traceback.format_exc(sys.exc_info()))
        return job_model_dict

    def get_leader_processor_id(self):
        """
        Determines the processorId of the current leader in a processors group.

        Returns the processorId of the leader if leader exists.
        Returns None otherwise.
        """
        leader_processor_id = None
        try:
            processors_path =  '{0}/processors'.format(self.zk_base_node)
            childZkNodes = self.kazoo_client.get_children(processors_path)
            childZkNodes.sort()
            child_processor_path = '{0}/{1}'.format(processors_path, childZkNodes[0])
            processor_data, _ = self.kazoo_client.get(child_processor_path)
            host, leader_processor_id = processor_data.split(" ")
        except:
            logger.error(traceback.format_exc(sys.exc_info()))
        return leader_processor_id

    def purge_all_nodes(self):
        """
        Recursively delete all zookeeper nodes from the base node: self.zk_base_node.
        """
        try:
            self.kazoo_client.delete(path=self.zk_base_node, version=-1, recursive=True)
        except:
            logger.error(traceback.format_exc(sys.exc_info()))

    def get_all_ephemeral_processors(self):
        """
        Determines the processor ids that are active in zookeeper.
        """
        processor_ids = []
        try:
            processors_path =  '{0}/processors'.format(self.zk_base_node)
            childZkNodes = self.kazoo_client.get_children(processors_path)
            childZkNodes.sort()

            for childZkNode in childZkNodes:
                child_processor_path = '{0}/{1}'.format(processors_path, childZkNode)
                processor_data, _ = self.kazoo_client.get(child_processor_path)
                host, processor_id = processor_data.split(" ")
                processor_ids.append(processor_id)
        except:
            logger.error(traceback.format_exc(sys.exc_info()))

        return processor_ids
