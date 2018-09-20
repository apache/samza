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
import sys
import logging
from kafka import SimpleProducer, SimpleConsumer
import time
import traceback
from stream_processor import StreamProcessor
from zk_client import ZkClient
import threading

logger = logging.getLogger(__name__)
NUM_MESSAGES = 50
GROUP_COORDINATION_TIMEOUT = 14
TEST_OUTPUT_TOPIC = 'standalone_integration_test_kafka_output_topic'
zk_client = None

### TODO: In each test add barrier state and processorId validations after fixing data serialization format in zookeeper(SAMZA-1749).
def __purge_zk_data():
    """
    Recursively deletes all data nodes created in zookeeper in a test-run.
    """
    zk_client.purge_all_nodes()

def __pump_messages_into_input_topic():
    """
    Produce 50 messages into input topic: standalone_integration_test_kafka_input_topic.
    """
    kafka_client = None
    input_topic = 'standalone_integration_test_kafka_input_topic'
    try:
        kafka_client = util.get_kafka_client()
        kafka_client.ensure_topic_exists(input_topic)
        producer = SimpleProducer(kafka_client, async=False, req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT, ack_timeout=30000)
        logger.info('Producing {0} messages to topic: {1}'.format(NUM_MESSAGES, input_topic))
        for message_index in range(1, NUM_MESSAGES + 1):
            producer.send_messages(input_topic, str(message_index))
    except:
        logger.error(traceback.format_exc(sys.exc_info()))
    finally:
        if kafka_client is not None:
            kafka_client.close()

def __setup_processors():
    """
    Instantiates and schedules three stream processors for execution in localhost.
    :return the instantiated stream processors.
    """
    processors = {}
    for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
        processors[processor_id] = StreamProcessor(host_name='localhost', processor_id=processor_id)
        processors[processor_id].start()
    return processors

def __tear_down_processors(processors):
    """
    Kills all the stream processor passed in :param processors.
    """
    for processor_id, processor in processors.iteritems():
        logger.info("Killing processor: {0}.".format(processor_id))
        processor.kill()

def __setup_zk_client():
    """
    Instantiate a ZkClient to connect to a zookeeper server in localhost.
    """
    global zk_client
    zk_client = ZkClient(zookeeper_host='127.0.0.1', zookeeper_port='2181', app_name='test-app-name', app_id='test-app-id')
    zk_client.start()

def __teardown_zk_client():
    """
    Stops the ZkClient.
    """
    global zk_client
    zk_client.stop()

def job_model_watcher(event, expected_processors):
    start_time_seconds = time.time()
    elapsed_time_seconds = (int)(time.time() - start_time_seconds)
    while elapsed_time_seconds <= 30:
        recent_job_model = zk_client.get_latest_job_model()
        if set(recent_job_model['containers'].keys()) == set(expected_processors):
            event.set()
            return
        else:
            time.sleep(2)
        elapsed_time_seconds = (int)(time.time() - start_time_seconds)

def __validate_job_model(job_model, killed_processors=[]):
    ## Validate the TaskModel. Check if all the partitions are assigned to the containers.
    expected_ssps = [{u'partition': 0, u'system': u'testSystemName', u'stream': u'standalone_integration_test_kafka_input_topic'},
                        {u'partition': 1, u'system': u'testSystemName', u'stream': u'standalone_integration_test_kafka_input_topic'},
                        {u'partition': 2, u'system': u'testSystemName', u'stream': u'standalone_integration_test_kafka_input_topic'}]
    actual_ssps = []
    for container_id, tasks in job_model['containers'].iteritems():
        for partition, ssps in tasks['tasks'].iteritems():
            actual_ssps.append(ssps['system-stream-partitions'][0])
    actual_ssps.sort()
    assert expected_ssps == actual_ssps, 'Expected ssp: {0}, Actual ssp: {1}.'.format(expected_ssps, actual_ssps)

    ## Validate the ContainerModel. Live processors should be present in the JobModel and killed processors should not be in JobModel.
    active_processors = zk_client.get_active_processors()
    assert set(active_processors) == set(job_model['containers'].keys()), 'ProcessorIds: {0} does not exist in JobModel: {1}.'.format(active_processors, job_model['containers'].keys())
    for processor_id in killed_processors:
        assert processor_id not in job_model['containers'], 'Processor: {0} exists in JobModel: {1}.'.format(processor_id, job_model)

def __get_job_model(expected_processors):
    event = threading.Event()
    zk_client.watch_job_model(job_model_watcher(event=event, expected_processors=expected_processors))
    event.wait(2 * GROUP_COORDINATION_TIMEOUT)
    return zk_client.get_latest_job_model()

def test_kill_leader():
    """
    Launches three stream processors. Kills the leader processor. Waits till the group coordination timeout
    and verifies that the final JobModel contains both the followers.
    """
    processors = {}
    try:
        __setup_zk_client()
        __pump_messages_into_input_topic()
        processors = __setup_processors()

        ## Validations before killing the leader.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model, [])

        leader_processor_id = zk_client.get_leader_processor_id()
        processors.pop(leader_processor_id).kill()

        ## Validations after killing the leader.
        job_model = __get_job_model(expected_processors=processors.keys())
        assert leader_processor_id != zk_client.get_leader_processor_id(), '{0} is still the leader'.format(leader_processor_id)
        __validate_job_model(job_model, [leader_processor_id])
    except:
        ## Explicitly logging exception, since zopkio doesn't log complete stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __tear_down_processors(processors)
        __purge_zk_data()
        __teardown_zk_client()

def test_kill_one_follower():
    """
    Launches three stream processors. Kills one follower processor. Waits till the group coordination timeout and
    verifies that the final JobModel contains the leader processor and un-killed follower processor.
    """
    processors = {}
    try:
        __setup_zk_client()
        __pump_messages_into_input_topic()
        processors = __setup_processors()

        leader_processor_id, killed_processors = zk_client.get_leader_processor_id(), []

        ## Validations before killing the follower.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model)

        for processor_id, deployer in processors.iteritems():
            if processor_id != leader_processor_id:
                follower = processors.pop(processor_id)
                follower.kill()
                killed_processors.append(follower)
                break

        ## Validations after killing the follower.
        job_model = __get_job_model(expected_processors=processors.keys())
        assert leader_processor_id == zk_client.get_leader_processor_id(), '{0} is not the leader'.format(leader_processor_id)
        __validate_job_model(job_model, killed_processors)
    except:
        ## Explicitly logging exception, since zopkio doesn't log complete stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __tear_down_processors(processors)
        __purge_zk_data()
        __teardown_zk_client()

def test_kill_multiple_followers():
    """
    Launches three stream processors. Kills both the follower processors. Waits for group coordination timeout
    and verifies that the final JobModel contains only the leader processor.
    """
    processors = {}
    try:
        __setup_zk_client()
        __pump_messages_into_input_topic()
        processors = __setup_processors()

        ## Validations before killing the followers.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model)

        leader_processor_id, killed_processors = zk_client.get_leader_processor_id(), []

        for processor_id in processors.keys():
            if processor_id != leader_processor_id:
                follower = processors.pop(processor_id)
                killed_processors.append(follower)
                follower.kill()

        ## Validations after killing the followers.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model, killed_processors)
    except:
        ## Explicitly logging exception, since zopkio doesn't log complete stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __tear_down_processors(processors)
        __purge_zk_data()
        __teardown_zk_client()

def test_kill_leader_and_a_follower():
    """
    Launches three stream processors. Kills both a leader and a follower processors.
    Waits till the group coordination timeout and verifies that the final JobModel contains only one processor.
    """
    processors = {}
    try:
        __setup_zk_client()
        __pump_messages_into_input_topic()
        processors = __setup_processors()

        leader_processor_id = zk_client.get_leader_processor_id()

        ## Validations before killing the leader and follower.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model)

        killed_processors = [leader_processor_id]
        processors.pop(leader_processor_id).kill()

        for processor_id in processors.keys():
            follower = processors.pop(processor_id)
            killed_processors.append(processor_id)
            follower.kill()
            break

        ## Validations after killing the leader and follower.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model, killed_processors)
    except:
        ## Explicitly logging exception, since zopkio doesn't log complete stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __tear_down_processors(processors)
        __purge_zk_data()
        __teardown_zk_client()

def test_pause_resume_leader():
    """
    Launches three processors. Pauses the leader processor. Wait till group coordination timeout and verifies that the
    JobModel doesn't contain leader processor. Resumes the leader processor and waits till group coordination timeout,
    verifies that new JobModel contains the previously paused leader processor.
    """
    processors = {}
    try:
        __setup_zk_client()
        __pump_messages_into_input_topic()
        processors = __setup_processors()

        ## Validations before pausing the leader.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model)

        leader_processor_id = zk_client.get_leader_processor_id()
        leader = processors.pop(leader_processor_id)

        logger.info("Pausing the leader processor: {0}.".format(leader_processor_id))
        leader.pause()

        ## Validations after pausing the leader.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model, [leader_processor_id])

        logger.info("Resuming the leader processor: {0}.".format(leader_processor_id))
        leader.resume()

        ## Validations after resuming the leader.
        job_model = __get_job_model(expected_processors=processors.keys())
        __validate_job_model(job_model)

        leader.kill()
    except:
        ## Explicitly logging exception, since zopkio doesn't log complete stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __tear_down_processors(processors)
        __purge_zk_data()
        __teardown_zk_client()
