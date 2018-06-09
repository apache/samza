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
TEST_OUTPUT_TOPIC = 'standaloneIntegrationTestKafkaOutputTopic'
zk_client = None

### TODO: In each test add barrier state and processorId validations after fixing data serialization format in zookeeper(SAMZA-1749).
def __purge_zk_data():
    """
    Recursively deletes all data nodes created in zookeeper in a test-run.
    """
    zk_client.purge_all_nodes()

def __pump_messages_into_input_topic():
    """
    Produce 50 messages into input topic: standaloneIntegrationTestKafkaInputTopic.
    """
    kafka_client = None
    input_topic = 'standaloneIntegrationTestKafkaInputTopic'
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

def job_model_watch(event, expected_processors):
    start_time_seconds = time.time()
    elapsed_time_seconds = (int)(time.time() - start_time_seconds)
    while elapsed_time_seconds <= 30:
	    recent_job_model = zk_client.get_latest_job_model()
	    if recent_job_model['containers'].keys() == expected_processors:
        	event.set()
		return
	    else:
		time.sleep(2)
            elapsed_time_seconds = (int)(time.time() - start_time_seconds)
        	

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

        leader_processor_id = zk_client.get_leader_processor_id()
        processors.pop(leader_processor_id).kill()

        event = threading.Event()
        zk_client.watch_job_model(job_model_watch(event = event, expected_processors=processors.keys()))

        event.wait(2 * GROUP_COORDINATION_TIMEOUT)

        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel: {1}.'.format(processor_id, job_model)
        assert leader_processor_id not in job_model['containers'], 'Leader processor: {0} exists in JobModel: {1}.'.format(leader_processor_id, job_model)
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

        leader_processor_id = zk_client.get_leader_processor_id()
        for processor_id, deployer in processors.iteritems():
            if processor_id != leader_processor_id:
                processors.pop(processor_id).kill()
                break

        event = threading.Event()
        zk_client.watch_job_model(job_model_watch(event = event, expected_processors=processors.keys()))

        event.wait(GROUP_COORDINATION_TIMEOUT * 2)

        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel: {1}.'.format(processor_id, job_model)
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

        leader_processor_id = zk_client.get_leader_processor_id()
        for processor_id in processors.keys():
            if processor_id != leader_processor_id:
                follower = processors.pop(processor_id)
                follower.kill()

        event = threading.Event()
        zk_client.watch_job_model(job_model_watch(event = event, expected_processors=[leader_processor_id]))

        event.wait(GROUP_COORDINATION_TIMEOUT * 2)

        ## Verifications after killing the processors.
        job_model = zk_client.get_latest_job_model()

        assert leader_processor_id in job_model['containers'], 'Leader processor: {0} does not exist in JobModel: {1}.'.format(leader_processor_id, job_model)
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
        processors.pop(leader_processor_id).kill()

        for processor_id in processors.keys():
            processors.pop(processor_id).kill()
            break

        event = threading.Event()
        zk_client.watch_job_model(job_model_watch(event = event, expected_processors=processors.keys()))

        event.wait(GROUP_COORDINATION_TIMEOUT * 2)

        ## Verifications after killing the processors.
        job_model = zk_client.get_latest_job_model()
        for processor_id in processors.keys():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel: {1}.'.format(processor_id, job_model)
        assert leader_processor_id not in job_model['containers'], 'Leader processor id: {0} exists in JobModel: {1}.'.format(leader_processor_id, job_model)
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
    verifies that new JobModel contains the leader processor.
    """
    processors = {}
    try:
        __setup_zk_client()
        __pump_messages_into_input_topic()
        processors = __setup_processors()

        event = threading.Event()
        zk_client.watch_job_model(job_model_watch(event = event, expected_processors=processors.keys()))

        event.wait(GROUP_COORDINATION_TIMEOUT * 2)

        ## First JobModel generation.
        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel: {1}.'.format(processor_id, job_model)

        leader_processor_id = zk_client.get_leader_processor_id()
        leader = processors.pop(leader_processor_id)

        logger.info("Pausing the leader processor: {0}.".format(leader_processor_id))
        leader.pause()

        event = threading.Event()
        zk_client.watch_job_model(job_model_watch(event = event, expected_processors=processors.keys()))

        event.wait(GROUP_COORDINATION_TIMEOUT * 2)

        ## Verifications after leader was suspended.
        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        logger.info("Resuming the leader processor: {0}.".format(leader_processor_id))
        leader.resume()

        event = threading.Event()
        expected_processors = processors.keys()
        expected_processors.append(leader_processor_id)
        zk_client.watch_job_model(job_model_watch(event = event, expected_processors=expected_processors))

        event.wait(GROUP_COORDINATION_TIMEOUT * 2)

        job_model = zk_client.get_latest_job_model()

        ## Verifications after leader was resumed.
        assert leader_processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(leader_processor_id, job_model['containers'])
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        leader.kill()
    except:
        ## Explicitly logging exception, since zopkio doesn't log complete stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __tear_down_processors(processors)
        __purge_zk_data()
        __teardown_zk_client()
