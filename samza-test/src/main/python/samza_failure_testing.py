#!/usr/bin/env python

#################################################################################################################################
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
#################################################################################################################################

import sys
import paramiko
import os.path
import random
import requests
import logging
from time import sleep
from optparse import OptionParser

#################################################################################################################################
# A "chaos monkey"-like script that periodically kills parts of Samza, including YARN (RM, NM), Kafka, and Samza (AM, container)
# This script depends on paramiko, an ssh library
#################################################################################################################################

# Create an ssh connection to the given host
def connect(host):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    client.connect(host)
    return client

# Run the given command line using the ssh connection (throw an error if anything on stderr)
def execute(conn, cmd):
    logging.info("Executing command {0}".format(cmd))
    stdin, stdout, stderr = conn.exec_command(cmd)
    output = stdout.read()
    err = stderr.read()
    if output:
        logging.info(output)
        return output
    if err:
        logging.error(err)
        raise Exception("Error executing command: %s" %err)

# Unfortunately pkill on mac seems to have a length limit which prevents it working with out epic java command line arguments
def pkill(pid):
    return "kill {0}".format(pid)

def get_pid(pattern):
    return "ps aux | grep " + pattern + " | grep -v 'grep' | awk -F' *' '{print $2}'"

# Kill the kafka broker
def kill_kafka(options):
    connection = connect(options.kafka_host)
    logging.info("Killing Kafka Broker ...")
    kafka_pid = execute(connection, get_pid("kafka.Kafka"))
    if kafka_pid is not None:
        execute(connection, pkill(kafka_pid))
        sleep(20)
        logging.info("Restarting Kafka Broker...")
        execute(connection, "nohup " + options.kafka_dir + "/bin/kafka-server-start.sh " + options.kafka_dir + "/config/server.properties > " + options.kafka_dir + "/logs/kafka.log 2>&1 &")
        connection.close()
    else:
        logging.info("Could not determine Kafka broker process. Not doing anything")


def query_yarn(host, port, query):
    return requests.get("http://{0}:{1}/{2}".format(host, port, query))

def get_app_ids_running(host, port):
    logging.info("Querying RM for RUNNING application information")
    list_apps_command = "ws/v1/cluster/apps?status=RUNNING"
    response = query_yarn(host, port, list_apps_command).json()
    if len(response) == 0 or not response['apps']:
      raise Exception("Got an empty apps response back. Can't run kill script without Samza jobs running.")
    apps = reduce(list.__add__, map(lambda x: list(x), response['apps'].values()))
    appInfo = []
    for app in apps:
        appInfo.append((app['id'], app['name']))
    return appInfo

# Kill the samza container instances
def kill_containers(hosts, app_id):
    for host in hosts:
        connection = connect(host)
        pid = execute(connection, get_pid("samza-container-0 | grep {0}".format(app_id)))
        if pid:
            logging.info("Killing samza container on {0} with pid {1}".format(host, pid))
            execute(connection, pkill(pid))
        connection.close()
    if pid is None:
        logging.info("Couldn't find any container on the list of hosts. Nothing to kill :(")

def require_arg(options, name):
    if not hasattr(options, name) or getattr(options, name) is None:
        print >> sys.stderr, "Missing required property:", name
        sys.exit(1)

# Command line options
parser = OptionParser()
parser.add_option("--node-list", dest="filename", help="A list of nodes in the YARN cluster", metavar="nodes.txt")
parser.add_option("--kill-time", dest="kill_time", help="The time in seconds to sleep between", metavar="s", default=90)
parser.add_option("--kafka-dir", dest="kafka_dir", help="The directory in which to find kafka", metavar="dir")
parser.add_option("--kafka-host", dest="kafka_host", help="Host on which Kafka is installed", metavar="localhost")
parser.add_option("--yarn-dir", dest="yarn_dir", help="The directory in which to find yarn", metavar="dir")
parser.add_option("--kill-kafka", action="store_true", dest="kill_kafka", default=False, help="Should we kill Kafka?")
parser.add_option("--kill-container", action="store_true", dest="kill_container", default=False, help="Should we kill Application Container?")
parser.add_option("--yarn-host", dest="yarn_host", help="Host that will respond to Yarn REST queries ", metavar="localhost")

(options, args) = parser.parse_args()

kill_script_log_path = '/tmp/samza-kill-log.log'
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S', filename=kill_script_log_path, level=logging.INFO)
console = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

components = []
app_info = {}
if options.kill_container:
    require_arg(options, 'filename')
    require_arg(options, 'yarn_host')
    require_arg(options, 'yarn_dir')
    components.append("container")
    hosts = [line.strip() for line in open(options.filename).readlines()]
    if len(hosts) < 1:
        print >> sys.stderr, "No hosts in host file."
        sys.exit(1)
    app_info = get_app_ids_running(options.yarn_host, 8088)

if options.kill_kafka:
    require_arg(options, 'kafka_host')
    require_arg(options, 'kafka_dir')
    components.append("kafka")

if len(components) == 0:
    parser.print_help()
else:
    while True:
        kill_time = int(options.kill_time)
        component_id = 0
        if len(components) > 1:
            component_id = random.randint(0, len(components) - 1)
        kill_component = components[component_id]
        if kill_component == "kafka":
            logging.info("Choosing Kafka broker on {0}".format(options.kafka_host))
            kill_kafka(options)
            logging.info("Sleeping for {0}".format(kill_time * 2))
            sleep(kill_time * 2)
        elif kill_component == "container":
            app_id_to_kill = random.randint(0, len(app_info) - 1)
            logging.info("Choosing a Samza Container for {0}".format(app_info[app_id_to_kill]))
            kill_containers(hosts, app_info[app_id_to_kill][0])
            logging.info("Sleeping for {0}".format(kill_time))
            sleep(kill_time)
