#!/bin/bash
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

home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

export EXECUTION_PLAN_DIR="$base_dir/plan"
mkdir -p $EXECUTION_PLAN_DIR

[[ $JAVA_OPTS != *-Dlog4j.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$(dirname $0)/log4j-console.xml"

exec $(dirname $0)/run-class.sh samza.examples.azure.AzureZKLocalApplication --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/azure-application-local-runner.properties
