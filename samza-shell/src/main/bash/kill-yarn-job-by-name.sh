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

if [[ -n $(find "$base_dir/lib" -regex ".*samza-log4j2.*.jar*") ]]; then
    [[ $JAVA_OPTS != *-Dlog4j.configurationFile* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configurationFile=file:$(dirname $0)/log4j2-console.xml"
elif [[ -n $(find "$base_dir/lib" -regex ".*samza-log4j.*.jar*") ]]; then
    [[ $JAVA_OPTS != *-Dlog4j.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$(dirname $0)/log4j-console.xml"
fi

# Get the id for the app with the specified name that is also ACCEPTED or RUNNING status
APP_ID=$(exec "$(dirname $0)"/run-class.sh org.apache.hadoop.yarn.client.cli.ApplicationCLI application -list | grep "[[:space:]]$1[[:space:]]" | grep "application_" | awk -F ' ' '{ print $1 }')
echo "Job name ($1) matched app IDs: ($APP_ID)"

# If the app id was not found, it either doesn't exist or was already stopped.
if [ -z "$APP_ID" ];
then
  exit 0
fi

# Verify that only one application matches
COUNT=$(echo "$APP_ID" | wc -l)
if [ $COUNT -gt 1 ];
then
  exit 150
fi

# Kill the job and check the return code
"$(dirname $0)"/kill-yarn-job.sh "$APP_ID"
