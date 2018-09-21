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

if [ `uname` == 'Linux' ];
then
  base_dir=$(readlink -f $(dirname $0))
else
  base_dir=$(dirname $0)
fi

parent_dir="$(dirname "$base_dir")"

CONF_FILE="-conf $parent_dir/conf/shell-defaults.conf"

if [ "x$LOG_HOME" = "x" ]; then
    export LOG_HOME=$parent_dir
fi

if [ "x$LOG4J_OPTS" = "x" ]; then
    export LOG4J_OPTS="-Dlog4j.configuration=file:$parent_dir/conf/samza-sql-shell-log4j.xml"
fi

if [ "x$HEAP_OPTS" = "x" ]; then
    export HEAP_OPTS="-Xmx4G -Xms4G"
fi

exec $base_dir/run-class.sh $LOG4J_OPTS -DLOG_HOME=$LOG_HOME org.apache.samza.sql.client.cli.Main $CONF_FILE "$@"
