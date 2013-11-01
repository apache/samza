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

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

if [ ! -d "$base_dir/lib" ]; then
  echo "Unable to find $base_dir/lib, which is required to run."
  exit 1
fi

CLASSPATH=$YARN_HOME/conf

for file in $base_dir/lib/*.[jw]ar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

if [ -z "$SAMZA_LOG_DIR" ]; then
  SAMZA_LOG_DIR="$base_dir"
fi

if [ -z "$SAMZA_CONTAINER_NAME" ]; then
  SAMZA_CONTAINER_NAME="undefined-samza-container-name"
fi

if [ -z "$SAMZA_OPTS" ]; then
  SAMZA_OPTS="-Xmx160M -XX:+PrintGCDateStamps -Xloggc:$SAMZA_LOG_DIR/gc.log -Dsamza.log.dir=$SAMZA_LOG_DIR -Dsamza.container.name=$SAMZA_CONTAINER_NAME"
  if [ -f $base_dir/lib/log4j.xml ]; then
    SAMZA_OPTS="$SAMZA_OPTS -Dlog4j.configuration=file:$base_dir/lib/log4j.xml"
  fi
fi

echo $JAVA $SAMZA_OPTS -cp $CLASSPATH $@
exec $JAVA $SAMZA_OPTS -cp $CLASSPATH $@
