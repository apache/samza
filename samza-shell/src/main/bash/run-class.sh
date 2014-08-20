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

HADOOP_YARN_HOME="${HADOOP_YARN_HOME:-$HOME/.samza}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_YARN_HOME/conf}"
CLASSPATH=$HADOOP_CONF_DIR
GC_LOG_ROTATION_OPTS="-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10241024"

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

# add usercache directory
mkdir -p $base_dir/tmp
JAVA_TEMP_DIR=$base_dir/tmp

# Check whether the JVM supports GC Log rotation, and enable it if so.
function enable_gc_log_rotation {
  `$JAVA -Xloggc:/dev/null $GC_LOG_ROTATION_OPTS -version 2> /dev/null`
  if [ $? -eq 0 ] ; then
    JAVA_OPTS="$JAVA_OPTS $GC_LOG_ROTATION_OPTS"
  fi
}

# Try and use 64-bit mode if available in JVM_OPTS
function check_and_enable_64_bit_mode {
  `$JAVA -d64 -version`
  if [ $? -eq 0 ] ; then
    JAVA_OPTS="$JAVA_OPTS -d64"
  fi
}

# Initialize SAMZA_LOG4J_CONFIG if currently undefined
if [ -z "$SAMZA_LOG4J_CONFIG" ]; then
  export SAMZA_LOG4J_CONFIG=file:$base_dir/lib/log4j.xml
fi

### Initialize JVM OPTS ###

# If JAVA_OPTS is not specified in job properties (task.opts), initialize to default
if [ -z "$JAVA_OPTS" ]; then
  # Enable GC related flags
  JAVA_OPTS="-Xmx768M -XX:+PrintGCDateStamps -Xloggc:$SAMZA_LOG_DIR/gc.log"

  # Enable GC log rotation by default
  enable_gc_log_rotation
else
  # Otherwise, check if the GC related flags are specified. If not - add the respective flags to JVM_OPTS.
  [[ $JAVA_OPTS != *PrintGCDateStamps* && $JAVA_OPTS != *loggc* ]] && JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDateStamps -Xloggc:$SAMZA_LOG_DIR/gc.log"

  # Also check if GC log rotation is already enabled. If not - add the respective flags to JVM_OPTS
  [[ $JAVA_OPTS != *UseGCLogFileRotation* ]] && enable_gc_log_rotation
fi

check_and_enable_64_bit_mode
JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=$SAMZA_LOG4J_CONFIG -Dsamza.log.dir=$SAMZA_LOG_DIR -Dsamza.container.name=$SAMZA_CONTAINER_NAME -Djava.io.tmpdir=$JAVA_TEMP_DIR"

echo $JAVA $JAVA_OPTS -cp $CLASSPATH $@
exec $JAVA $JAVA_OPTS -cp $CLASSPATH $@
