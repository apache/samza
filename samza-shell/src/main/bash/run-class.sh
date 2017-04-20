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

echo home_dir=$home_dir
echo "framework base (location of this script). base_dir=$base_dir"

if [ ! -d "$base_dir/lib" ]; then
  echo "Unable to find $base_dir/lib, which is required to run."
  exit 1
fi

HADOOP_YARN_HOME="${HADOOP_YARN_HOME:-$HOME/.samza}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_YARN_HOME/conf}"
CLASSPATH=$HADOOP_CONF_DIR
GC_LOG_ROTATION_OPTS="-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10241024"
DEFAULT_LOG4J_FILE=$base_dir/lib/log4j.xml
BASE_LIB_DIR="$base_dir/lib"
# JOB_LIB_DIR will be set for yarn container in ContainerUtil.java
# for others we set it to home_dir/lib
JOB_LIB_DIR="${JOB_LIB_DIR:-$home_dir/lib}"

export JOB_LIB_DIR=$JOB_LIB_DIR

if [ -d "$JOB_LIB_DIR" ] && [ "$JOB_LIB_DIR" != "$BASE_LIB_DIR" ]; then
  # build a common classpath
  # this class path will contain all the jars from the framework and the job's libs.
  # in case of different version of the same lib - we pick the highest

  #all jars from the fwk
  base_jars=`ls $BASE_LIB_DIR/*.[jw]ar`
  #all jars from the job
  job_jars=`for file in $JOB_LIB_DIR/*.[jw]ar; do name=\`basename $file\`; if [[ $base_jars != *"$name"* ]]; then echo "$file"; fi; done`
  # get all lib jars and reverse sort it by versions
  all_jars=`for file in $base_jars $job_jars; do echo \`basename $file|sed 's/.*[-]\([0-9]\+\..*\)[jw]ar$/\1/'\` $file; done|sort -t. -k 1,1nr -k 2,2nr -k 3,3nr -k 4,4nr|awk '{print $2}'`
  # generate the class path based on the sorted result
  for jar in $all_jars; do CLASSPATH=$CLASSPATH:$jar; done

  # for debug only
  echo base_jars=$base_jars
  echo job_jars=$job_jars
  echo all_jars=$all_jars
  echo generated combined CLASSPATH=$CLASSPATH
else
  #default behavior
  for file in $BASE_LIB_DIR/*.[jw]ar;
  do
    CLASSPATH=$CLASSPATH:$file
  done
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

if [ -z "$SAMZA_LOG_DIR" ]; then
  SAMZA_LOG_DIR="$base_dir"
fi

# add usercache directory
mkdir -p $base_dir/tmp
JAVA_TEMP_DIR=$base_dir/tmp

# Check whether the JVM supports GC Log rotation, and enable it if so.
function check_and_enable_gc_log_rotation {
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

### Inherit JVM_OPTS from task.opts configuration, and initialize defaults ###

# Check if log4j configuration is specified. If not - set to lib/log4j.xml
[[ $JAVA_OPTS != *-Dlog4j.configuration* && -f $DEFAULT_LOG4J_FILE ]] && JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$DEFAULT_LOG4J_FILE"

# Check if samza.log.dir is specified. If not - set to environment variable if it is set
[[ $JAVA_OPTS != *-Dsamza.log.dir* && ! -z "$SAMZA_LOG_DIR" ]] && JAVA_OPTS="$JAVA_OPTS -Dsamza.log.dir=$SAMZA_LOG_DIR"

# Check if java.io.tmpdir is specified. If not - set to tmp in the base_dir
[[ $JAVA_OPTS != *-Djava.io.tmpdir* ]] && JAVA_OPTS="$JAVA_OPTS -Djava.io.tmpdir=$JAVA_TEMP_DIR"

# Check if a max-heap size is specified. If not - set a 768M heap
[[ $JAVA_OPTS != *-Xmx* ]] && JAVA_OPTS="$JAVA_OPTS -Xmx768M"

# Check if the GC related flags are specified. If not - add the respective flags to JVM_OPTS.
[[ $JAVA_OPTS != *PrintGCDateStamps* && $JAVA_OPTS != *-Xloggc* ]] && JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDateStamps -Xloggc:$SAMZA_LOG_DIR/gc.log"

# Check if GC log rotation is already enabled. If not - add the respective flags to JVM_OPTS
[[ $JAVA_OPTS != *UseGCLogFileRotation* ]] && check_and_enable_gc_log_rotation

# Check if 64 bit is set. If not - try and set it if it's supported
[[ $JAVA_OPTS != *-d64* ]] && check_and_enable_64_bit_mode

echo $JAVA $JAVA_OPTS -cp $CLASSPATH "$@"
exec $JAVA $JAVA_OPTS -cp $CLASSPATH "$@"
