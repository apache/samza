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
GC_LOG_ROTATION_OPTS="-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10241024"
LOG4J_FILE_NAME="log4j.xml"
LOG4J2_FILE_NAME="log4j2.xml"
BASE_LIB_DIR="$base_dir/lib"
DEFAULT_LOG4J_FILE=$BASE_LIB_DIR/$LOG4J_FILE_NAME
DEFAULT_LOG4J2_FILE=$BASE_LIB_DIR/$LOG4J2_FILE_NAME

# APPLICATION_LIB_DIR can be a directory which is different from $BASE_LIB_DIR which contains some additional
# application-specific resources. If it is not set, then $BASE_LIB_DIR will be used as the value.
APPLICATION_LIB_DIR="${APPLICATION_LIB_DIR:-$BASE_LIB_DIR}"
export APPLICATION_LIB_DIR=$APPLICATION_LIB_DIR

echo APPLICATION_LIB_DIR=$APPLICATION_LIB_DIR
echo BASE_LIB_DIR=$BASE_LIB_DIR

BASE_LIB_CLASSPATH=""
# all the jars need to be appended on newlines to ensure line argument length of 72 bytes is not violated
for file in $BASE_LIB_DIR/*.[jw]ar;
do
  BASE_LIB_CLASSPATH=$BASE_LIB_CLASSPATH" $file \n"
done
echo generated from BASE_LIB_DIR BASE_LIB_CLASSPATH=$BASE_LIB_CLASSPATH

# In some cases (AWS) $JAVA_HOME/bin doesn't contain jar.
if [ -z "$JAVA_HOME" ] || [ ! -e "$JAVA_HOME/bin/jar" ]; then
  JAR="jar"
else
  JAR="$JAVA_HOME/bin/jar"
fi

# Create a pathing JAR for the JARs in the BASE_LIB_DIR
# Newlines and spaces are intended to ensure proper parsing of manifest in pathing jar
printf "Class-Path: \n $BASE_LIB_CLASSPATH \n" > base-lib-manifest.txt
# Creates a new archive and adds custom manifest information to base-lib-pathing.jar
eval "$JAR -cvmf base-lib-manifest.txt base-lib-pathing.jar"

# Create a pathing JAR for the runtime framework resources. It is useful to separate this from the base-lib-pathing.jar
# because the split deployment framework may only need the resources from this runtime pathing JAR.
if ! [[ $HADOOP_CONF_DIR =~ .*/$ ]]; then
  # manifest requires a directory to have a trailing slash
  HADOOP_CONF_DIR="$HADOOP_CONF_DIR/"
fi
# HADOOP_CONF_DIR should be supplied to classpath explicitly for Yarn to parse configs
RUNTIME_FRAMEWORK_RESOURCES_CLASSPATH="$HADOOP_CONF_DIR \n"
# TODO add JARs from ADDITIONAL_CLASSPATH_DIR to runtime-framework-resources-pathing.jar as well
printf "Class-Path: \n $RUNTIME_FRAMEWORK_RESOURCES_CLASSPATH \n" > runtime-framework-resources-manifest.txt
eval "$JAR -cvmf runtime-framework-resources-manifest.txt runtime-framework-resources-pathing.jar"

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

# Make the MDC inheritable to child threads by setting the system property to true if config not explicitly specified
[[ $JAVA_OPTS != *-DisThreadContextMapInheritable* ]] && JAVA_OPTS="$JAVA_OPTS -DisThreadContextMapInheritable=true"

# Check if log4j configuration is specified; if not, look for a configuration file:
# 1) Check if using log4j or log4j2
# 2) Check if configuration file system property is already set
# 3) If not, then look in $APPLICATION_LIB_DIR for configuration file (remember that $APPLICATION_LIB_DIR can be same or
#    different from $BASE_LIB_DIR).
# 4) If still can't find it, fall back to default (from $BASE_LIB_DIR).
if [[ -n $(find "$BASE_LIB_DIR" -regex ".*samza-log4j2.*.jar*") ]]; then
  if [[ $JAVA_OPTS != *-Dlog4j.configurationFile* ]]; then
    if [[ -n $(find "$APPLICATION_LIB_DIR" -maxdepth 1 -name $LOG4J2_FILE_NAME) ]]; then
      export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configurationFile=file:$APPLICATION_LIB_DIR/$LOG4J2_FILE_NAME"
    else
      export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configurationFile=file:$DEFAULT_LOG4J2_FILE"
    fi
  fi
elif [[ -n $(find "$BASE_LIB_DIR" -regex ".*samza-log4j.*.jar*") ]]; then
  if [[ $JAVA_OPTS != *-Dlog4j.configuration* ]]; then
    if [[ -n $(find "$APPLICATION_LIB_DIR" -maxdepth 1 -name $LOG4J_FILE_NAME) ]]; then
      export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$APPLICATION_LIB_DIR/$LOG4J_FILE_NAME"
    else
      export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$DEFAULT_LOG4J_FILE"
    fi
  fi
fi

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

echo $JAVA $JAVA_OPTS -cp base-lib-pathing.jar:runtime-framework-resources-pathing.jar "$@"

## If localized resource lib directory is defined, then include it in the classpath.
if [[ -z "${ADDITIONAL_CLASSPATH_DIR}" ]]; then
  exec $JAVA $JAVA_OPTS -cp base-lib-pathing.jar:runtime-framework-resources-pathing.jar "$@"
else
  exec $JAVA $JAVA_OPTS -cp base-lib-pathing.jar:runtime-framework-resources-pathing.jar:$ADDITIONAL_CLASSPATH_DIR "$@"
fi