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

# For Managed Beam Workflow type jobs, user jar and log4j2.xml will be placed under __userPackage/lib.
# If __userPackage/lib exists, set APPLICATION_LIB_DIR to __userPackage/lib
# TODO - use parameter like job type (WORKFLOW_DSL) to decide whether there is a separate user lib path
if [ -d "$home_dir/__userPackage/lib" ]; then
  APPLICATION_LIB_DIR="$home_dir/__userPackage/lib"
  echo APPLICATION_LIB_DIR=$APPLICATION_LIB_DIR
fi

# APPLICATION_LIB_DIR can be a directory which is different from $BASE_LIB_DIR which contains some additional
# application-specific resources. If it is not set, then $BASE_LIB_DIR will be used as the value.
APPLICATION_LIB_DIR="${APPLICATION_LIB_DIR:-$BASE_LIB_DIR}"
export APPLICATION_LIB_DIR=$APPLICATION_LIB_DIR

echo APPLICATION_LIB_DIR=$APPLICATION_LIB_DIR
echo BASE_LIB_DIR=$BASE_LIB_DIR

CLASSPATH=""

# This is LinkedIn Hadoop cluster specific dependency! The jar file is needed
# for the Samza job to run on LinkedIn's Hadoop YARN cluster.
# There is no clean way to include this dependency anywhere else, so we just
# manually include it here.
# Long term fix: make Hadoop YARN cluster officially support Samza job and prepare
# runtime dependency for us.
#
if [ -e /export/apps/hadoop/site/lib/grid-topology-1.0.jar ]; then
  CLASSPATH=$CLASSPATH" /export/apps/hadoop/site/lib/grid-topology-1.0.jar \n"
fi

# all the jars need to be appended on newlines to ensure line argument length of 72 bytes is not violated
for file in $BASE_LIB_DIR/*.[jw]ar;
do
  CLASSPATH=$CLASSPATH" $file \n"
done
echo generated from BASE_LIB_DIR CLASSPATH=$CLASSPATH

# when APPLICATION_LIB_DIR is different from BASE_LIB_DIR, meaning it is a Managed Beam Workflow job, append
# user jars in __userPackage/lib to the classpath
# TODO - In the initial version of Managed Beam Workflow job, it's ensured that no extra jars are supplied from the
# user job besides the user jar that only contains the pipeline proto file and non-jar resources. When simple UDF
# support is to be added, we need to ensure no jar or class collision happens while combining user jars with framework jars
# on classpath
USER_CLASSPATH=""
if [ "$APPLICATION_LIB_DIR" != "$BASE_LIB_DIR" ]; then
  for file in $APPLICATION_LIB_DIR/*.[jw]ar;
  do
    USER_CLASSPATH=$USER_CLASSPATH" $file \n"
  done
  echo generated from $APPLICATION_LIB_DIR USER_CLASSPATH=$USER_CLASSPATH
fi

# In some cases (AWS) $JAVA_HOME/bin doesn't contain jar.
if [ -z "$JAVA_HOME" ] || [ ! -e "$JAVA_HOME/bin/jar" ]; then
  JAR="jar"
else
  JAR="$JAVA_HOME/bin/jar"
fi

# Create a separate directory for writing files related to classpath management. It is easier to manage
# permissions for the classpath-related files when they are in their own directory. An example of where
# this is helpful is when using container images which might have predefined permissions for certain
# directories.
CLASSPATH_WORKSPACE_DIR=$base_dir/classpath_workspace
mkdir -p $CLASSPATH_WORKSPACE_DIR
# file containing the classpath string; used to avoid passing long classpaths directly to the jar command
PATHING_MANIFEST_FILE=$CLASSPATH_WORKSPACE_DIR/manifest.txt
# jar file to include on the classpath for running the main class
PATHING_JAR_FILE=$CLASSPATH_WORKSPACE_DIR/pathing.jar

# Newlines and spaces are intended to ensure proper parsing of manifest in pathing jar
printf "Class-Path: \n $CLASSPATH \n" > $PATHING_MANIFEST_FILE
# Creates a new archive and adds custom manifest information to pathing.jar
eval "$JAR -cvmf $PATHING_MANIFEST_FILE $PATHING_JAR_FILE"

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
      # LI-ONLY CONFIG: Used to set log4j2 configurations for util-log (xeril) to be printed to the same .log file
      [[ $JAVA_OPTS != *-Dlog4j2.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j2.configuration=file:$APPLICATION_LIB_DIR/$LOG4J2_FILE_NAME"
    else
      export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configurationFile=file:$DEFAULT_LOG4J2_FILE"
      # LI-ONLY CONFIG: Used to set log4j2 configurations for util-log (xeril) to be printed to the same .log file
      [[ $JAVA_OPTS != *-Dlog4j2.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j2.configuration=file:$DEFAULT_LOG4J2_FILE"
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

# Linkedin-specific: Add JVM option to guarantee exit on OOM
JAVA_OPTS="${JAVA_OPTS} -XX:+ExitOnOutOfMemoryError"

# Linkedin-specific: Add pre-installed boot jars to JVM boot class path, e.g. support Jetty Http2 communication by ALPN
function setup_boot_jars_in_jvm_boot_classpath() {
  # Get and format java version to follow SI's convention, e.g. 1.8.0_172 to 1_8_0_172
  RAW_JAVA_VERSION=$($JAVA -version 2>&1 | awk -F '"' '/version/ {print $2}')
  FORMATTED_JAVA_VERSION=${RAW_JAVA_VERSION//./_}

  # The boot jars are pre-installed in different paths for Mac OS and Linux OS
  if [[ "$OSTYPE" == "darwin"* ]]; then
    BOOT_JAR_HOME_DIR="/Library/Java/Boot/${FORMATTED_JAVA_VERSION}/"
  else
    BOOT_JAR_HOME_DIR="/export/apps/jdkboot/${FORMATTED_JAVA_VERSION}/"
  fi

  # Add all the files under boot jar home dir in path separated with colons
  BOOT_JAR_PATHS=""
  if [ -d "${BOOT_JAR_HOME_DIR}" ]; then
    for entry in "$BOOT_JAR_HOME_DIR"*
    do
      BOOT_JAR_PATHS="${BOOT_JAR_PATHS}${entry}:"
    done
  else
    echo "No boot jar is pre-installed under dir: ${BOOT_JAR_HOME_DIR}"
  fi
  if [ -z "$BOOT_JAR_PATHS" ]; then
    return
  fi
  # Remove last colon
  BOOT_JAR_PATHS="$(echo $BOOT_JAR_PATHS | sed 's/.$//')"

  JVM_BOOT_CLASSPATH_CONFIG_KEY="-Xbootclasspath"
  # If "-Xbootclasspath" is already setup in JAVA_OPTS, rebuild the value with found boot jar paths
  if [[ "$JAVA_OPTS" =~ "$JVM_BOOT_CLASSPATH_CONFIG_KEY" ]]; then
    NEW_JAVA_OPTS=""
    for option in $JAVA_OPTS
    do
      if [[ "$option" == "${JVM_BOOT_CLASSPATH_CONFIG_KEY}"* ]]; then
        JVM_BOOT_CLASSPATH="${option}:${BOOT_JAR_PATHS}"
      else
        NEW_JAVA_OPTS="${NEW_JAVA_OPTS} $option"
      fi
    done
    JAVA_OPTS="${NEW_JAVA_OPTS}"
  else
    JVM_BOOT_CLASSPATH="${JVM_BOOT_CLASSPATH_CONFIG_KEY}/p:${BOOT_JAR_PATHS}"
  fi

  JAVA_OPTS="${JAVA_OPTS} ${JVM_BOOT_CLASSPATH}"
}
setup_boot_jars_in_jvm_boot_classpath

# HADOOP_CONF_DIR should be supplied to classpath explicitly for Yarn to parse configs
echo $JAVA $JAVA_OPTS -cp $HADOOP_CONF_DIR:$PATHING_JAR_FILE "$@"

## If localized resource lib directory is defined, then include it in the classpath.
if [[ -z "${ADDITIONAL_CLASSPATH_DIR}" ]]; then
  # LI-specific: Adding option to invoke script on OOM here because adding it in JAVA_OPTS causes encoding issues https://stackoverflow.com/questions/12532051/xxonoutofmemoryerror-cmd-arg-gives-error-could-not-find-or-load-main-c
  exec $JAVA $JAVA_OPTS -XX:OnOutOfMemoryError="$BASE_LIB_DIR/../bin/handle-oom.sh $SAMZA_LOG_DIR" -cp $HADOOP_CONF_DIR:$PATHING_JAR_FILE "$@"
else
  # LI-specific: Adding option to invoke script on OOM here because adding it in JAVA_OPTS causes encoding issues https://stackoverflow.com/questions/12532051/xxonoutofmemoryerror-cmd-arg-gives-error-could-not-find-or-load-main-c
  exec $JAVA $JAVA_OPTS -XX:OnOutOfMemoryError="$BASE_LIB_DIR/../bin/handle-oom.sh $SAMZA_LOG_DIR" -cp $HADOOP_CONF_DIR:$PATHING_JAR_FILE:$ADDITIONAL_CLASSPATH_DIR "$@"
fi
