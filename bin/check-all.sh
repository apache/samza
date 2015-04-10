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

# Run the gradlew check task against all supported JDKs and Scala versions.
# Requires appropriate JAVAx_HOME variables to be set for Java 6, 7 and 8.

set -e

SCALAs=( "2.10" )
JDKs=( "JAVA7_HOME" "JAVA8_HOME" )
YARNs=( "2.4.0" "2.5.0" )

# get base directory
home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

# validate JDKs, and default when appropriate
any_not_set=false
for i in "${JDKs[@]}"
do
  if [ -z "${!i}" ]; then
    echo "${i} is not set."

    # if java_home is available (usually OSX), then use it to find JAVA_HOME
    if [ -f /usr/libexec/java_home ]; then
      jdk_number=${i:4:1}
      jdk_version=$(/usr/libexec/java_home -v 1.${jdk_number})
      eval "${i}=${jdk_version}";
      echo "${i} defaulted to ${!i}"
    else
      any_not_set=true
    fi
  fi
done

if [ "$any_not_set" = "true" ]; then
  echo "Be sure that all necessary JAVA home variables are set and re-run."
  exit 0
fi

# run all checks
for i in "${JDKs[@]}"
do
  for scala_version in "${SCALAs[@]}"
  do
    jdk_number=${i:4:1}

    for yarn_version in "${YARNs[@]}"
    do
      echo "------------- Running check task against JDK${jdk_number}/Scala ${scala_version}/YARN ${yarn_version}"
      $base_dir/gradlew -PscalaVersion=${scala_version} -PyarnVersion=${yarn_version} -Dorg.gradle.java.home=${!i} clean check $@
      echo "------------- Finished running check task against JDK${jdk_number}/Scala ${scala_version}/YARN ${yarn_version}"
    done
  done
done
