#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

set -e

echo "Is PR? ${TRAVIS_PULL_REQUEST}"
echo "Branch: ${TRAVIS_BRANCH}"

if [ "${TRAVIS_PULL_REQUEST}" = "false" ]; then
    if [ "${TRAVIS_BRANCH}" = "master" ]; then
        echo "Building with code analysis"
	    ./gradlew clean test jacocoTestReport reportScoverage aggregateScoverage
	    exit 0
    fi
fi
echo "Building without code analysis"
./gradlew clean test
