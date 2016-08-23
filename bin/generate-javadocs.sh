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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$DIR/..
API_JAVADOC_DIR=$BASE_DIR/docs/learn/documentation/versioned/api/javadocs
REST_JAVADOC_DIR=$BASE_DIR/docs/learn/documentation/versioned/rest/javadocs

cd $BASE_DIR
./gradlew javadoc

# API Docs
rm -rf $API_JAVADOC_DIR
mkdir -p $API_JAVADOC_DIR
cp -r $BASE_DIR/samza-api/build/docs/javadoc/* $API_JAVADOC_DIR

# REST Docs
rm -rf $REST_JAVADOC_DIR
mkdir -p $REST_JAVADOC_DIR
cp -r $BASE_DIR/samza-rest/build/docs/javadoc/* $REST_JAVADOC_DIR
cd -
