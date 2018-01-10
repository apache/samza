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

# get the version number
version=`cat _config.yml | grep 'version:' | cut -d' ' -f 2 | sed -e "s/'//g"`
latestRelease=`cat _config.yml | grep 'latest-release:' | cut -d' ' -f 2 | sed -e "s/'//g"`
DIR=$(dirname $0)/..

if test -z "$version" || test -z "$latestRelease"; then
  echo "Please make sure _config.yml has \"version\" and \"latest-release\""
  exit 0
fi

echo "version:" $version

echo "replaced img/versioned to img/"$version
mv -f $DIR/_site/img/versioned $DIR/_site/img/$version

echo "replaced learn/documentation/versioned to learn/documentation/"$version
mv -f $DIR/_site/learn/documentation/versioned $DIR/_site/learn/documentation/$version

echo "replaced learn/tutorials/versioned to learn/tutorials/"$version
mv -f $DIR/_site/learn/tutorials/versioned $DIR/_site/learn/tutorials/$version

echo "replaced learn/releases/versioned to learn/releases/"$version
mv -f $DIR/_site/startup/releases/versioned $DIR/_site/startup/releases/$version

echo "replaced startup/hello-samza/versioned to startup/hello-samza/"$version
mv -f $DIR/_site/startup/hello-samza/versioned $DIR/_site/startup/hello-samza/$version
