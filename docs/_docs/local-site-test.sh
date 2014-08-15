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

# The goal of this script is to make testing the site locally easier.
# It downloads old pages from SVN and replaces with/add new pages.

DIR=$(dirname $0)/..
cd $DIR

echo "downloading SVN..."
SVN_TMP=`mktemp -d /tmp/samza-svn.XXXX`
svn co https://svn.apache.org/repos/asf/incubator/samza/ $SVN_TMP
cp -r _site/* $SVN_TMP/site/
cp -r $SVN_TMP/site/* _site
rm -rf $SVN_TMP

# replace version number
echo "replacing version number..."
_docs/replace-versioned.sh