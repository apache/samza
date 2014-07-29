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
DOCS_DIR=$BASE_DIR/docs
VERSION=$1
COMMENT=$2
USER=$3

if test -z "$VERSION" || test -z "$COMMENT" || test -z "$USER"; then
  echo
  echo "  USAGE:"
  echo
  echo "    ${BASH_SOURCE[0]##*/} 0.7.0 \"updating welcome page\" criccomini"
  echo
  exit 0
fi

echo "Using uer: $USER"
echo "Using version: $VERSION"
echo "Using comment: $COMMENT"
echo "Generating javadocs."
$BASE_DIR/bin/generate-javadocs.sh $VERSION

echo "Building site."
cd $DOCS_DIR
bundle exec jekyll build

echo "Checking out SVN site."
SVN_TMP=`mktemp -d /tmp/samza-svn.XXXX`
svn co https://svn.apache.org/repos/asf/incubator/samza/ $SVN_TMP
cp -r _site/* $SVN_TMP/site/
svn add --force $SVN_TMP/site
svn commit $SVN_TMP -m"$COMMENT" --username $USER
rm -rf $SVN_TMP
