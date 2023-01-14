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
DOAP_FILE=$BASE_DIR/doap_Samza.rdf
COMMENT=$1
USER=$2

if test -z "$COMMENT" || test -z "$USER"; then
  echo
  echo "  USAGE:"
  echo
  echo "    ${BASH_SOURCE[0]##*/} \"updating welcome page\" criccomini"
  echo
  exit 0
fi

echo "Using user: $USER"
echo "Using comment: $COMMENT"
echo "Generating javadocs."
$BASE_DIR/bin/generate-javadocs.sh

echo "Building site."
cd "$BASE_DIR"
./gradlew docs:jekyllbuild

cd "$DOCS_DIR"
echo "Replacing version"
./_docs/replace-versioned.sh

echo "Checking out SVN site."
SVN_TMP=`mktemp -d /tmp/samza-svn.XXXX`
svn co https://svn.apache.org/repos/asf/samza/ $SVN_TMP
cp -r _site/* $SVN_TMP/site/
cp $DOAP_FILE $SVN_TMP/site/
svn add --force $SVN_TMP/site
svn commit $SVN_TMP -m"$COMMENT" --username $USER
rm -rf $SVN_TMP
