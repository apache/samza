#!/bin/bash -e
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
TEST_DIR=$1

if test -z "$TEST_DIR"; then
  echo
  echo "  USAGE:"
  echo
  echo "    ${BASH_SOURCE[0]##*/} \"<dirname to run tests in>\" [zopkio args...]"
  echo
  exit 0
fi

# always use absolute paths for ABS_TEST_DIR
ABS_TEST_DIR=$(cd $(dirname $TEST_DIR); pwd)/$(basename $TEST_DIR)
SCRIPTS_DIR=$ABS_TEST_DIR/scripts

# safety check for virtualenv
if [ -f $HOME/.pydistutils.cfg ]; then
  echo "Virtualenv can't run while $HOME/.pydistutils.cfg exists."
  echo "Please remove $HOME/.pydistutils.cfg, and try again."
  exit 0
fi

# build integration test tarball
./gradlew releaseTestJobs

# create integration test directory
mkdir -p $ABS_TEST_DIR
rm -rf $SCRIPTS_DIR
cp -r samza-test/src/main/python/ $SCRIPTS_DIR
cp ./samza-test/build/distributions/samza-test*.tgz $ABS_TEST_DIR
cd $ABS_TEST_DIR

# setup virtualenv locally if it's not already there
VIRTUAL_ENV=virtualenv-12.0.2
if [[ ! -d "${ABS_TEST_DIR}/${VIRTUAL_ENV}" ]] ; then
  curl -O https://pypi.python.org/packages/source/v/virtualenv/$VIRTUAL_ENV.tar.gz
  tar xvfz $VIRTUAL_ENV.tar.gz
fi

# build a clean virtual environment
SAMZA_INTEGRATION_TESTS_DIR=$ABS_TEST_DIR/samza-integration-tests
if [[ ! -d "${SAMZA_INTEGRATION_TESTS_DIR}" ]] ; then
  python $VIRTUAL_ENV/virtualenv.py $SAMZA_INTEGRATION_TESTS_DIR
fi

# activate the virtual environment
source $SAMZA_INTEGRATION_TESTS_DIR/bin/activate

# install zopkio and requests
pip install -r $SCRIPTS_DIR/requirements.txt

# treat all trailing parameters (after dirname) as zopkio switches
shift
SWITCHES="$*"

# default to info-level debugging if not specified
if [[ $SWITCHES != *"console-log-level"* ]]; then
  SWITCHES="$SWITCHES --console-log-level INFO"
fi

# run the tests
zopkio --config-overrides remote_install_path=$ABS_TEST_DIR $SWITCHES $SCRIPTS_DIR/tests.py

# go back to execution directory
deactivate
cd $DIR
