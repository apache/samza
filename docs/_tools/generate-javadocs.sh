#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$DIR/../..
VERSION=$1
JAVADOC_DIR=$BASE_DIR/docs/learn/documentation/$VERSION/api/javadocs

if test -z "$VERSION"; then
  echo
  echo "  USAGE:"
  echo
  echo "    ${BASH_SOURCE[0]##*/} 0.7.0"
  echo
  exit 0
fi

cd $BASE_DIR
./gradlew javadoc
rm -rf $JAVADOC_DIR
mkdir -p $JAVADOC_DIR
cp -r $BASE_DIR/samza-api/build/docs/javadoc/* $JAVADOC_DIR
cd -
