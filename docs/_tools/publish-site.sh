#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$DIR/../..
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
$DOCS_DIR/_tools/generate-javadocs.sh $VERSION

echo "Building site."
cd $DOCS_DIR
jekyll build

echo "Checking out SVN site."
SVN_TMP=`mktemp -d /tmp/samza-svn.XXXX`
svn co https://svn.apache.org/repos/asf/incubator/samza/ $SVN_TMP
cp -r _site/* $SVN_TMP/site/
svn add --force $SVN_TMP/site
svn commit $SVN_TMP -m"$COMMENT" --username $USER
rm -rf $SVN_TMP
