<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->
## Setup

Samza's documentation uses Jekyll to build a website out of markdown pages. To install Jekyll, run this command:

    sudo gem install jekyll redcarpet

To run the website locally, execute:

    jekyll serve --watch --host 0.0.0.0

To compile the website in the _site directory, execute:

    jekyll build

## Versioning

The "Learn" section of this website is versioned. To add a new version, copy the folder at the version number-level (0.7.0 to 0.8.0, for example).

All links between pages inside a versioned folder should be relative links, not absolute.

## Javadocs

To auto-generate the latest Javadocs, run:

    _tools/generate-javadocs.sh <version>

The version number is the number that will be used in the /docs/learn/documentation/<version>/api/javadocs path.

## Release

To build and publish the website to Samza's Apache SVN repository, run:

    _tools/publish-site.sh 0.7.0 "updating welcome page" criccomini

This command will re-build the Javadocs and website, checkout https://svn.apache.org/repos/asf/incubator/samza/site/ locally, copy the site into the directory, and commit the changes.
