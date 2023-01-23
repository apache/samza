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

Samza's documentation uses Jekyll to build a website out of markdown pages. Jekyll dependencies are now managed with Gradle.

To compile the website in the _site directory, execute:

    ./gradlew docs:jekyllBuild

To serve the website on [localhost:4000](http://localhost:4000/), run the following:

    ./gradlew docs:jekyllServeLocal

To serve the website on so its visible to other hosts on port 4000, run the following:

    ./gradlew docs:jekyllServePublic

The jekyllServe* tasks do not end as they will wait for Jekyll to exit.  You will need to cntl-c jekyllServeLocal or JekyllServePublic to stop the task.


## Versioning

If you're working with versioned content (anything in the learn or img directories), you'll also need to run a script that generates the appropriate directories:

    _docs/local-site-test.sh

Run this script after starting the site server with the `serve` command above. Otherwise, you may see some errors when trying to view versioned content.
In addition, the local-site-test.sh script must be run every time a change is made to versioned content locally in order to trigger a refresh with Jekyll.

Keep in mind that versioned content in older versions links to samza.apache.org, not the localhost:4000. This is because they are not updated by your branch and are using the values in SVN instead.

To add a new version, change the version number in _config.yml. All links in pages should use {{site.version}}, not hard-coded version number.

## Javadocs

To auto-generate the latest Javadocs, run:

    bin/generate-javadocs.sh

## Publish

To build and publish the website to Samza's Apache SVN repository, run:

    bin/publish-site.sh "updating welcome page" criccomini

This command will re-build the Javadocs and website, checkout https://svn.apache.org/repos/asf/samza/site/ locally, copy the site into the directory, and commit the changes.

Sanity-check a couple of links in the website corresponding to the latest release. For example, when releasing Samza 1.0.0, verify that http://samza.apache.org/learn/documentation/1.0.0/ and http://samza.apache.org/learn/documentation/latest/ links work.

## Release-new-version Website Checklist

Assume we want to release x.x.x and the next release is y.y.y in master, need to work on two branches: x.x.x and master.

Following can be done when updating the gradle.properties file

1. in x.x.x branch,

    * if this is a major release, modify the docs/_config.yml to make both the "version" and "latest-release" properties to x.x.x

    * remove "git checkout latest" line and the "-SNAPSHOT" version suffix in each of the tutorials
      * docs/startup/hello-samza/versioned/index.md
      * docs/learn/tutorials/versioned/hello-samza-high-level-code.md
      * docs/learn/tutorials/versioned/hello-samza-high-level-yarn.md
      * docs/learn/tutorials/versioned/hello-samza-high-level-zk.md
      * docs/learn/tutorials/versioned/samza-rest-getting-started.md

2. in master branch,

    * if this is a major release, modify the docs/_config.yml to make the "latest-release" to x.x.x

    * if this is a major release, add the x.x.x release part in docs/archive/index.html

    * update the download page (docs/startup/download/index.md) to use x.x.x release
      by adding an entry to the Sources and Samza Tools sections to use the new x.x.x release

    * add a release page docs/_releases/x.x.x.md

    * add a new entry for the new version in docs/_menu/index.html

    * update the version number in "tar -xvf ./target/hello-samza-y.y.y-dist.tar.gz -C deploy/samza" in each of the tutorials (and search for other uses of version x.x.x which may need to be replaced with y.y.y)
      * docs/startup/hello-samza/versioned/index.md
      * docs/learn/tutorials/versioned/hello-samza-high-level-yarn.md
      * docs/learn/tutorials/versioned/hello-samza-high-level-zk.md
      * docs/learn/tutorials/versioned/samza-rest-getting-started.md

    * Write a blog post on the open source blog.
       Update versioning (docs/_blog/YYYY-MM-DD-TITLE.md). Usually the same info as in docs/_releases/x.x.x.md.

After apache mirrors pick up the new release,

3. in x.x.x branch, run bin/publish-site.sh "updating welcome page" criccomini

4. in master branch, run bin/publish-site.sh "updating welcome page" criccomini
