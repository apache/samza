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

Samza's documentation uses Jekyll to build a website out of markdown pages. Prerequisites:

1. You need [Ruby](https://www.ruby-lang.org/) installed on your machine (run `ruby --version` to check)
2. Install [Bundler](http://bundler.io/) by running `sudo gem install bundler`
3. To install Jekyll and its dependencies, change to the `docs` directory and run `bundle install`

To serve the website on [localhost:4000](http://localhost:4000/):

    bundle exec jekyll serve --watch --baseurl

To compile the website in the \_site directory, execute:

    bundle exec jekyll build

To test the site, run:

    bundle exec jekyll serve --watch --baseurl

## Versioning

If you're working with versioned content (anything in the learn or img directories), you'll also need to run a script that generates the appropriate directories:

      _docs/local-site-test.sh

The local-site-test.sh script must be run every time a change is made to versioned content locally in order to trigger a refresh with Jekyll.

Keep in mind that versioned content in older versions links to samza.apache.org, not the localhost:4000. This is because they are not updated by your branch and are using the values in SVN instead.

To add a new version, change the version number in _config.yml. All links in pages should use {{site.version}}, not hard-coded version number.

## Javadocs

To auto-generate the latest Javadocs, run:

    bin/generate-javadocs.sh

## Publish

To build and publish the website to Samza's Apache SVN repository, run:

    bin/publish-site.sh "updating welcome page" criccomini

This command will re-build the Javadocs and website, checkout https://svn.apache.org/repos/asf/samza/site/ locally, copy the site into the directory, and commit the changes.

## Release-new-version Website Checklist

Assume we want to release x.x.x and the next release is y.y.y in master, need to work on two branches: x.x.x and master.

Following can be done when updating the gradle.properties file

1. in x.x.x branch,

    * modify the docs/_config.yml to make the "version" and "latest-release" to x.x.x

    * remove "git checkout latest" in docs/startup/hello-samza/versioned/index.md

2. in master branch,

    * modify the docs/_config.yml to make the "latest-release" to x.x.x

    * add the x.x.x release to Archive category in docs/_layout/default.html and x.x.x release part in docs/archive/index.html

    * update the download page to use x.x.x release

    * update the version number in "tar -xvf ./target/hello-samza-y.y.y-dist.tar.gz -C deploy/samza" in docs/startup/hello-samza/versioned/index.md

After apache mirrors pick up the new release,

3. in x.x.x branch, run bin/publish-site.sh "updating welcome page" criccomini

4. in master branch, run bin/publish-site.sh "updating welcome page" criccomini
