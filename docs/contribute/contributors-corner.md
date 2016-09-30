---
layout: page
title: Contributor's Corner
---
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
- [Ask Questions](#ask-questions)
- [File Bug Reports](#file-bug-reports)
- [Find A Project to Work On](#find-a-project-to-work-on)
- [Contributing to Apache Samza](#contributing-to-apache-samza)
- [For Committers](#for-committers)
  * [Prepare to merge a Pull Request (PR) by setting up the following:](#prepare-to-merge-a-pull-request--pr--by-setting-up-the-following-)
  * [Merging a Pull Request](#merging-a-pull-request)

Apache Samza is developed by an open and friendly community. Everybody is welcome to contribute and engage with the community. We are happy to accept contributions, be it trivial cleanups or bug-fix or new features. There are many ways to engage with the community and contribute to Apache Samza, including filing bugs, asking questions and joining discussions in our mailing lists, contributing code or documentation, or testing.

## Ask Questions
Apache Samza community is happy to answer any questions related to the project. We use our [mailing list](/community/mailing-lists.html) for all communications and discussions. You can also interact in StackOverflow under the tag - [#apache-samza](http://stackoverflow.com/questions/tagged/apache-samza)

## File Bug Reports
Apache Samza uses [JIRA](https://issues.apache.org/jira/browse/SAMZA) to file bug reports. In order to file a bug report, please open Samza JIRA and include the following details:

* A descriptive title
* Description of the issue you are facing
* Version of Apache Samza in which this issue is seen - You can add the version(s) by updating "Affects Version/s" field in the JIRA
* Environment details (such as OS, JDK version etc) whenever applicable
 
If you need someone to immediately take a look at your JIRA, please don't hesitate to send an email to the dev mailing list.

## Find A Project to Work On

We tag bugs in [JIRA](https://issues.apache.org/jira/browse/SAMZA) with "[newbie](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open)" label that are good for people getting started with the code base. When you feel confident, you can pick-up "[newbie++](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20%22newbie%2B%2B%22%20AND%20status%20%3D%20Open)" JIRAs. Picking up these JIRAs are the best way to familiarize yourself with the codebase. 

More meaty projects are [here](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20project%20AND%20status%20%3D%20Open). The process for working on a large project is:

1. Instigate a discussion on the [JIRA](https://issues.apache.org/jira/browse/SAMZA)
2. Write a [design document](design-documents.html)
3. Request feedback for the [design document](design-documents.html) on the Jira and the samza-dev mailing list
4. Come to an agreement on design
5. Implement design

*If you are unclear whether a change you are proposing requires a design document, feel free ask us through our mailing list!*

## Contributing to Apache Samza

Samza's code is in an Apache Git repository located [here](https://git-wip-us.apache.org/repos/asf?p=samza.git;a=tree).

You can check out Samza's code by running:

```
git clone http://git-wip-us.apache.org/repos/asf/samza.git
```

The Samza website is built by Jekyll from the markdown files found in the `docs` subdirectory.

We use Pull Requests to review and discuss your contributions. In order to contribute code, please do the following:

* If you are working on a big new feature, follow the steps outlined above regarding [design documents](/contribute/design-documents.html) page
* If there is no JIRA for your work, please open a [JIRA](https://issues.apache.org/jira/browse/SAMZA) before creating a Pull Request. If it is a trivial fix (such as typo, doc fix etc), you may skip the JIRA creation.
* Creating Pull Request
    1. Fork the Github repository at [http://github.com/apache/samza](http://github.com/apache/samza) if you haven't already 
    2. Create a new branch in your repository and push your changes to that branch
        * Make sure you have observed the recommendations in the [coding guide](/contribute/coding-guide.html) and [testing](/contribute/tests.html)
    3. [Open a Pull Request](https://help.github.com/articles/about-pull-requests/) against the "master" branch of apache/samza
        * Make sure that the Pull Request title is of the format "SAMZA-&lt;JiraNumber&gt; : &lt;JiraTitle&gt;"
        * Make sure that your patch cleanly applies against the master branch. If not, rebase before creating the Pull Request
    4. Change the status of the JIRA to "Patch Available" so that it notifies the committers of the patch being available
* Nag us if we don't follow up on your JIRA in a timely fashion.
* If your Pull Request is approved, it will automatically be closed, with any associated JIRA when a committer merges your changes. 
* If your Pull Request is not approved and requires changes based on reviews, please make changes to your patch. 
    * While making the changes, kindly update the JIRA status from "Patch Available" to "In Progress". 
    * Make sure that you have rebased your branch to latest in the master branch before updating the Pull Request. This will help avoid conflicts during the merge. We cannot commit patches that have merge conflicts!
* If your Pull Request is rejected for whatever reason, please close it promptly because committers cannot close your Pull Requests!  

## For Committers

If you are a committer you need to use https instead of http to check in, otherwise you will get an error regarding an inability to acquire a lock. Note that older versions of git may also give this error even when the repo was cloned with https; if you experience this try a newer version of git. 

### Prepare to merge a Pull Request (PR) by setting up the following:

1. Setup JIRA on your host
    * Install Jira packages - ```sudo pip install jira```
    * Set the `JIRA_USERNAME` and `JIRA_PASSWORD` environment variables with the appropriate credentials for interacting with Jira. This is required to correctly close the JIRA associated with the PR
2. Setup aliases for the remote repositories:​(Samza Github repo and Apache Samza Repo)
    * Add ASF git repo for committing the PR
    ```git remote add samza-apache https://git­wip­us.apache.org/repos/asf/samza.git``` 
    * Add Github repo for fetching the patch from the PR
    ```git remote add samza-github https://github.com/apache/samza.git```
3. Set up API tokens for Git
    * Create an OAuth key for making requests to the GitHub API. If this is not defined, then requests will be unauthenticated and you can't access the API. An OAuth key for the API can be created at [https://github.com/settings/tokens](https://github.com/settings/tokens)        
    * Set the created OAuth key as `GITHUB_OAUTH_KEY` environment variable.

### Merging a Pull Request

* Committers can use the `bin/merge-pull-request.py` script to merge an approved PR. The script is interactive and will walk you through the steps for merging the patch, closing the PR and the associated JIRA.
```
cd samza
./bin/merge-pull-request.py 
```
* Whenever possible, make sure that the commit title includes the JIRA number and title.
* Merging changes that don't cleanly apply on the master should be avoided.  
* For committers wishing to update the webpage, please see `docs/README.md` for instructions.
