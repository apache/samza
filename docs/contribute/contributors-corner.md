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
- [Contributing Documentation](#contributing-documentation)
- [Contributing to Apache Samza](#contributing-to-apache-samza)
  * [Building and Testing](#building-and-testing)
  * [Developing with an IDE](#developing-with-an-ide)
- [Pull Requests](#pull-requests)  

Apache Samza is developed by an open and friendly community. Everybody is welcome to contribute and engage with the community. There are many such opportunities:

* ask or answer questions
* review proposed design ideas 
* improve the documentation
* contribute bug reports
* write new sample applications
* add new or improve existing test cases
* add new libraries (new IO systems, etc)
* work on the core Samza stream processing framework
* review design proposals and documents

## Ask Questions

Apache Samza community is happy to answer any questions related to the project with any communication channels listed in [mailing list](/community/contact-us.html). 

## File Bug Reports

Apache Samza uses [JIRA](https://issues.apache.org/jira/browse/SAMZA) to file bug reports. In order to file a bug report, please open Samza JIRA and include the following details:

* A descriptive title
* Description of the issue you are facing
* Version of Apache Samza in which this issue is seen - You can add the version(s) by updating "Affects Version/s" field in the JIRA
* Environment details (such as OS, JDK version etc) whenever applicable
 
If you need someone to immediately take a look at your JIRA, please don’t hesitate to send an email to the [dev mailing list](mailto:dev@samza.apache.org).

## Find A Project to Work On

We tag tickets in [JIRA](https://issues.apache.org/jira/browse/SAMZA) with “[newbie](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20newbie%20AND%20status%20%3D%20Open)” label that are good for people getting started with the code base. When you feel confident, you can pick-up “[newbie++](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20%22newbie%2B%2B%22%20AND%20status%20%3D%20Open)” JIRAs. Picking up these JIRAs are the best way to familiarize yourself with the codebase.

More meaty projects are [here](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20project%20AND%20status%20%3D%20Open). The process for working on a large project is documented in details in [samza enhancement proposal](/contribute/enhancement-proposal.html). 
If you are unclear whether a change you are proposing requires a design document, ask us through our [mailing list](mailto:dev@samza.apache.org).

## Contributing Documentation

The Samza documentation is based on markdown files, which are built using Jekyll. To propose a change to documentation, edit their corresponding markdown source files in Samza’s [docs/](https://github.com/apache/samza/tree/master/docs) directory. The [README.md](https://github.com/apache/samza/blob/master/docs/README.md) in the same directory shows how to build the documentation locally and test your changes. The process to propose a doc change is otherwise the same as the process for proposing code changes below.

## Contributing to Apache Samza

The official Samza codebase is hosted on the Apache Git repository located [here](https://github.com/apache/samza) and we use [Pull Requests](https://help.github.com/articles/about-pull-requests/) on [GitHub](https://github.com/apache/samza) for code reviews. If you are unfamiliar with this workflow, [Git Handbook](https://guides.github.com/introduction/git-handbook/) has many useful introductions and instructions.

You should [fork](https://guides.github.com/activities/forking/) the [Samza’s Github repository](https://github.com/apache/samza) to create your own repository.

### Building and Testing

We use [Gradle](https://gradle.org/) to build and test Samza. You do not need to install gradle, but you do need a Java SDK installed. You can develop on Linux and macOS. 

The entire set of unit tests can be run with this command at the root of the git repository.

```
./gradlew test
```

### Developing with an IDE

Generate an IDEA project .ipr file with:

```
./gradlew idea
```

## Pull Requests

If there is no [JIRA](https://issues.apache.org/jira/browse/SAMZA) ticket for your work, please open one before creating a Pull Request. If it is a trivial fix (such as a typo, doc fix etc), you may skip ticket creation. Here’s the development workflow:

* Create a new branch in your repository and push your changes to that branch
  * Make sure you have observed the recommendations in the [coding guide](/contribute/coding-guide.html) and [testing](/contribute/tests.html)
* [Open a Pull Request](https://help.github.com/articles/about-pull-requests/) against the “master” branch of apache/samza
  * Make sure that the Pull Request title is of the format "SAMZA-&lt;JiraNumber&gt; : &lt;JiraTitle&gt;"
  * Make sure that your patch cleanly applies against the master branch. If not, rebase before creating the Pull Request
  * Optional: add a reviewer by @ the username
* [Ping us](mailto:dev@samza.apache.org) if we don’t follow up on your JIRA in a timely fashion.  
* If your Pull Request is approved, it will automatically be closed, along with any associated JIRA ticket when a committer merges your changes.
* If your Pull Request is not approved and requires changes based on reviews, please make changes and update the Pull Request.
  * Fixes can simply be pushed to the same branch from which you opened your pull request.
  * Please address feedback via additional commits instead of amending existing commits. This makes it easier for the reviewers to know what has changed since the last review.
  * Jenkins will automatically re-test when new commits are pushed.
  * Despite our efforts, Samza may have flaky tests at any given point, if the failure is unrelated to your pull request and you have been able to run the tests locally successfully, please mention it in the pull request.
* If your Pull Request is rejected or discarded for whatever reason, please close it promptly because committers cannot close your Pull Requests since committers cannot close them.   