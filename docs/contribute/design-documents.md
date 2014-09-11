---
layout: page
title: Design Documents
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

When making larger changes to Samza, or working on a [project](/contribute/projects.html), please write a design document. All of Samza's existing design documents can be found [here](https://issues.apache.org/jira/issues/?jql=project%20%3D%20SAMZA%20AND%20labels%20%3D%20design%20ORDER%20BY%20priority%20DESC).

### Why Write a Design Document?

The goal of the design document is to:

1. Define the problem you're trying to solve.
2. Propose a solution, and explore alternatives.
3. Instigate discussion on the issue.
4. Arrive at a final conclusion on design.
5. Archive design documents for future use.

It may be necessary to iterate on the design document several times before a final conclusion is reached.

### How to Write a Design Document

Design documents should be attached to the [JIRA](https://issues.apache.org/jira/browse/SAMZA) for the feature that the design is for. The [JIRA](https://issues.apache.org/jira/browse/SAMZA) should be labeled with the "design" label.

There is no single format for a design document, but it's common to include:

1. Introduction
2. Definition of problem
3. Possible solutions
4. Opinion on best solution
5. Details on how the solution should be implemented.

An example of a design document can be seen on [SAMZA-402](https://issues.apache.org/jira/browse/SAMZA-402), which contains several versions of both the raw [Markdown](http://daringfireball.net/projects/markdown/syntax) file and the PDF for the design document.

### Tools

Some useful tools for writing design documents are:

* [Markdown](http://daringfireball.net/projects/markdown/syntax): A syntax for writing well-formatted text-based documents.
* [asciiflow.com](http://asciiflow.com): A webpage to draw flow charts using ASCII. This is useful for design docs written in text formats such as markdown.
* [Markdown Reader](https://chrome.google.com/webstore/detail/markdown-reader/gpoigdifkoadgajcincpilkjmejcaanc): A Chrome extension that lets you view markdown (.md) files in Chrome. Once viewed in Chrome, the markdown file can be printed on a Mac simply by printing the page, and selecting "Save as PDF" as the printer.
* [TLA/TLA+](http://research.microsoft.com/en-us/um/people/lamport/tla/tla.html): A formal specification language for writing high-level specifications of concurrent and distributed systems.
* [Pandoc](http://johnmacfarlane.net/pandoc/): A tool for converting various text formats into one another. Pandoc supports converting [Markdown](http://daringfireball.net/projects/markdown/syntax) to PDF, among others.