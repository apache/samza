---
layout: page
title: Samza Enhancement Proposal
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

- [When is a SEP needed](#when-is-a-sep-needed)
- [What is the SEP Process](#what-is-the-sep-process)
- [What should be included in SEP](#what-should-be-included-in-sep)
- [What are the criteria to accept or reject a SEP](#what-are-the-criteria-to-accept-or-reject-a-sep)

*Samza Enhancement Proposal* (SEP) is the official process for proposing major changes to Apache Samza. All technical decisions in Samza have pros and cons - Having a SEP helps us capture the thought process that lead to a decision.

We want to make Samza a core architectural component for users. Samza also supports a large number of integrations with other tools, systems, and clients. Keeping these kinds of usages healthy requires a high level of compatibility across releases. As a result each new major feature or public API has to be done in a way that we can stick with it going forward. 

Note that this process isn't meant to discourage incompatible changes. Rather it is intended to avoid accidentally introducing half thought-out interfaces and protocols that cause needless heartburn when changed.

## When is a SEP needed

Any major **functional** change to Samza's core functionality, that is changes to the behavior of the classes in samza-core package requires a SEP.

Any of the following should be considered a major change:

* major new feature, subsystem, or piece of functionality
* change that impacts the public interfaces of the project

The following are treated as public interfaces:

* Classes in the public package - samza-api
* Configurations in Samza
* Metrics reported by Samza
* Command line tools and their arguments

## What is the SEP Process

1. Create a page which is a child of [this one](https://cwiki.apache.org/confluence/display/SAMZA/Samza+Enhancement+Proposal). Take the next available SEP number and give your proposal a descriptive heading. e.g. "SEP 73: Samza Standalone with ZK Coordination". If you don't have the necessary permissions for creating a new page, please ask on the [dev mailing list](mailto:dev@samza.apache.org).
2. Fill in the sections as described in [What should be included in SEP](#what-should-be-included-in-sep).
3. Start a [DISCUSS] thread on the Apache mailing list. Please ensure that the subject of the thread is of the format [DISCUSS] SEP-{your SEP number} {your SEP heading}. The discussion should happen on the mailing list and **NOT** on the wiki since the wiki comment system doesn't work well for larger discussions. In the process of the discussion you may update the proposal. You should let people know the changes you are making.
4. Once the proposal is finalized call a [VOTE] to have the proposal adopted. These proposals are more serious than code changes and more serious even than release votes. The criterion for acceptance is lazy majority.
5. Please update the SEP wiki page to reflect the current stage of the SEP after a vote. This acts as the permanent record indicating the result of the SEP (e.g., Accepted or Rejected). Also report the result of the SEP vote to the voting thread on the mailing list so the conclusion is clear.

## What should be included in SEP

A typical SEP should include the following sections:

* **Problem**: Describe the problem to be solved
* **Motivation**: Why the problem should be solved
* **Proposed Changes**: Describe the changes you want to do. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences, depending on the scope of the change.
* **New or changed public interfaces, if any**: Impact to any of the "compatibility commitments" described above. We want to call these out in particular, so everyone thinks about them.
* **Implementation / Test Plan (if applicable)**: Some large projects may consist of multiple sub-projects and requires more planning for implementation and testing. It may be prudent to call them out in the proposal.
* **Migration Plan and Compatibility**: If this feature requires additional support for a no-downtime upgrade describe how that will work. If a no-downtime upgrade for users cannot be support, describe the migration plan for the users.
* **Rejected Alternatives**: What are the other alternatives you considered and why are they worse? The goal of this section is to help people understand why this is the best solution now, and also to prevent churn in the future when old alternatives are reconsidered.

## What are the criteria to accept or reject a SEP

We will be following the [Apache voting guideline](https://www.apache.org/foundation/voting.html) to drive our decisions regarding an SEP. Decisions will be made on the [VOTE] mailing thread for a particular SEP. All community members are encouraged to participate by replying to the VOTE mailing thread. The votes can have the following meaning:

<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th align="center">Vote</th>
      <th align="center">Meaning</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td align="center">+1</td>
      <td>'Yes,' 'Agree,' or 'the action should be performed.'</td>
    </tr>
    <tr>
      <td align="center">0</td>
      <td>Neutral about the proposed action (or mildly negative but not enough so to want to block it.</td>
    </tr>
    <tr>
      <td align="center">-1</td>
      <td>This is a negative vote. On issues where consensus is required, this vote counts as a **veto**. All vetoes must contain an explanation of why the veto is appropriate. Vetoes with no explanation are void. It may also be appropriate for a -1 vote to include an alternative course of action. </td>
    </tr>
  </tbody>
</table>

The criteria for acceptance of a SEP is [lazy majority](https://cwiki.apache.org/confluence/display/KAFKA/Bylaws#Bylaws-Approvals).
