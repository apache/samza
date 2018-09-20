---
layout: page
title: Case Studies
exclude_from_loop: true
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

Explore the many use-cases of the Samza Framework via our case-studies.

<ul class="case-studies">

  {% for company in site.case-studies %}
    {% if company.exclude_from_loop %}
        {% continue %}
    {% endif %}
    <li>
      <a href="{{ company.url }}" title="{{ company.menu_title }}">
        <span style="background-image: url('https://logo.clearbit.com/{{ company.study_domain }}?size=256');"></span>
      </a>
      <div class="study-detail">
        <a href="https://{{ company.study_domain }}" class="external-link" rel="nofollow">
          <i class="icon ion-md-share-alt"></i> {{ company.menu_title }}
        </a>
        {% if company.excerpt %}
        <div class="study-description">
        {{ company.excerpt }}
        </div>
        {% endif %}
        <a class="btn" href="{{ company.url }}">View Case Study</a>
      </div>
    </li>
  {% endfor %}

</ul>