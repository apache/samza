---
layout: page
title: Samza Blog
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

<div class="news_cards">

  {% assign sorted = (site.blog | sort: 'date') | reverse %}

  {% for post in sorted %}

    {% if post.exclude_from_loop %}
      {% continue %}
    {% endif %}

    {% assign icon = "ion-md-paper" %}

    {% if post.icon %}

    {% assign icon = "ion-md-" | append: post.icon %}

    {% endif %}

  <a class="news__card" href="{{ post.url }}">
    <i class="news__card-icon icon {{ icon }}"></i>
    <div class="news__card-date">{{ post.date | date: "%B %-d, %Y" }}</div>
    <div class="news__card-title">{{ post.title }}</div>
    {% if post.excerpt %}
    <div class="news__card-description">
      {{ post.excerpt }}
    </div>
    {% endif %}
    <span class="news__card-button">Read more</span>
  </a>
  {% endfor %}

</div>