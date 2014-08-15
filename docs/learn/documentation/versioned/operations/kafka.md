---
layout: page
title: Kafka
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

<!-- TODO kafka page should be fleshed out a bit -->

<!-- TODO when 0.8.1 is released, update with state management config information -->

Kafka has a great [operations wiki](http://kafka.apache.org/08/ops.html), which provides some detail on how to operate Kafka at scale.

### Auto-Create Topics

Kafka brokers should be configured to automatically create topics. Without this, it's going to be very cumbersome to run Samze jobs, since jobs will write to arbitrary (and sometimes new) topics.

{% highlight jproperties %}
auto.create.topics.enable=true
{% endhighlight %}
