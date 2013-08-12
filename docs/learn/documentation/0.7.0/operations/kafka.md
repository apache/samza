---
layout: page
title: Kafka
---

<!-- TODO kafka page should be fleshed out a bit -->

<!-- TODO when 0.8.1 is released, update with state management config information -->

Kafka has a great [operations wiki](http://kafka.apache.org/08/ops.html), which provides some detail on how to operate Kafka at scale.

### Auto-Create Topics

Kafka brokers should be configured to automatically create topics. Without this, it's going to be very cumbersome to run Samze jobs, since jobs will write to arbitrary (and sometimes new) topics.

    auto.create.topics.enable=true
