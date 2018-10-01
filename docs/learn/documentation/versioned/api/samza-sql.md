---
layout: page
title: Samza SQL
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


# Section 1

# Sample Applications


# Section 2

# Section 3


# Section 4

The table below summarizes table metrics:


| Metrics | Class | Description |
|---------|-------|-------------|
|`get-ns`|`ReadableTable`|Average latency of `get/getAsync()` operations|
|`getAll-ns`|`ReadableTable`|Average latency of `getAll/getAllAsync()` operations|
|`num-gets`|`ReadableTable`|Count of `get/getAsync()` operations
|`num-getAlls`|`ReadableTable`|Count of `getAll/getAllAsync()` operations


### Section 5 example

It is up to the developer whether to implement both `TableReadFunction` and 
`TableWriteFunction` in one class or two separate classes. Defining them in 
separate classes can be cleaner if their implementations are elaborate and 
extended, whereas keeping them in a single class may be more practical if 
they share a considerable amount of code or are relatively short.
