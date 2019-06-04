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

### Table Of Contents
- [Introduction](#introduction)
- [Code Examples](#code-examples)
- [Key Concepts](#key-concepts)
  - [SQL Representation](#sql-representation)
  - [SQL Grammar](#sql-grammar)
  - [UDFs](#udfs)
  - [UDF Polymorphism](#udf-polymorphism)
- [Known Limitations](#known-limitations)

### Introduction
Samza SQL allows you to define your stream processing logic 
declaratively as a a SQL query. This allows you to create streaming 
pipelines without Java code or configuration unless you require 
user-defined functions (UDFs). 

### Code Examples

The [Hello Samza](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/sql) 
SQL examples demonstrate how to use Samza SQL API.  

- The [filter](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/sql/samza-sql-filter) 
demonstrates filtering and insert Samza SQL job.

- The [Case-When](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/sql/samza-sql-casewhen/src/main/sql)
shows how to use ```CASE WHEN``` statement, along with ```UDF``` to identify qualifying events.

- The [join](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/sql/samza-sql-stream-table-join)
demonstrates how to peform a stream-table join. Please note that join operation is currently not 
fully cooked, and we are actively working on stabilizing it. 

- The [group by](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/sql/samza-sql-groupby)
show how to do group by. Similar to Join, Group By is being actively stabilized. 


### Key Concepts

Each Samza SQL job consists of one or more Samza SQL statements.
Each statement represents a single streaming pipeline.

#### SQL Representation

Support for SQL internally uses Apache Calcite, which provides 
SQL parsing and query planning. The query is automatically translated 
to Samza's high level API and runs on Samza's execution engine.

The mapping from SQL to the Samza's high level API is a simple 
deterministic one-to-one mapping. For example, ```Select```, i.e., 
projections, maps to a filter operation, while ```from``` maps to 
a scan(s) and join(s) - if selecting from multiple streams and tables
- operators, and so on.  

The table below lists the supported SQL operations.

 Operation | Syntax hints | Comments      
 --- | --- | --- 
 PROJECTION | SELECT/INSERT/UPSERT | See [SQL Grammar](#sql-grammar) below 
 FILTERING | WHERE expression |See [SQL Grammar](#sql-grammar) below 
 UDFs | udf_name(args)    | In both SELECT and WHERE clause 
 JOIN | [LEFT/RIGHT] JOIN .. ON .. | Stream-table inner, left- or right-outer joins. Currently not fully stable. 
 AGGREGATION | COUNT ( ...) .. GROUP BY | Currently only COUNT is supported, using processing-time based window. 


#### SQL Grammar

Samza SQL's grammar is a subset of capabilities supported by Calcite's SQL parser.

```
statement:
  |   insert
  |   query 
  
query:
  values
  | {
      select
    }
 
insert:
      ( INSERT | UPSERT ) INTO tablePrimary
      [ '(' column [, column ]* ')' ]
      query 

select:
  SELECT
  { * | projectItem [, projectItem ]* }
  FROM tableExpression
  [ WHERE booleanExpression ]
  [ GROUP BY { groupItem [, groupItem ]* } ]
   
projectItem:
  expression [ [ AS ] columnAlias ]
  | tableAlias . *
 
tableExpression:
  tableReference [, tableReference ]*
  | tableExpression [ NATURAL ] [ LEFT | RIGHT | FULL ] JOIN tableExpression [ joinCondition ]
 
joinCondition:
  ON booleanExpression
  | USING '(' column [, column ]* ')'
 
tableReference:
  tablePrimary
  [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]
 
tablePrimary:
  [ TABLE ] [ [ catalogName . ] schemaName . ] tableName
   
values:
  VALUES expression [, expression ]*

```

#### UDFs

In addition to existing SQL logical operations, Samza SQL 
allows the user to extend its functionality by running 
user-code through User Defined Functions (UDFs) as part of the Stream processing 
pipeline corresponding to the SQL.


#### UDF Polymorphism 

Since UDF's execute method takes an array of generic objects as
parameter, Samza SQL UDF framework is flexible enough to 
support polymorphic udf functions with varying sets of arguments 
as long as UDF implementations support them.

For example in the below sql statement, UDF will be passed an 
object array of size 2 with first element containing id of type  
"LONG" and second element name of type "String". The type of the 
objects that are passed depends on the type of those fields in Samza 
SQL message format.

{% highlight sql %}
select myudf(id, name) from identity.profile
{% endhighlight %}


### Known Limitations

Samza SQL only supports simple stateless queries including selections
and projections. We are actively working on supporting stateful operations 
such as aggregations, windows and joins.


