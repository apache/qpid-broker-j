## Table of Contents
1. [Introduction](#introduction)
2. [Broker Configuration](#broker-configuration)
3. [Request Format](#request-format)  

    3.1 [SQL Query Format](#sql-query-format)    
    3.2 [Datetime Format](#datetime-format)    
    3.3 [Datetime Pattern](#datetime-pattern)    
    3.4 [Decimal Digits](#decimal-digits)    
    3.5 [Rounding Mode](#rounding-mode)    

4. [Object Types (Domains)](#object-types-domains)

5. [Filtering Results](#filtering-results)    

   5.1 [Broker Data Types](#broker-data-types)
   
   5.2 [Keyword DISTINCT](#keyword-distinct)

   5.3 [Comparison Operators](#comparison-operators)   
     * [BETWEEN](#between)        
     * [EQUAL](#equal)  
     * [GREATER THAN](#greater-than)   
     * [GREATER THAN OR EQUAL](#greater-than-or-equal)    
     * [IN](#in)    
     * [IS NULL](#is-null)    
     * [LESS THAN](#less-than)    
     * [LESS THAN OR EQUAL](#less-than-or-equal)    
     * [LIKE](#like)  
   
   5.4 [Conditional Operators](#conditional-operators)
     * [CASE](#case)
   
   5.5 [Logical Operators](#logical-operators)    
   
6. [Sorting Results](#sorting-results)

7. [Aggregation](#aggregation)

8. [Grouping](#grouping)

9. [Functions](#functions)    

   9.1 [Aggregation Functions](#aggregation-functions)    
     * [AVG](#avg)    
     * [COUNT](#count)    
     * [MAX](#max)    
     * [MIN](#min)    
     * [SUM](#sum) 
   
   9.2 [Datetime Functions](#datetime-functions)
     * [CURRENT_TIMESTAMP](#current_timestamp)
     * [DATE](#date)
     * [DATEADD](#dateadd)
     * [DATEDIFF](#datediff)
     * [EXTRACT](#extract)
   
   9.3 [Null Functions](#null-functions)
     * [COALESCE](#coalesce)
   
   9.4 [Numeric Functions](#numeric-functions)
     * [ABS](#abs)
     * [ROUND](#round)
     * [TRUNC](#trunc)
   
   9.5 [String Functions](#string-functions)
     * [CONCAT](#concat)
     * [LEN/LENGTH](#len--length)
     * [LOWER](#lower)
     * [LTRIM](#ltrim)
     * [POSITION](#position)
     * [REPLACE](#replace)
     * [RTRIM](#rtrim)
     * [SUBSTR/SUBSTRING](#substr--substring)
     * [TRIM](#trim)
     * [UPPER](#upper)

10. [Set Operations](#set-operations)

11. [Subqueries](#subqueries)
   
12. [Performance Tips](#performance-tips)

## Introduction

Broker query engine extends existing functionality of broker query API and allows executing complex SQL-like queries
against the broker. It allows using predicates combining AND/OR/NOT logical operations, supports aggregation and 
grouping as well as numerous numeric, datetime and string functions.

Currently, querying from multiple object types (domains) in a single query as well as all types of joins are not supported.

## Broker Configuration

Some properties influencing the query output can be specified directly in the request, but there are also global
properties, affecting the way query engine works.

| Context Property Name                     | Description         |
|-------------------------------------------|---------------------|
| qpid.port.http.query.engine.cacheSize     | Query cache size    |
| qpid.port.http.query.engine.maxQueryDepth | Maximal query depth |
| qpid.port.http.query.engine.zoneId        | Timezone ID         |

### Query cache size

After query is parsed from the SQL string, it is stored into a cache. When the same query will be fired against 
the query engine, parsing will be omitted and the query structure will be retrieved from cache. By default, query cache
size is 1000. This means, that when 1000 different queries will be fired against the query engine, the next one will 
override the oldest cache entry. When set to 0 or to negative value, query cache will not be used and each query
will be parsed.

### Maximal query depth

The longer is the query and the more conditions it contains, the bigger becomes the query depth. To limit query 
complexity, maximal query depth parameter can be used. By default, maximal query depth is 4096. This should suffice 
for most queries even complicated ones. If query depth exceeds this limit, following error will
be returned:

```
{
    "errorMessage": "Max query depth reached: 4096"
}
```

### Zone ID

Zone ID value should follow the rules described in [javadoc](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#of-java.lang.String-).
The default value for zone id is `UTC`.

## Request Format

An authorized request should be sent to the following endpoint:

POST http://&lt;hostname&gt;:&lt;port&gt;/api/latest/querybroker/broker

SQL query should be supplied in the `sql` field of the JSON body:

```
{
    "sql": "select * from broker"
}
```

### SQL Query Format

SQL keywords, operators and functions are case-insensitive, so are domain names (object types) specified in the `FROM` 
clause. Field names specified in the `SELECT` clause are case-sensitive.
Following queries are similar:
```
{
    "sql": "SELECT name FROM BROKER"
}
```
```
{
    "sql": "SELECT name FROM broker"
}
```
```
{
    "sql": "select name from broker"
}
```
They will return the same output.
When an entity field name is written in wrong case or misspelled, an error will be returned. For example, following query
```
{
    "sql": "SELECT NAME FROM BROKER"
}
```
has field `NAME` written in upper case, which will result in an error:
```
{
    "errorMessage": "Domain 'BROKER' does not contain field 'NAME'"
}
```
In this document many SQL queries are split into several lines for better readability, but JSON format does not support 
multiline string fields. Therefore, even the long SQL queries should be placed in `sql` field of the JSON body as a 
single line.

Aside from SQL query several configuration parameters can be provided to influence output format:

| Field name      | Description                                                                                                                   |
|-----------------|-------------------------------------------------------------------------------------------------------------------------------|
| dateTimeFormat  | Format of the datetime fields, possible values: LONG, STRING                                                                  |
| dateTimePattern | Pattern for datetime fields formatting, e.g. yyyy-MM-dd HH:mm:ss                                                              |
| decimalDigits   | Amount of decimal digits                                                                                                      |
| roundingMode    | Rounding mode for arithmetic operations, possible values UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN, UNNECESSARY |

### Datetime Format
When datetime format is specified as `LONG`, datetime fields will be returned as milliseconds from UNIX epoch. So, following query
```
{
    "sql": "select id, name, createdTime from broker",
    "dateTimeFormat": "LONG"
}
```
<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "id": "ce8bbaf0-3efa-4176-889a-7987ac1988cc",
            "name": "broker",
            "createdTime": 1645195849272
        }
    ],
    "total": 1
}
```
  </p>
</details>

In opposite the query
```
{
    "sql": "select id, name, createdTime from broker",
    "dateTimeFormat": "STRING"
}
```
<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "id": "ce8bbaf0-3efa-4176-889a-7987ac1988cc",
            "name": "broker",
            "createdTime": "2022-02-18 15:50:49.272"
        }
    ],
    "total": 1
}
```
  </p>
</details>

### Datetime Pattern

The default format of the string datetime representation is `yyyy-MM-DD HH:mm:ss.SSS`. It can be changed using the parameter `dateTimePattern`.

The query
```
{
    "sql": "select id, name, createdTime from broker",
    "dateTimeFormat": "STRING",
    "dateTimePattern": "yyyy/MM/dd HH:mm:ss.SSS"
}
```
<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "id": "ce8bbaf0-3efa-4176-889a-7987ac1988cc",
            "name": "broker",
            "createdTime": "2022/02/18 15:50:49.272"
        }
    ],
    "total": 1
}
```
  </p>
</details>

### Decimal Digits

By default, decimal digits value is 6, meaning there will be 6 digits after decimal point. For example, following query

```
{
    "sql": "select avg(queueDepthMessages) from queue"
}
```
<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "avg(queueDepthMessages)": 0.437227
        }
    ],
    "total": 1
}
```
  </p>
</details>

This behavior can be changed for each value separately using ROUND or TRUNC functions, but can also be changed for
the whole query result by supplying `decimalDigits` parameter. Following query
```
{
    "sql": "select avg(queueDepthMessages) from queue",
    "decimalDigits": 2
}
```
<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "avg(queueDepthMessages)": 0.43
        }
    ],
    "total": 1
}
```
  </p>
</details>

### Rounding Mode

Rounding mode affects how results of the arithmetic operations will be rounded. The rules of applying different rounding
modes can be found in appropriate [javadoc](https://docs.oracle.com/javase/8/docs/api/java/math/RoundingMode.html).
Default rounding mode is HALF_UP. Changing rounding mode will affect division operations, but will not affect results of
[ROUND()](#round) and [TRUNC()](#trunc) functions (which always use rounding mode HALF_UP and HALF_DOWN appropriately).
Following query
```
{
    "sql": "select 2/3",
    "decimalDigits": 2,
    "roundingMode": "DOWN"
}
```
uses rounding mode `DOWN`
<details>
  <summary>and returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "2/3": 0.66
        }
    ],
    "total": 1
}
```
  </p>
</details>

When rounding mode will be changed to `UP`
```
{
    "sql": "select 2/3",
    "decimalDigits": 2,
    "roundingMode": "UP"
}
```
<details>
  <summary>result will be changed as well (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "2/3": 0.67
        }
    ],
    "total": 1
}
```
  </p>
</details>

## Object Types (Domains)

Object types or domains to query from are specified in the `FROM` clause. The broker object hierarchy can be retrieved
using an endpoint http://&lt;hostname&gt;:&lt;port&gt;/service/metadata

Alternatively following SQL query can be fired
```
{
    "sql": "select * from domain"
}
```
<details>
  <summary>returning similar result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "name": "AccessControlProvider"
        },
        {
            "name": "AclRule"
        },
        {
            "name": "AuthenticationProvider"
        },
        {
            "name": "Binding"
        },
        {
            "name": "BrokerConnectionLimitProvider"
        },
        {
            "name": "BrokerLogInclusionRule"
        },
        {
            "name": "BrokerLogger"
        },
        {
            "name": "Certificate"
        },
        {
            "name": "Connection"
        },
        {
            "name": "ConnectionLimitRule"
        },
        {
            "name": "Consumer"
        },
        {
            "name": "Domain"
        },
        {
            "name": "Exchange"
        },
        {
            "name": "Group"
        },
        {
            "name": "GroupMember"
        },
        {
            "name": "GroupProvider"
        },
        {
            "name": "KeyStore"
        },
        {
            "name": "Plugin"
        },
        {
            "name": "Port"
        },
        {
            "name": "Queue"
        },
        {
            "name": "RemoteReplicationNode"
        },
        {
            "name": "Session"
        },
        {
            "name": "TrustStore"
        },
        {
            "name": "User"
        },
        {
            "name": "VirtualHost"
        },
        {
            "name": "VirtualHostAccessControlProvider"
        },
        {
            "name": "VirtualHostAlias"
        },
        {
            "name": "VirtualHostConnectionLimitProvider"
        },
        {
            "name": "VirtualHostLogInclusionRule"
        },
        {
            "name": "VirtualHostLogger"
        },
        {
            "name": "VirtualHostNode"
        }
    ],
    "total": 31
}
```
  </p>
</details>

In addition to the object types supported by broker query REST API, following object types (domains) can be used as well:

| Domain              |
|---------------------|
| AclRule             | 
| Binding             | 
| Certificate         | 
| ConnectionLimitRule |
| Domain              |

Those objects do not belong to the broker object hierarchy (as they don't descend from ConfiguredObject), they were 
added to make queries against listed domains more simple.

For example, following query
```
SELECT *
FROM AclRule
WHERE identity = 'amqp_user1'
```

<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "identity": "amqp_user1",
            "attributes": {},
            "objectType": "Virtualhost",
            "operation": "Access",
            "outcome": "ALLOW_LOG"
        },
        {
            "identity": "amqp_user1",
            "attributes": {
                "NAME": "request.amqp_user1",
                "ROUTING_KEY": "*"
            },
            "objectType": "Exchange",
            "operation": "Publish",
            "outcome": "ALLOW"
        },
        {
            "identity": "amqp_user1",
            "attributes": {
                "NAME": "broadcast.amqp_user1.*"
            },
            "objectType": "Queue",
            "operation": "Consume",
            "outcome": "ALLOW_LOG"
        },
        {
            "identity": "amqp_user1",
            "attributes": {
                "NAME": "response.amqp_user1"
            },
            "objectType": "Queue",
            "operation": "Consume",
            "outcome": "ALLOW_LOG"
        }
    ],
    "total": 4
}
```
  </p>
</details>

Please note, that keyword `FROM` isn't mandatory, it is possible to execute queries without it, when the result shouldn't
retrieve any data from broker. Few examples of such queries would be:

###### Returns current timestamp
```
SELECT CURRENT_TIMESTAMP()
```

###### Returns current date
```
SELECT DATE(CURRENT_TIMESTAMP())
```

###### Returns result of an arithmetic expression
```
SELECT (2 + 10) / 3
```

###### Returns result of a logic expression
```
SELECT 2 * 5 > 12
```

## Filtering results

Filtering is achieved by using different operators groups in a `WHERE` clause. Operators can be divided into comparison
operators, conditional operators and logical operators.

### Broker Data Types

Broker entities have fields belonging to different java types: primitives (boolean, int, long, double), strings, 
datetime ([Date](https://docs.oracle.com/javase/8/docs/api/java/util/Date.html), 
[LocalDate](https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html),
[LocalDateTime](https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html),
[Instant](https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html)). 
Object IDs are usually of [UUID](https://docs.oracle.com/javase/8/docs/api/java/util/UUID.html) type.
Many values are enums.

When comparing field values, they follow some implicit casting rules: enums and UUIDs are cast to strings, 
datetime values are cast to [Instant](https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html),
numeric values are cast to [BigDecimal](https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html).

When casting string value to date, by default is used pattern `uuuu-MM-dd`.

That allows to run following queries:

```
SELECT *
FROM certificate
WHERE DATE(validUntil) = '2020-12-31'
```

Here string value is implicitly cast to Instant and both value are compared as Instant instances.

When casting string to datetime, by default is used pattern `uuuu-MM-dd HH:mm:ss` with optional 0-6 second fractions.

That allows to run following queries:

```
SELECT *
FROM certificate
WHERE DATE(validUntil) > '2020-12-31 23:59:59.999'
```

Here string value is implicitly cast to Instant as well and both value are compared as Instant instances.

It is important to compare values of the same type, otherwise an error may be returned or query may be evaluated 
erroneously. For example, following query

```
SELECT *
FROM queue
WHERE durable = 'true'
```

will return an empty result, because field `durable` is of boolean type and comparing boolean value with a string 'true'
will always return false. The correct query should be

```
SELECT *
FROM queue
WHERE durable = true
```

### Keyword DISTINCT

To remove duplicates from the results keyword `DISTINCT` can be used. For example, query
```
SELECT overflowPolicy 
FROM queue
```
will return results for all queues, but query
```
SELECT DISTINCT overflowPolicy 
FROM queue
```
will return only several values.

### Comparison Operators

#### BETWEEN

##### Definition and Usage

The BETWEEN operator selects values within a given range. The values can be numbers, text, or dates. The BETWEEN 
operator is inclusive: begin and end values are included.

##### Syntax

`BETWEEN(expression1 AND expression2)`

`BETWEEN(expression1, expression2)`

`BETWEEN expression1 AND expression2`

`BETWEEN expression1, expression2`

##### Parameter Values

| Parameter   | Description      |
|-------------|------------------|
| expression1 | Lower threshold  |
| expression2 | Higher threshold |

##### Examples

###### Find names of the queues having depth in messages between 1000 and 2000
```
SELECT 
    name
FROM queue
WHERE queueDepthMessages BETWEEN (1000, 2000)
```

###### Find certificates expiring between 2024-12-01 and 2024-12-31
```
SELECT * 
FROM certificate 
WHERE DATE(validUntil) BETWEEN ('2024-12-01' AND '2024-12-31')
```

#### EQUAL

##### Definition and Usage

Equal operator is designated using `=` character. It allows comparison of boolean, datetime, numeric and string values.
Both compared values must have same type.

##### Syntax

`expression1 = expression2`

##### Parameter Values

| Parameter   | Description                |
|-------------|----------------------------|
| expression1 | Expression to compare to   |
| expression2 | Expression to compare with |

##### Examples

###### Find queue by name
```
SELECT *
FROM queue
WHERE name = 'broadcast.amqp_user1.Public'
```

#### GREATER THAN

##### Definition and Usage

Greater than operator is designated using `>` character. It allows comparison of datetime, numeric and string values.
Both compared values must have same type.

##### Syntax

`expression1 > expression2`

##### Parameter Values

| Parameter   | Description                |
|-------------|----------------------------|
| expression1 | Expression to compare to   |
| expression2 | Expression to compare with |

##### Examples

###### Find queues having message depth greater than 1000
```
SELECT *
FROM queue
WHERE queueDepthMessages > 1000
```

#### GREATER THAN OR EQUAL

##### Definition and Usage

Greater than or equal operator is designated using `>=` characters. It allows comparison of datetime, numeric and string values.
Both compared values must have same type.

##### Syntax

`expression1 >= expression2`

##### Parameter Values

| Parameter   | Description                |
|-------------|----------------------------|
| expression1 | Expression to compare to   |
| expression2 | Expression to compare with |

##### Examples

###### Find queues having message depth greater or equal to 1000
```
SELECT *
FROM queue
WHERE queueDepthMessages >= 1000
```

#### IN

##### Definition and Usage

The IN operator allows specifying multiple values in a WHERE clause. The IN operator is a shorthand for multiple 
OR conditions. 

Alternatively IN operator can be used with a subquery. When a subquery is used, it should return only one value, 
otherwise an error will be returned.

##### Syntax

`expression IN (value_1, value_2, ..., value_n)`

or

`expression IN (SELECT value FROM domain)`

##### Parameter Values

| Parameter         | Description              |
|-------------------|--------------------------|
| expression        | Expression to compare to |
| value_1 - value_n | Values to compare with   |

##### Examples

###### Find bindings having destination queue belonging to the list
```
SELECT * 
FROM binding 
WHERE destination IN ('broadcast.amqp_user1.Service1', 'broadcast.amqp_user1.Service2', 'broadcast.amqp_user1.Service3')
```

###### Find bindings having destination queue with message depth between 1000 and 2000
```
SELECT * 
FROM binding 
WHERE destination IN (SELECT name FROM queue WHERE queueDepthMessages BETWEEN (1000, 2000))
```

#### IS NULL

##### Definition and Usage

The IS NULL operator is used to compare ordinary values with NULL values.

##### Syntax

`expression IS NULL`

##### Parameter Values

| Parameter         | Description                   |
|-------------------|-------------------------------|
| expression        | Expression to compare to NULL |

##### Examples

###### Find queues having NULL description
```
SELECT * 
FROM queue 
WHERE description IS NULL
```

#### LESS THAN

##### Definition and Usage

Less than operator is designated using `<` character. It allows comparison of datetime, numeric and string values.
Both compared values must have same type.

##### Syntax

`expression1 < expression2`

##### Parameter Values

| Parameter   | Description                |
|-------------|----------------------------|
| expression1 | Expression to compare to   |
| expression2 | Expression to compare with |

##### Examples

###### Find queues having message depth less than 1000
```
SELECT *
FROM queue
WHERE queueDepthMessages < 1000
```

#### LESS THAN OR EQUAL

##### Definition and Usage

Less than or equal operator is designated using `<=` characters. It allows comparison of datetime, numeric and string values.
Both compared values must have same type.

##### Syntax

`expression1 <= expression2`

##### Parameter Values

| Parameter   | Description                |
|-------------|----------------------------|
| expression1 | Expression to compare to   |
| expression2 | Expression to compare with |

##### Examples

###### Find queues having message depth less than or equal to 1000
```
SELECT *
FROM queue
WHERE queueDepthMessages <= 1000
```

#### LIKE

##### Definition and Usage

The LIKE operator is used to search for a specified pattern in a string. There are two wildcards often used in 
conjunction with the LIKE operator: the percent sign `%` represents zero, one, or multiple characters; the question 
mark `?` represents one, single character.

##### Syntax

`expression LIKE pattern`

`expression LIKE pattern ESCAPE escapeCharacter`

`expression LIKE (pattern)`

`expression LIKE (pattern ESCAPE escapeCharacter)`

##### Parameter Values

| Parameter       | Description                                            |
|-----------------|--------------------------------------------------------|
| expression      | Expression to compare to                               |
| pattern         | Pattern to compare against                             |
| escapeCharacter | Character used to escape percent sign or question mark |

##### Examples

###### Find queues having name starting with a string "broadcast"
```
SELECT *
FROM queue
WHERE name LIKE 'broadcast%'
```

###### Find queues with name containing string "amqp_user1"
```
SELECT *
FROM queue
WHERE name LIKE '%amqp_user1%'
```

### Conditional Operators

#### CASE

##### Definition and Usage

The CASE statement goes through conditions and returns a value when the first condition is met (like an if-then-else 
statement). So, once a condition is true, it will stop reading and return the result. If no conditions are true, 
it returns the value in the ELSE clause.

##### Syntax

```
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    WHEN conditionN THEN resultN
    ELSE result
END
```

##### Parameter Values

| Parameter               | Description                                            |
|-------------------------|--------------------------------------------------------|
| condition1 - conditionN | Conditions to estimate                                 |
| result1 - resultN       | Results to return                                      |

##### Examples

###### Group queues into good (< 60% of max depth), bad (60% - 90% of max depth) and critical (> 90% of max depth), count number of queues in each group. Consider queues with unlimited depth being good.
```
SELECT 
    COUNT(*), 
    CASE 
        WHEN maximumQueueDepthMessages != -1 AND maximumQueueDepthBytes != -1 
            AND (queueDepthMessages > maximumQueueDepthMessages * 0.9 OR queueDepthBytes > maximumQueueDepthBytes * 0.9) 
        THEN 'critical' 
        WHEN maximumQueueDepthMessages != -1 AND maximumQueueDepthBytes != -1 
            AND queueDepthMessages BETWEEN (maximumQueueDepthMessages * 0.6 AND maximumQueueDepthMessages * 0.9) 
            OR queueDepthBytes BETWEEN (maximumQueueDepthBytes * 0.6 AND maximumQueueDepthBytes * 0.9) 
        THEN 'bad' 
        ELSE 'good' 
    END AS queueState 
FROM queue 
GROUP BY queueState
```

### Logical Operators

The `AND` and `OR` operators are used to filter records based on more than one condition: the `AND` operator displays a record
if all the conditions separated by `AND` are TRUE. The `OR` operator displays a record if any of the conditions separated 
by `OR` is TRUE. The `NOT` operator displays a record if the condition(s) is NOT TRUE.

## Sorting results

Default sorting order is ascending, default sorting field is `name` for domains having this field. Results of the following query will be sorted ascending by name:

```
SELECT *
FROM queue
```

Few exceptions are following:

| Domain              | Default sorting field |
|---------------------|-----------------------|
| AclRule             | identity              |
| Certificate         | alias                 |
| ConnectionLimitRule | identity              |

Results of the following query will be sorted ascending by alias:

```
SELECT *
FROM certificate
```

To apply another sorting rules clause `ORDER BY` should be used. It may contain one of the fields specified in the `SELECT` clause:

```
SELECT 
    id, name, state
FROM queue 
ORDER BY name
```

Alternatively it may contain fields not specified in `SELECT` clause:

```
SELECT 
    id, name, state
FROM queue 
ORDER BY overflowPolicy
```

Instead of using field names or aliases items in the `ORDER BY` clause can also be referenced by ordinal - the numeric 
value of their order of appearance in the `SELECT` clause. For example, following query

```
SELECT 
   name, overflowPolicy
FROM queue
ORDER BY 2 DESC, 1 ASC
```
will return results sorted in descending order by overflow policy and inside the groups with the same overflow policy 
name results will be sorted by queue name in ascending order.

## Aggregation

Aggregation is achieved using functions [AVG()](#avg), [COUNT()](#count), [MAX()](#max), [MIN()](#min) and [SUM()](#sum).

It's important to remember, that aggregation functions don't consider NULL values. For example, following query
```
SELECT COUNT(description)
FROM queue
```
will return count of queues having non-null value of a field `description`.

To consider NULL values, they should be handled using [COALESCE()](#coalesce) function or [CASE](#case)
operator:
```
SELECT COUNT(COALESCE(description, ''))
FROM queue
```
Alternatively
```
SELECT COUNT(CASE WHEN description IS NULL THEN '' ELSE description END)
FROM queue
```

Several aggregation functions can be used together in the same query:
```
SELECT 
   COUNT(*), 
   AVG(queueDepthMessages), 
   SUM(queueDepthMessages), 
   SUM(queueDepthBytes), 
   MIN(queueDepthMessages), 
   MAX(queueDepthMessages), 
   MIN(queueDepthBytes), 
   MAX(queueDepthBytes) 
FROM queue
```

## Grouping

Grouping of the aggregated results can be achieved using the `GROUP BY` clause.

##### Examples

###### Find count of ACL rules for each user and output them in descending order
```
SELECT 
    COUNT(*) AS cnt, identity
FROM aclrule
GROUP BY identity 
ORDER BY 1 DESC
```
###### Result

<details>
  <summary>(Click to expand).</summary>
  <p>

```
{
  "results": [
    {
      "cnt": {
        "amqp_user1": 6,
        "amqp_user2": 4,
        "amqp_user3": 4,
        ... some results ommited ...
        "amqp_user97": 2,
        "amqp_user98": 1,
        "amqp_user99": 1
      }
    }
  ],
  "total": 1
}
```
  </p>
</details>

To filter the grouped result `HAVING` clause can be used:
```
SELECT
   overflowPolicy, COUNT(*) 
FROM queue 
GROUP BY overflowPolicy 
HAVING SUM(queueDepthMessages) > 1000
```


## Functions

### Aggregation Functions

#### AVG

##### Definition and Usage

The AVG() function returns the average value of a collection.

##### Syntax

`AVG(expression)`

##### Parameter Values

| Parameter  | Description                                              |
|------------|----------------------------------------------------------|
| expression | Expression result average value of which should be found |

##### Examples

###### Find average amount of bytes used by queues with names starting with "broadcast"
```
SELECT 
    AVG(queueDepthBytes)
FROM queue
WHERE name LIKE 'broadcast%'
```

#### COUNT

##### Definition and Usage

The COUNT() function returns the number of items that matches a specified criterion.

##### Syntax

`COUNT(expression)` or `COUNT(DISTINCT expression)`

##### Parameter Values

| Parameter  | Description                                  |
|------------|----------------------------------------------|
| expression | Expression result of which should be counted |

##### Examples

###### Find amount of queues with names starting with "broadcast"
```
SELECT 
    COUNT (*)
FROM queue
WHERE name LIKE 'broadcast%'
```

#### MAX

##### Definition and Usage

The MAX() function returns the maximum value of a collection.

##### Syntax

`MAX(expression)`

##### Parameter Values

| Parameter  | Description                                              |
|------------|----------------------------------------------------------|
| expression | Expression result maximal value of which should be found |

##### Examples

###### Find maximal amount of bytes used by queues with names starting with "broadcast"
```
SELECT 
    MAX(queueDepthBytes)
FROM queue
WHERE name LIKE 'broadcast%'
```

#### MIN

##### Definition and Usage

The MIN() function returns the minimum value of a collection.

##### Syntax

`MIN(expression)`

##### Parameter Values

| Parameter  | Description                                              |
|------------|----------------------------------------------------------|
| expression | Expression result minimal value of which should be found |

##### Examples

###### Find minimal amount of bytes used by queues with names starting with "broadcast"
```
SELECT 
    MIN(queueDepthBytes)
FROM queue
WHERE name LIKE 'broadcast%'
```

#### SUM

##### Definition and Usage

The SUM() function returns the total sum of a numeric collection.

##### Syntax

`SUM(expression)`

##### Parameter Values

| Parameter  | Description                                 |
|------------|---------------------------------------------|
| expression | Expression result of which should be summed |

##### Examples

###### Find amount of bytes used by queues having names starting with "broadcast"
```
SELECT 
    SUM(queueDepthBytes)
FROM queue
WHERE name LIKE 'broadcast%'
```

### Datetime Functions

#### CURRENT_TIMESTAMP

##### Definition and Usage

The CURRENT_TIMESTAMP() function returns current date and time.

##### Syntax

`CURRENT_TIMESTAMP()`

##### Parameter Values

Function has no parameters

##### Examples

###### Find current date and time
```
SELECT CURRENT_TIMESTAMP()
```

#### DATE

##### Definition and Usage

The DATE() function extracts the date part from a datetime expression.

##### Syntax

`DATE(expression)`

##### Parameter Values

| Parameter  | Description                 |
|------------|-----------------------------|
| expression | A valid date/datetime value |

##### Examples

###### Find certificates having validFrom equal to 01. January 2020
```
SELECT * 
FROM certificate 
WHERE DATE(validFrom) = '2020-01-01'
```

###### Find certificates expiring between 01. January 2020 and 10. January 2020
```
SELECT * 
FROM certificate 
WHERE DATE(validUntil) BETWEEN ('2020-01-01', '2020-01-10')
```

#### DATEADD

##### Definition and Usage

The DATEADD() function adds a time/date interval to a date and returns the date.

##### Syntax

`DATEADD(TIMEUNIT, VALUE, DATE)`

##### Parameter Values

| Parameter | Description                                                                                                                           |
|-----------|---------------------------------------------------------------------------------------------------------------------------------------|
| TIMEUNIT  | The type of time unit to add. <br/>Can be one of the following values: <br/>YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND |
| VALUE     | The value of the time/date interval to add. Both positive and negative values are allowed                                             |
| DATE      | The date to be modified                                                                                                               |


##### Examples

###### Find certificates expiring in less than 30 days
```
SELECT * 
FROM certificate 
WHERE DATEADD(DAY, -30, validUntil) < CURRENT_TIMESTAMP()
LIMIT 10 OFFSET 0
```

#### DATEDIFF

##### Definition and Usage

The DATEDIFF() function returns the number of time units between two date values.

##### Syntax

`DATEDIFF(TIMEUNIT, DATE1, DATE2)`

##### Parameter Values

| Parameter | Description                                                                                                                                |
|-----------|--------------------------------------------------------------------------------------------------------------------------------------------|
| TIMEUNIT  | Time unit to calculate difference. <br/>Can be one of the following values: <br/>YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND |
| DATE1     | Start date                                                                                                                                 |
| DATE2     | End date                                                                                                                                   |

##### Examples

###### Find certificate aliases and days until expiry
```
SELECT 
    alias, 
    DATEDIFF(DAY, CURRENT_TIMESTAMP(), validUntil) AS days_until_expiry
FROM certificate 
LIMIT 10 OFFSET 0
```

#### EXTRACT

##### Definition and Usage

The EXTRACT() function extracts a part from a given date.

##### Syntax

`EXTRACT(TIMEUNIT FROM DATE)`

##### Parameter Values

| Parameter | Description                                                                                                                   |
|-----------|-------------------------------------------------------------------------------------------------------------------------------|
| TIMEUNIT  | Time unit to extract. <br/>Can be one of the following values: <br/>YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND |
| DATE      | The date to extract a part from                                                                                               |

##### Examples

###### Find certificates issued in January 2020
```
SELECT *
FROM certificate 
WHERE EXTRACT(YEAR FROM validFrom) = 2020 
AND EXTRACT(MONTH FROM validFrom) = 1 
LIMIT 10 OFFSET 0
```

### Null Functions

#### COALESCE

##### Definition and Usage

The COALESCE() function returns the first non-null value in a list.

##### Syntax

`COALESCE(value_1, value_2, ...., value_n)`

##### Parameter Values

| Parameter         | Description        |
|-------------------|--------------------|
| value_1 - value_n | The values to test |

##### Examples

###### Find count of queues having NULL description
```
SELECT 
    COUNT(COALESCE(description, 'empty')) AS RESULT 
FROM queue 
HAVING COALESCE(description, 'empty') = 'empty' 
```

### Numeric Functions

#### ABS

##### Definition and Usage

The ABS() function returns the absolute value of a number.

##### Syntax

`ABS(number)`

##### Parameter Values

| Parameter | Description     |
|-----------|-----------------|
| number    | A numeric value |

##### Examples

###### Find absolute amount of days after the validFrom date of the certificates
```
SELECT 
    ABS(DATEDIFF(DAY, CURRENT_TIMESTAMP(), validFrom)) 
FROM certificate
```

#### ROUND

##### Definition and Usage

The ROUND() function takes a numeric parameter and rounds it to the specified number of decimal places.

##### Syntax

`ROUND(number, decimals)`

##### Parameter Values

| Parameter | Description                              |
|-----------|------------------------------------------|
| number    | The number to be rounded                 |
| decimals  | The number of decimal places to round to |

##### Examples

###### Find average queue depth in messages and round result to 2 decimal places
```
SELECT 
    ROUND(AVG(queueDepthMessages)) as result 
FROM queue
```

#### TRUNC

##### Definition and Usage

The TRUNC() function takes a numeric parameter and truncates it to the specified number of decimal places.

##### Syntax

`TRUNC(number, decimals)`

##### Parameter Values

| Parameter | Description                                 |
|-----------|---------------------------------------------|
| number    | The number to be truncated                  |
| decimals  | The number of decimal places to truncate to |

##### Examples

###### Find average queue depth in messages and truncate result to 2 decimal places
```
SELECT 
    TRUNC(AVG(queueDepthMessages)) as result 
FROM queue
```

### String Functions

#### CONCAT

##### Definition and Usage

The CONCAT() function takes a variable number of arguments and concatenates them into a single string.
It requires a minimum of one input value, otherwise CONCAT will raise an error. CONCAT implicitly
converts all arguments to string types before concatenation. The implicit conversion to strings follows
the existing rules for data type conversions. If any argument is NULL, CONCAT returns NULL.

##### Syntax

`CONCAT(expression_1, expression_2, expression_3, ..., expression_n)`

##### Parameter Values

| Parameter                   | Description                      |
|-----------------------------|----------------------------------|
| expression_1 - expression_n | The expressions to add together. |

##### Examples

###### Output certificate alias and validity dates using format "alias: validFrom - validUntil"
```
SELECT 
    CONCAT(alias, ': ', DATE(validFrom), ' - ', DATE(validUntil)) as validity 
FROM certificate
```

#### LEN / LENGTH

##### Definition and Usage

The LEN() / LENGTH() function takes a string parameter and returns its length.
The implicit conversion to strings follows the existing rules for data
type conversions. If any argument is NULL, LEN / LENGTH returns 0.

##### Syntax

`LEN(string)` or `LENGTH(string)`

##### Parameter Values

| Parameter | Description                         |
|-----------|-------------------------------------|
| string    | The string to count the length for. |

##### Examples

###### Find certificate aliases having alias length greater than 10
```
SELECT 
    alias 
FROM certificate 
WHERE LENGTH(alias) > 10 
LIMIT 10 OFFSET 0
```

#### LOWER

##### Definition and Usage

The LOWER() function takes a string parameter and converts it to lower case. The implicit conversion to strings follows
the existing rules for data type conversions. If argument is NULL, LOWER returns NULL.

##### Syntax

`LOWER(string)`

##### Parameter Values

| Parameter | Description            |
|-----------|------------------------|
| string    | The string to convert. |

##### Examples

###### Filter connections by principal name (case-insensitive)
```
SELECT * 
FROM connection 
WHERE LOWER(principal) = 'amqp_user1'
LIMIT 10 OFFSET 0
```

#### LTRIM

##### Definition and Usage

The LTRIM() removes leading spaces from a string. If argument is NULL, LTRIM returns NULL.

##### Syntax

`LTRIM(string)` or `LTRIM(string, chars)`

##### Parameter Values

| Parameter | Description                               |
|-----------|-------------------------------------------|
| string    | The string to remove leading spaces from. |
| chars     | Specific characters to remove.            |

##### Examples

###### Find connection remote addresses removing `/` characters from the left side
```
SELECT 
   LTRIM(remoteAddress, '/') AS remoteAddress 
FROM connection"
```

#### POSITION

##### Definition and Usage

The POSITION() function takes a search pattern and a source string as parameters and returns the position of the first occurrence
of a pattern in a source string. If the pattern is not found within the source string, this function returns 0.
Optionally takes third integer parameter, defining from which position search should be started. Third parameter
should be an integer greater than 0. If source string is NULL, returns zero.

##### Syntax

`POSITION(pattern IN source)` or `POSITION(pattern IN source, startIndex)`

##### Parameter Values

| Parameter  | Description                                  |
|------------|----------------------------------------------|
| pattern    | The pattern to search for in source.         |
| source     | The original string that will be searched.   |
| startIndex | The index from which search will be started. |

##### Examples

###### Find queues having string "broadcast" in their names
```
SELECT * 
FROM queue 
WHERE POSITION('broadcast', name) > 0 
LIMIT 10 OFFSET 0
```

#### REPLACE

##### Definition and Usage

The REPLACE() function replaces all occurrences of a substring within
a string, with a new substring. If source string is NULL, returns NULL.

##### Syntax

`REPLACE(source, pattern, replacement)`

##### Parameter Values

| Parameter   | Description                    |
|-------------|--------------------------------|
| source      | The original string.           |
| pattern     | The substring to be replaced.  |
| replacement | The new replacement substring. |

##### Examples

###### Output certificate issuer names without leading "CN="
```
SELECT 
    REPLACE(issuerName, 'CN=', '') AS issuer 
FROM certificate 
LIMIT 10 OFFSET 0
```

#### RTRIM

##### Definition and Usage

The RTRIM() function removes trailing spaces from a string. If argument is NULL, RTRIM returns NULL.

##### Syntax

`RTRIM(string)` or `RTRIM(string, chars)`

##### Parameter Values

| Parameter | Description                                |
|-----------|--------------------------------------------|
| string    | The string to remove trailing spaces from. |
| chars     | Specific characters to remove.             |

##### Examples

###### Find connection remote addresses
```
SELECT
   RTRIM(remoteAddress)
FROM connection
```

#### SUBSTR / SUBSTRING

##### Definition and Usage

The SUBSTRING() function takes a source parameter, a start index parameter and optional length parameter. Returns substring
of a source string from the start index to the end or using the length parameter. If source string is NULL,
return NULL.

##### Syntax

`SUBSTRING(source, startIndex, length)`

##### Parameter Values

| Parameter  | Description                                                                                                                                                                                                                   |
|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| source     | The string to extract from.                                                                                                                                                                                                   |
| startIndex | The start position. Can be both a positive or negative number. If it is a positive number, this function extracts from the beginning of the string. If it is a negative number, function extracts from the end of the string. |
| length     | The number of characters to extract. If omitted, the whole string will be returned (from the start position). If zero or negative, an empty string is returned.                                                               |

##### Examples

###### Find queue names removing from name part before the `.` character
```
SELECT 
   SUBSTRING(name, POSITION('.', name) + 1, LEN(name) - POSITION('.', name)) 
FROM queue
```

#### TRIM

The TRIM() function removes both leading and trailing spaces from a string.
If argument is NULL, TRIM returns NULL.

##### Syntax

`TRIM(string)` or `TRIM(string, chars)`

##### Parameter Values

| Parameter | Description                                            |
|-----------|--------------------------------------------------------|
| string    | The string to remove leading and trailing spaces from. |
| chars     | Specific characters to remove.                         |

##### Examples

###### Find connections remote addresses removing `/` characters from both sides
```
SELECT 
   TRIM(remoteAddress, '/') 
FROM connection
```

#### UPPER

##### Definition and Usage

The UPPER() function takes a string parameter and converts it to upper case. The implicit conversion to strings follows
the existing rules for data type conversions. If argument is NULL, UPPER returns NULL.

##### Syntax

`UPPER(string)`

##### Parameter Values

| Parameter | Description            |
|-----------|------------------------|
| string    | The string to convert. |

##### Examples

###### Filter connections by principal name (case-insensitive)
```
SELECT * 
FROM connection 
WHERE UPPER(principal) = 'AMQP_USER1'
LIMIT 10 OFFSET 0
```

## Set Operations

UNION, MINUS and INTERSECT set operations are supported.

The UNION operator is used to combine the result-set of two or more SELECT statements. Every SELECT statement within 
UNION must have the same number of columns. The UNION operator selects distinct values by default.
To keep duplicates, UNION ALL should be used.

For example, following query return certificate aliases along with the user names:
```
SELECT UPPER(alias) 
FROM certificate 
UNION 
SELECT UPPER(name) 
FROM user
```

The MINUS operator is used to remove the results of right SELECT statement from the results of left SELECT statement.
Every SELECT statement within MINUS must have the same number of columns. The MINUS operator selects distinct values by default. 
To eliminate duplicates, MINUS ALL should be used.

For example, following query finds queue names, not specified as binding destinations:
```
SELECT name 
FROM queue 
MINUS 
SELECT destination 
FROM binding
```

The INTERSECT operation is used to retain the results of right SELECT statement present in the results of left SELECT statement.
Every SELECT statement within INTERSECT must have the same number of columns. The INTERSECT operator selects distinct values by default.
to eliminate duplicates, INTERSECT ALL should be used.

For example, following query finds certificate aliases similar with the user names
```
SELECT UPPER(alias) 
FROM certificate 
INTERSECT 
SELECT UPPER(name) 
FROM user
```

## Subqueries

When executing subquery parent query domain mat be passed into the subquery using alias.

E.g. this query

```
SELECT 
   id, 
   name, 
   (SELECT name FROM connection WHERE SUBSTRING(name, 1, POSITION(']' IN name)) = '[' + SUBSTRING(c.name, 1, POSITION('|' IN c.name) - 1) + ']') as connection, 
   (SELECT id FROM connection WHERE SUBSTRING(name, 1, POSITION(']' IN name)) = '[' + SUBSTRING(c.name, 1, POSITION('|' IN c.name) - 1) + ']') as connectionId, 
   (SELECT name FROM session WHERE id = c.session.id) as session 
FROM consumer c
```

<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "id": "7a4d7a86-652b-4112-b535-61272b936b57",
            "name": "1|1|qpid-jms:receiver:ID:6bd18833-3c96-4936-b9ee-9dec5f408b5c:1:1:1:broadcast.amqp_user1.public",
            "connection": "[1] 127.0.0.1:39134",
            "connectionId": "afbd0480-43b1-4b39-bc00-260c077095f3",
            "session": "1"
        }
    ],
    "total": 1
}
```
  </p>
</details>

Query 

```
SELECT 
   name, 
   destination, 
   (SELECT id FROM queue WHERE name = b.destination) AS destinationId, 
   exchange,  
   (SELECT id FROM exchange WHERE name = b.exchange) AS exchangeId 
FROM binding b 
WHERE name = 'broadcast.amqp_user1.xxx.#'
```
<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "name": "broadcast.amqp_user1.xxx.#",
            "destination": "broadcast.amqp_user1.xxx",
            "destinationId": "d5ce9e78-8558-40db-8690-15abf69ab255",
            "exchange": "broadcast",
            "exchangeId": "470273aa-7243-4cb7-80ec-13e698c36158"
        },
        {
            "name": "broadcast.amqp_user1.xxx.#",
            "destination": "broadcast.amqp_user2.xxx",
            "destinationId": "88357d15-a590-4ccf-aee8-2d5cda77752e",
            "exchange": "broadcast",
            "exchangeId": "470273aa-7243-4cb7-80ec-13e698c36158"
        },
        {
            "name": "broadcast.amqp_user1.xxx.#",
            "destination": "broadcast.amqp_user3.xxx",
            "destinationId": "c8200f89-2587-4b0c-a8f6-120cda975d03",
            "exchange": "broadcast",
            "exchangeId": "470273aa-7243-4cb7-80ec-13e698c36158"
        }
    ],
    "total": 3
}
```
  </p>
</details>

Query

```
SELECT 
   alias, 
   (SELECT COUNT(id) FROM queue WHERE POSITION(UPPER(c.alias) IN name) > 0) AS queueCount 
FROM certificate c
```
<details>
  <summary>returns following result (Click to expand).</summary>
  <p>

```
{
    "results": [
        {
            "alias": "xxx",
            "queueCount": 5
        },
        {
            "alias": "xxy",
            "queueCount": 5
        },
        {
            "alias": "xxz",
            "queueCount": 7
        }
    ],
    "total": 3
}
```
  </p>
</details>


Please consider that subquery usage may be not performant in case of many objects returned
either by parent query or by subquery.

## Performance Tips

Try to select entity fields by names instead of using an asterix. For example, this query
```
SELECT 
    id, name, state, overflowPolicy, expiryPolicy
FROM queue
```
will be executed faster than this one:
```
SELECT *
FROM queue
```
Try to use `LIMIT` and `OFFSET` clauses where applicable to reduce the response JSON size:
```
SELECT 
    id, name, state, overflowPolicy, expiryPolicy
FROM queue
LIMIT 10 OFFSET 0
```
When using subqueries avoid to fire the against unfiltered domains. For example, this query
```
SELECT 
   name, 
   (SELECT id FROM queue WHERE name = b.destination) AS destinationId
FROM binding b 
WHERE name = 'broadcast.amqp_user1.xxx.#'
```
will be executed faster than this one:
```
SELECT 
   name, 
   (SELECT id FROM queue WHERE name = b.destination) AS destinationId
FROM binding b 
```