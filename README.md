jdbc2hive 
============

Feature
============

  * Support push DB related conditions in hive query to MySQL
  * Support fetch only required fields in MySQL to optimize performance
  * Support splited field to run multi maps
  * Use MySQL explain to estimate fetched rows
  * Just support MySQL now


Usage 
============


**Build**

    $ git clone 
    $ mvn clean -Dmaven.test.skip=true package

**Run query**

    $ hive -e 'add jar target/{build-with-distribute.jar}; {your own query}'


Configuration 
============

**Required**

  * `jdbc2hive.table.name` table name
  * `jdbc2hive.splited.by` field in table used by spliting to many maps, just support int, bigint, timestamp now
  * `jdbc2hive.jdbc.url` jdbc url configuration
  * `jdbc2hive.jdbc.class` jdbc class, just support `com.mysql.jdbc.Driver`

**Optional**

  * `jdbc2hive.column.map` hive to db field map; if hive field is same as db field, leave it blank
  * `jdbc2hive.value.trimnewline` define whether clean data, default is true.

Example
============


**Create table**

```
ADD JAR jdbc2hive.jar;

CREATE EXTERNAL TABLE if not exists jdbc2hive_example
(
 hive_id INT,
 name STRING,
 description STRING
)
STORED BY 'com.anjuke.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "jdbc2hive.jdbc.url" = "jdbc:mysql://127.0.0.1:3306/test_db?user=test&password=test&characterEncoding=utf8&tinyInt1isBit=false&zeroDateTimeBehavior=convertToNull",
    "jdbc2hive.jdbc.class" = "com.mysql.jdbc.Driver",
    "jdbc2hive.splited.by" = "id",
    "jdbc2hive.table.name" = "test_table",
    "jdbc2hive.column.map" = "hive_id=id"
);
        
```

**Usage**

```
ADD JAR jdbc2hive.jar;

SELECT COUNT(*) from jdbc2hive_example;
```
