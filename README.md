jdbc2hive 
============

Feature
============

  * Support push MySQL related condition in hive query to MySQL
  * Support fetch only required fields in MySQL to optimize performance
  * Support splited field to run multi maps
  * Use MySQL explain to estimate fetched rows
  * Just support MySQL now.


Usgage 
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
  * `jdbc2hive.splited.by` splited by
  * `jdbc2hive.jdbc.url` jdbc url configuration
  * `jdbc2hive.jdbc.class` jdbc class, just support `com.mysql.jdbc.Driver`

**Optional**

  * `jdbc2hive.column.map` hive to db field map; if hive field is same as db field, leave it blank
  * `jdbc2hive.value.trimnewline` define whether clean data, default is true.

Example
============


