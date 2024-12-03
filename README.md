# DLSync
DLSync is a database change management that deploys database changes to our database. 
Each object(view, table, udf ...) in our database will 
have a corresponding SQL script file where every change to this object is tracked in this file only. DLSync keeps track of what changes have been deployed to database 
by using hash. Hence DLSync is smart enough to identify what scripts have changed.git 
Using this DLSync only deploys changed script to database objects.
DLSync also understands interdependency between different scripts, thus applies these changes
according their dependency.
Based on how we define the changes to database objects, DLSync divides database object scripts to 2 types, State and migration scripts.
### 1. State Script
This type of script is used for object of Views, UDF, Stored Procedure. 
In this type of script we can update the current existing code and these changes will be reflected in our database by DLsyc by replacing the existing object with the new code of the script provided.
### 2. Migration Script
This type of script is used for object type of Tables, Sequences, Streams, Stages, Task. 
Here the script is treated as migration. Each migration script file has 1 or more migration code. One migration code contains version, author(optional), content, rollback(optional) and verify(optional).
The migration code are applied to the database object sequentially by the version number.
## Key Features 
- It combines state based and migration based change management to deploy
- Each object will have it's corresponding unique Script file where we can define the change to that object
- It can detect change between previous deployment and current script state.
- It can reorder scripts based on their dependency before deploying to database.
- It supports parametrization of scripts where we can define variables that change between different database instances.
- It supports parameter config file where each parameter config file corresponds to an instance 
- It supports rollback to previous deployment. 
- Rollback is very simple and intuitive. Only one needs to rollback git repository of the script and triggering rollback module.
- It supports verify module where each database object is checked with current script state to check for deployment verification or tracking out of sync database changes.
- It supports create script where we can create script file for each database objects.
- It supports lineage creation where we can visualize object dependencies

## How to use this tool
### File structure
To use this tool first create your script root directory.
This directory will contain all scripts and configurations.
Inside this directory create a directory structure like:
```
/script-root                                        # Root directory for the scripts
├── /main                                           # Main scripts for deployment 
│   ├── /database_name_1                            # Database name 
│   │   ├── /schema_name_1                          # database Schema name
│   │   │   ├── /[object_type]_1                    # Database Object type like (VIEWS, FUNCTIONS, TABLES ...)
│   │   │   │   ├── object_name_1.sql               # The database object name(table name, view name, function name ...)
│   │   │   │   ├── object_name_2.sql               # The database object name(table name, view name, function name ...)
│   │   │   ├── /[object_type]_2                    # Database Object type like (VIEWS, FUNCTIONS, TABLES ...)
│   │   │   │   ├── object_name_3.sql               # The database object name(table name, view name, function name ...)
│   │   │   │   ├── object_name_4.sql               # The database object name(table name, view name, function name ...)
│   │   ├── /schema_name_2                          # database Schema name
│   │   │   ├── /[object_type]_1                    # Database Object type like (VIEWS, FUNCTIONS, TABLES ...)
│   │   │   │   ├── object_name_5.sql               # The database object name(table name, view name, function name ...)
│   │   │   │   ├── object_name_6.sql               # The database object name(table name, view name, function name ...)
│   │   │   ├── /[object_type]_2                    # Database Object type like (VIEWS, FUNCTIONS, TABLES ...)
│   │   │   │   ├── object_name_7.sql               # The database object name(table name, view name, function name ...)
│   │   │   │   ├── object_name_8.sql               # The database object name(table name, view name, function name ...)
├── /tests                                          # SQL unit test scripts
├── config.yml                                      # configuration file
├── parameter-[profile-1].properties                # parameter property file  
├── parameter-[profile-2].properties                # parameter property file
└── parameter-[profile-3].properties                # parameter property file
```

Where 
- **database_name_*:** is the database name of your project, 
- **schema_name_*:** are schemas inside the database, 
- **object_type:** is type of the object only 1 of the following (VIEWS, FUNCTIONS, PROCEDURES, TABLES, SEQUENCES, STAGES, STREAMS, TASKS)
- **object_name_*.sql:** are individual database object scripts.
- **config.yml:** is a configuration file used to configure DLSync behavior.
- **parameter-[profile-*].properties:** is parameter to value map file. This is going to be used by corresponding individual instances of your database.
This property files will help you parametrize changing parameters and their value. For each deployment instance of your database(project) you should create a separate parameter profile property.
These property files should have names in the above format by replacing "format" by your deployment instance name.
where profile is the instance name of your database. you will provide the profile name in environment variable while running this tool.

### File content
The structure and content of the scripts will defer based on the type of script
#### State Script 
This script file is used for object types of views, functions and procedures.
Each object in your database will have its own sql file that tracks the change to that object.
The sql file should be named with the database object name. 
The State script file should adhere to the following rules
1. The file name should match database object name.
2. The file should contain only one SQL DDL script that creates the specified object.
3. The create script should refer the object with its full qualified name (database.schema.object_name)
And the content of this file should only have one sql script that creates this object on database.
#### Migration Script
This script file is used for object types of TABLES, SEQUENCES, STAGES, STREAMS and TASKS.
This script can contain multiple migration versions. Each migration script will contain header, a single DDL or DML change to the object, an optional rollback script, and an optional verify script.
The migration script will have the following format:
```
---version: 0, author: name
create or replace table my_db.my_schema.my_table(id varchar, my_column varchar);
---rollback: drop table if exists my_db.my_schema.my_table;
---verify: select * from my_db.my_schema.my_table;

---version: 1, author: name
insert into my_db.my_schema.my_table values('1', 'value');
---rollback: delete from my_db.my_schema.my_table where id = '1';
---verify: select * from my_db.my_schema.my_table where id = '1';
```

The migration script should adhere to the following rules:
1. Each change to database object should be wrapped in a migration format specified above.
2. Each migration should contain migration header (version and author) and the content of the migration, rollback(optional) and verify (optional).
3. migration header should start in a new line with three hyphens(---) and can contain only version and author.
4. Version should be unique number per each script file and should be incremental order. And it is used to order the scripts based on hierarchy
5. author is optional alphanumeric characters used for informational purpose only to track who added the changes.
6. Content of the change (migration) should be specified after migration header in a new line. And it can span multiple lines.
7. Content should always be terminated by semi-colon (;). 
8. Rollback if specified should start in a new line with "**---rollback:** ". The rollback script should be on a single line and must be terminated with semi-colon (;);
9. Verify if specified should start in a new line with "**---verify:** ". The verify script should be on a single line and must be terminated with semi-colon (;); 

#### parametrization of script
The script files defined can have parameters that change between different instances. The format for using parameters is:
```
${parameter_name}
```
where parameter_name is the name of parameter defined in the parameter-[profile].property file with its value.
For example,
```
create or replace view ${database}.${schema}.my_view as select * from ${db}.${schema}.my_table;
```
This script file is using two parameters, database and schema. Thus, their value should be specified in the parameter-[profile].property file. 
#### parameter-[profile].property file 
This file should contain the parameter used by the scripts. This is a property file where you define parameter and their values corresponding to an instance.
For example,
```
database=my_database_dev
schema=my_schema_dev
other_param=other_value
```
### Running The application
This tool requires the following environment variables in order to run:
```
profile=dev;    #To specifiy which parameter property file should use. (for this case There should be a file called parameter-dev.property)
script_root=path/to/db_scripts  #This should point to the scripts root directory directory
account=my_account  #account used for connection
db=database  #your database
schema=dl_sync  #your dl_sync schema. It will use this schema to store neccessary objects for this tool
user=user_name  #user name of the database
password=password #password for the connection
warehouse=my_warehouse #warehouse to be used by the connection
role=my_role    #role used by this tool
```
There are 4 main modules. Each module of the tool can be triggered from the command line argument.
#### Running Deploy
```
dlsync DEPLOY
```
#### Running Deploy without deploying object. (Used to mark scripts deployed prior this tool as deployed, so that DLSync won't replace existing database objects.)
```
dlsync DEPLOY --only-hashes
```
#### Running Rollback
```
dlsync ROLLBACK
```
#### Running verify
```
dlsync VERIFY
```
#### Running Create script
```
dlsync CREATE_SCRIPT
```
#### Running Create lineage
```
dlsync CREATE_LINEAGE
```

There is an example scripts provided in the directory ```example_scripts``` .