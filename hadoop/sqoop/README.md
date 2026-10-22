# MySQL and Sqoop acceptance module

The capstone requires machine master data, maintenance history and work orders to flow from MySQL to HDFS through Sqoop. This module provides the demo source schema and a repeatable import runner.

## Prepare MySQL

Run hadoop/sqoop/mysql_source_schema.sql against a demo MySQL instance. It creates the smart_manufacturing database and repeatable example rows.

Create a least-privilege MySQL account separately. Grant SELECT only on smart_manufacturing.machine_master, smart_manufacturing.maintenance_history, and smart_manufacturing.work_orders. Ensure MySQL Connector/J is available to the installed Sqoop runtime. Legacy Sqoop distributions may require an older Java runtime; use versions supported by your cluster.

## Store the Sqoop password securely

Create a file containing only the database password and put it on HDFS with restrictive permissions. Do not commit the password or place it in a command-line argument. Example (adapt paths and database account):

    umask 077
    printf '%s' "$MYSQL_PASSWORD" > /tmp/mysql-sqoop.pwd
    hdfs dfs -mkdir -p /user/$USER/.credentials
    hdfs dfs -put /tmp/mysql-sqoop.pwd /user/$USER/.credentials/mysql.pwd
    hdfs dfs -chmod 400 /user/$USER/.credentials/mysql.pwd
    rm -f /tmp/mysql-sqoop.pwd

Set MYSQL_PASSWORD securely before running the example and unset it afterwards. The password file is deliberately excluded from Git.

## Run imports

    export MYSQL_JDBC_URL='jdbc:mysql://localhost:3306/smart_manufacturing'
    export MYSQL_USER='sqoop_user'
    export SQOOP_PASSWORD_FILE="/user/$USER/.credentials/mysql.pwd"
    bash hadoop/sqoop/import_reference_tables.sh

The runner uses one mapper per table, imports into a unique timestamped HDFS path and refuses to overwrite an existing target. Expected output files are part-m-00000.

## Evidence required for acceptance

- mysql --version, sqoop version, hadoop version and yarn node -list.
- SELECT COUNT(*) for each source table.
- Sqoop logs showing all three table imports completed.
- hdfs dfs -count -q for each target and a small hdfs dfs -cat sample.
- Reconciliation comparing MySQL and HDFS row counts.

Source SQL and scripts are implementation artifacts. Mark Sqoop acceptance PASS only after running against working MySQL/HDFS/YARN/Sqoop services and comparing counts.
