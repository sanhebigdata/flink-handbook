package com.sanhe.cdc.sql.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlSourceExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE mysql_binlog (\n" +
                " db_name STRING METADATA FROM 'database_name' VIRTUAL,\n" +
                "    table_name STRING METADATA  FROM 'table_name' VIRTUAL,\n" +
                "    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n" +
                "    order_id INT,\n" +
                "    order_date TIMESTAMP(0),\n" +
                "    customer_name STRING,\n" +
                "    price DECIMAL(10, 5),\n" +
                "    product_id INT,\n" +
                "    order_status BOOLEAN,\n" +
                "    PRIMARY KEY(order_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'sanhe',\n" +
                "    'password' = 'sanhe2023',\n" +
                "    'database-name' = 'cdc_db',\n" +
                "    'table-name' = 'products', \n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ");");

        tEnv.executeSql("select * from mysql_binlog ").print();


    }
}
