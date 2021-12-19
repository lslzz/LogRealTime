package com.cetecloud.logalarm.utils;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.cetecloud.logalarm.beans.MyDeserializationSchema;
import com.cetecloud.logalarm.common.LogAlarmConfig;


import java.util.Properties;

/**
 * @author lslzz
 */
public class MySqlUtils {


    public static DebeziumSourceFunction<String> getMySqlSourceFunction(){

        Properties debezProp = new Properties();
        debezProp.setProperty("debezium.snapshot.locking.mode","none");
        //System.setProperty("HADOOP_USER_NAME","atguigu");
        DebeziumSourceFunction<String> mySqlSource = MySQLSource
                .<String>builder()
                .hostname(LogAlarmConfig.HOSTNAME)
                .port(LogAlarmConfig.MYSQL_PORT)
                .username(LogAlarmConfig.MYSQL_USER_NAME)
                .password(LogAlarmConfig.MYSQL_PASS_WORD)
                .databaseList("gmall0408_realtime")
                .tableList("gmall0408_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debezProp)
                .deserializer(new MyDeserializationSchema())
                //.deserializer(new StringDebeziumDeserializationSchema())
                .build();
        return mySqlSource;
    }
}
