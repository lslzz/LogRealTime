package com.cetecloud.logalarm.common;

/**
 * @author lslzz
 */
public class LogAlarmConfig {
    public static final String HBASE_SCHEMA = "GMALL0408_REALTIME";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";
    public static final String CLICKHOUSE_SERVER="jdbc:clickhouse://hadoop101:8123/default";
    public static final String ODS_BASE_DB_M_TOPIC = "ods_base_db_m";
    public static final String ODS_BASE_DB_M_GROUP_ID = "base_db_groupId";
    public static final String HOSTNAME = "hadoop101";
    public static final int MYSQL_PORT = 3306;
    public static final int REDIS_PORT = 6379;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASS_WORD = "123456";

}
