package com.cetecloud.logalarm.app.func;

import com.alibaba.fastjson.JSONObject;
import com.cetecloud.logalarm.common.LogAlarmConfig;
import com.cetecloud.logalarm.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

/**
 * @author lslzz
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled","true");
        conn = DriverManager.getConnection(LogAlarmConfig.PHOENIX_SERVER,properties);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        PreparedStatement ps = null;
        // 获取表名
        String tableName = "log_alarm_info";

        try {
            String upsertSql = getUpSertSql(tableName, jsonObj);
            System.out.println("upsertSql>>>" + upsertSql);
            ps = conn.prepareStatement(upsertSql);
            // 执行插入数据的sql
            ps.execute();
            // phoenix 不支持事务的自动提交,在这里要进行手动提交,事务只和DML操作有关
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("<<<<向Phoenix中插入数据失败>>>>");
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    /**
     * 获取拼接的upSertSQL
     *
     * @param tableName
     * @param data
     * @return
     */
    private String getUpSertSql(String tableName, JSONObject data) {
        // 获取keys 集合
        Set<String> keys = data.keySet();
        // 获取values 集合,直接取出默认是一一对应
        Collection<Object> values = data.values();
        String upsertSql = "upsert into " + LogAlarmConfig.HBASE_SCHEMA + ".\""+ tableName +"\""
                + "(" + StringUtils.join(keys, ",") + ")" + "values('"
                + StringUtils.join(values, "','") + "')";
        return upsertSql;
    }
}
