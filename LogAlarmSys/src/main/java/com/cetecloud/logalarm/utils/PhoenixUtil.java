package com.cetecloud.logalarm.utils;

import com.alibaba.fastjson.JSONObject;
import com.cetecloud.logalarm.common.LogAlarmConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lslzz
 * Phoenix 查询工具类
 */
public class PhoenixUtil {
    private static Connection conn;

    /**
     * 初始化连接
     */
    public static void queryInit() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(LogAlarmConfig.PHOENIX_SERVER);
            conn.setSchema(LogAlarmConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询结果集
     *
     * @param sql
     * @param clz
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        List<T> resList = new ArrayList<>();
        if (conn == null) {
            queryInit();
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 创建数据库操作对象
            ps = conn.prepareStatement(sql);
            // 执行sql语句
            rs = ps.executeQuery();
            // 获取元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                // 拿到一条查询结果，用一个T类型的对象进行封装
                T obj = clz.newInstance();
                // phoenix 的索引值是从1开始的
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    // 使用BeanUtils工具类给属性赋值
                    BeanUtils.setProperty(obj, columnName, value);
                }
                resList.add(obj);
            }

        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
        return resList;
    }

    public static void main(String[] args) {
        System.out.println(queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));
    }
}
