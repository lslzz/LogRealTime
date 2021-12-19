package com.cetecloud.logalarm.utils;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * 查询维度工具类
 */
public class DimUtil {
    /**
     * 不使用缓存查询维度信息
     *
     * @param tableName        表名
     * @param colNameAndValues 过滤条件
     * @return
     */
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... colNameAndValues) {
        JSONObject jsonObject = null;
        StringBuilder sql = new StringBuilder("select * from " + tableName + " where ");
        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;

            if (i > 0) {
                sql.append(" and ");
            }
            sql.append(colName).append("='").append(colValue).append("'");
        }


        List<JSONObject> dimList = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
        if (dimList != null && !dimList.isEmpty()) {
            // 因为关联维度都是根据Key关联到一条数据，所以索引为零
            jsonObject = dimList.get(0);
        } else {
            System.out.println("数据未找到>>>>>>" + sql);
        }

        return jsonObject;
    }

    /**
     * 使用缓存查询维度信息
     *
     * @param tableName 表名
     * @param id        查询ID
     * @return
     */
    public static JSONObject getDimInfo(String tableName, String id) {
        Tuple2<String, String> col = Tuple2.of("id", id);
        return getDimInfo(tableName, col);
    }

    /**
     * 使用缓存查询维度信息
     *
     * @param tableName        表名
     * @param colNameAndValues 过滤条件
     * @return
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... colNameAndValues) {
        JSONObject dimInfo = null;
        Jedis jedis = null;
        String dimStr = null;

        StringBuilder sql = new StringBuilder("select * from " + tableName + " where ");
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;

            if (i > 0) {
                sql.append(" and ");
                redisKey.append("_");
            }
            sql.append(colName).append("='").append(colValue).append("'");
            redisKey.append(colValue);
        }
        jedis = RedisUtil.getJedis();
        dimStr = jedis.get(redisKey.toString());

        if (StringUtils.isNotEmpty(dimStr)) {
            dimInfo = JSON.parseObject(dimStr);
        } else {
            List<JSONObject> dimList = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
            if (dimList != null && !dimList.isEmpty()) {
                // 因为关联维度都是根据Key关联到一条数据，所以索引为零
                dimInfo = dimList.get(0);
                if (jedis != null) {
                    // 将查询出的结果缓存到redis
                    jedis.setex(redisKey.toString(), 24 * 60 * 60, dimInfo.toJSONString());
                }
            } else {
                System.out.println("数据未找到>>>>>>" + sql);
            }
        }

        if (jedis != null) {
            System.out.println("关闭redis链接");
            jedis.close();
        }
        return dimInfo;
    }

    public static void deleteCached(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;

        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("缓存异常");
        }
    }

    public static void main(String[] args) {
        //System.out.println(getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "13")).toJSONString());
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "13"));
    }
}
