package com.cetecloud.logalarm.utils;


import com.cetecloud.logalarm.common.LogAlarmConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 通过连接池获取 Jedis 工具类
 */
public class RedisUtil {
    private static JedisPool jedisPool = null;

    private static void initJedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(1000); // 最大可用连接数
        jedisPoolConfig.setMaxIdle(5); // 最大闲置连接数
        jedisPoolConfig.setMinIdle(5); // 最小闲置连接数
        jedisPoolConfig.setMaxWaitMillis(2000); // 设置等待事间
        jedisPoolConfig.setTestOnBorrow(true); // 连接时进行一次ping测试
        jedisPoolConfig.setTestOnCreate(true);
        jedisPoolConfig.setBlockWhenExhausted(true); // 连接耗尽是否等待

        jedisPool =
                new JedisPool(jedisPoolConfig, LogAlarmConfig.HOSTNAME, LogAlarmConfig.REDIS_PORT, 1000);

    }
    public static Jedis getJedis() {
        if (jedisPool == null) {
            initJedisPool();
            System.out.println("开始获取连接池");
            return jedisPool.getResource();
        } else {
            System.out.println("连接池中" + jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }
}
