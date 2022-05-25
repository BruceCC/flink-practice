package org.leave.flink.practice.utils.jedis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
@Slf4j
public class RedisUtil {

    private static final String DEFAULT_REDIS_SEPARATOR = ";";

    private static final String HOST_PORT_SEPARATOR = ":";

    private JedisPool[] jedisPools = new JedisPool[0];

    private static final RedisUtil INSTANCE = new RedisUtil();

    private RedisUtil() {
        initPool();
    }

    private void initPool() {

        // 操作超时时间,默认2秒
        int timeout = NumberUtils.toInt(RedisConfig.getConfigProperty("redis.timeout"), 2000);
        // jedis池最大连接数总数，默认8
        int maxTotal = NumberUtils.toInt(RedisConfig.getConfigProperty("redis.jedisPoolConfig.maxTotal"), 8);
        // jedis池最大空闲连接数，默认8
        int maxIdle = NumberUtils.toInt(RedisConfig.getConfigProperty("redis.jedisPoolConfig.maxIdle"), 8);
        // jedis池最少空闲连接数
        int minIdle = NumberUtils.toInt(RedisConfig.getConfigProperty("redis.jedisPoolConfig.minIdle"), 0);
        // jedis池没有对象返回时，最大等待时间单位为毫秒
        long maxWaitMillis = NumberUtils.toLong(RedisConfig.getConfigProperty("redis.jedisPoolConfig.maxWaitTime"), -1);
        // 在borrow一个jedis实例时，是否提前进行validate操作
        boolean testOnBorrow = Boolean.parseBoolean(RedisConfig.getConfigProperty("redis.jedisPoolConfig.testOnBorrow"));

        // 设置jedis连接池配置
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setMaxWait(Duration.ofMillis(maxWaitMillis));
        poolConfig.setTestOnBorrow(testOnBorrow);

        // 取得redis的url
        String redisUrls = RedisConfig.getConfigProperty("redis.jedisPoolConfig.urls");
        if (StringUtils.isEmpty(redisUrls)) {
            throw new IllegalStateException("the urls of redis is not configured");
        }
        log.info("the urls of redis is {}", redisUrls);

        // 生成连接池
        List<JedisPool> jedisPoolList = new ArrayList<JedisPool>();
        for (String redisUrl : redisUrls.split(DEFAULT_REDIS_SEPARATOR)) {
            String[] redisUrlInfo = redisUrl.split(HOST_PORT_SEPARATOR);
            jedisPoolList.add(new JedisPool(poolConfig, redisUrlInfo[0], Integer.parseInt(redisUrlInfo[1]), timeout));
        }

        jedisPools = jedisPoolList.toArray(jedisPools);
    }

    public static RedisUtil getInstance() {
        return INSTANCE;
    }

    /**
     * 实现jedis连接的获取和释放，具体的redis业务逻辑由executor实现
     *
     * @param executor RedisExecutor接口的实现类
     * @return
     */
    public <T> T execute(String key, RedisExecutor<T> executor) {
        Jedis jedis = jedisPools[(0x7FFFFFFF & key.hashCode()) % jedisPools.length].getResource();
        T result;
        try {
            result = executor.execute(jedis);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    public String set(final String key, final String value) {
        return execute(key, jedis -> jedis.set(key, value));
    }

    public String set(final String key, final String value, final SetParams params) {
        return execute(key, jedis -> jedis.set(key, value, params));
    }

    public String get(final String key) {
        return execute(key, jedis -> jedis.get(key));
    }

    public Boolean exists(final String key) {
        return execute(key, jedis -> jedis.exists(key));
    }

    public Long setnx(final String key, final String value) {
        return execute(key, jedis -> jedis.setnx(key, value));
    }

    public String setex(final String key, final int seconds, final String value) {
        return execute(key, jedis -> jedis.setex(key, seconds, value));
    }

    public Long expire(final String key, final int seconds) {
        return execute(key, jedis -> jedis.expire(key, seconds));
    }

    public Long incr(final String key) {
        return execute(key, jedis -> jedis.incr(key));
    }

    public Long decr(final String key) {
        return execute(key, jedis -> jedis.decr(key));
    }

    public Long hset(final String key, final String field, final String value) {
        return execute(key, jedis -> jedis.hset(key, field, value));
    }

    public String hget(final String key, final String field) {
        return execute(key, jedis -> jedis.hget(key, field));
    }

    public String hmset(final String key, final Map<String, String> hash) {
        return execute(key, jedis -> jedis.hmset(key, hash));
    }

    public List<String> hmget(final String key, final String... fields) {
        return execute(key, jedis -> jedis.hmget(key, fields));
    }

    public Long del(final String key) {
        return execute(key, jedis -> jedis.del(key));
    }

    public Map<String, String> hgetAll(final String key) {
        return execute(key, jedis -> jedis.hgetAll(key));
    }

    public void destroy() {
        for (int i = 0; i < jedisPools.length; i++) {
            jedisPools[i].close();
        }
    }
}
