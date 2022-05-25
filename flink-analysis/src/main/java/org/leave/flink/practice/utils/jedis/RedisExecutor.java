package org.leave.flink.practice.utils.jedis;

import redis.clients.jedis.Jedis;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
public interface RedisExecutor<T> {
    /**
     * redis具体逻辑接口
     * @param jedis
     * @return
     */
    T execute(Jedis jedis);
}
