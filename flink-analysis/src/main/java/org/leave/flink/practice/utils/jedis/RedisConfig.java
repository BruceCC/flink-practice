package org.leave.flink.practice.utils.jedis;

import java.util.ResourceBundle;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
public class RedisConfig {
    private static final String DEFAULT_REDIS_PROPERTIES = "redis";
    private static ResourceBundle REDIS_CONFIG = ResourceBundle.getBundle(DEFAULT_REDIS_PROPERTIES);

    public static String getConfigProperty(String key) {
        return REDIS_CONFIG.getString(key);
    }
}
