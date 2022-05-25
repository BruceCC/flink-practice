package org.leave.flink.practice.utils.jedis;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
public class RedisUtilTest {
    public static void main(String[] args) {
        RedisUtil redisUtil = RedisUtil.getInstance();
        System.out.println(redisUtil.set("keyTest","valueTest"));
        System.out.println(redisUtil.get("keyTest"));
        redisUtil.destroy();
    }
}
