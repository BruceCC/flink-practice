package org.leave.flink.practice.market.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.leave.flink.practice.market.bean.AppMarketUserBehavior;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 * 实现自定义的模拟市场用户行为数据源
 */
public class SimulatedEventSource extends RichSourceFunction<AppMarketUserBehavior> {
    /**
     * 标识数据源是否正常运行
     */
    private boolean isRunning = true;

    /**
     * 定义用户行为的集合
     */
    private static final String[] BEHAVIOR_TYPES = {"CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL"};

    /**
     * 定义渠道的集合
     */
    private static final String[] CHANNEL_SETS = {"wechat", "weibo", "appstore", "huaweistore"};

    @Override
    public void run(SourceContext<AppMarketUserBehavior> ctx) throws Exception {
        Long maxElementNumber = Long.MAX_VALUE;
        Long count = 0L;
        SecureRandom random = new SecureRandom();
        while (isRunning && count < maxElementNumber) {
            String userId = UUID.randomUUID().toString();
            String behavior = BEHAVIOR_TYPES[random.nextInt(BEHAVIOR_TYPES.length)];
            String channel = CHANNEL_SETS[random.nextInt(CHANNEL_SETS.length)];
            Long ts = System.currentTimeMillis();
            ctx.collect(new AppMarketUserBehavior(userId, behavior, channel, ts));
            count += 1;
            TimeUnit.MICROSECONDS.sleep(10L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
