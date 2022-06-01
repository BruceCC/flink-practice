package org.leave.flink.practice.market.task;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.leave.flink.practice.market.bean.MarketUserBehavior;
import org.leave.flink.practice.market.function.MarketCountTotal;
import org.leave.flink.practice.market.function.ChannelCountAgg;
import org.leave.flink.practice.market.source.SimulatedEventSource;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/19
 * APP推广渠道实时统计
 */
public class MarketTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
        env.addSource(new SimulatedEventSource())
                //指定事件时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MarketUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<MarketUserBehavior>) (element, recordTimestamp) -> element.getTimestamp()))
                .filter((FilterFunction<MarketUserBehavior>) value -> !"UNINSTALL".equalsIgnoreCase(value.getBehavior()))
                //按照渠道和行为分组
                .keyBy((KeySelector<MarketUserBehavior, Tuple2<String, String>>) value -> Tuple2.of(value.getChannel(), value.getBehavior()))
                //按照渠道统计应用市场使用情况
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(10)))
                .aggregate(new ChannelCountAgg(), new MarketCountTotal())
                .print();

        env.execute("App market count");

    }
}
