package org.leave.flink.practice.analysisi.hotitems.task;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.leave.flink.practice.analysisi.hotitems.bean.AppMarketUserBehavior;
import org.leave.flink.practice.analysisi.hotitems.function.AppMarketCountTotal;
import org.leave.flink.practice.analysisi.hotitems.function.ChannelCountAgg;
import org.leave.flink.practice.analysisi.hotitems.source.SimulatedEventSource;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/19
 * APP推广渠道实时统计
 */
public class AppMarketTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
        env.addSource(new SimulatedEventSource())
                //指定事件时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AppMarketUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((SerializableTimestampAssigner<AppMarketUserBehavior>) (element, recordTimestamp) -> element.getTimestamp()))
                .filter((FilterFunction<AppMarketUserBehavior>) value -> "UNINSTALL".equalsIgnoreCase(value.getBehavior()))
                //提取渠道使用记录
                .map((MapFunction<AppMarketUserBehavior, Tuple2<String, Long>>) value -> Tuple2.of(value.getChannel(), 1L))
                //按照渠道分组
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                //按照渠道统计应用市场使用情况
                .window(TumblingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .aggregate(new ChannelCountAgg(), new AppMarketCountTotal())
                .print();

        env.execute("app marketing job");

    }
}
