package org.leave.flink.practice.market.task;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.leave.flink.practice.market.bean.AppMarketUserBehavior;
import org.leave.flink.practice.market.function.AppMarketCountByChannel;
import org.leave.flink.practice.market.source.SimulatedEventSource;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 * APP推广总数实时统计
 */
public class AppMarkdetChannelTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
        env.addSource(new SimulatedEventSource())
                //指定事件时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AppMarketUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((SerializableTimestampAssigner<AppMarketUserBehavior>) (element, recordTimestamp) -> element.getTimestamp()))
                .filter((FilterFunction<AppMarketUserBehavior>) value -> !"UNINSTALL".equalsIgnoreCase(value.getBehavior()))
                //按照渠道和行为分组
                /*.keyBy((KeySelector<AppMarketUserBehavior, Tuple2<String, String>>) value -> Tuple2.of(value.getChannel(), value.getBehavior()))*/
                .map((MapFunction<AppMarketUserBehavior, Tuple2<Tuple2<String, String>, Long>>) value -> Tuple2.of(Tuple2.of(value.getChannel(), value.getBehavior()), 1L))
                .keyBy((KeySelector<Tuple2<Tuple2<String, String>, Long>, Tuple2<String, String>>) value -> value.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(10)))
                .process(new AppMarketCountByChannel())
                .print();

        env.execute("App market countby channel");

    }
}
