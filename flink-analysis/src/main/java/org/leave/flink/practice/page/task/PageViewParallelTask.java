package org.leave.flink.practice.page.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.leave.flink.practice.goods.bean.UserBehavior;
import org.leave.flink.practice.goods.constant.UserBehaviorConstant;
import org.leave.flink.practice.page.bean.PageViewCount;
import org.leave.flink.practice.page.function.PvCountAgg;
import org.leave.flink.practice.page.function.PvCountResult;
import org.leave.flink.practice.page.function.TotalPvCount;

import java.security.SecureRandom;
import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 * 并行任务改进，设计随机key，解决数据倾斜问题
 */
@Slf4j
public class PageViewParallelTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        //获取数据源
        DataStream<String> sourceStream = env.readTextFile("data/sample.csv");

        DataStream<PageViewCount> dataStream = sourceStream.map((MapFunction<String, UserBehavior>) value -> {
            try {
                String[] splits = value.split(",");
                return new UserBehavior(Long.parseLong(splits[0].trim()), Long.parseLong(splits[1].trim()), Integer.parseInt(splits[2].trim()), splits[3].trim().toLowerCase(), Long.parseLong(splits[4].trim()) * 1000L);

            } catch (Exception e) {
                log.error("Dirty data: " + value);
                return null;
            }
        })
                .filter((FilterFunction<UserBehavior>) value -> null != value)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (element, recordTimestamp) -> element.getTimestamp()))
                .filter((FilterFunction<UserBehavior>) value -> UserBehaviorConstant.PV.equals(value.getBehavior()))
                //并行任务改进，设计随机key，解决数据倾斜问题
                .map((MapFunction<UserBehavior, Tuple2<Integer, Long>>) value -> {
                    SecureRandom random = new SecureRandom();
                    return Tuple2.of(random.nextInt(10), 1L);
                })
                .keyBy((KeySelector<Tuple2<Integer, Long>, Integer>) value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PvCountAgg(), new PvCountResult())
                .keyBy((KeySelector<PageViewCount, Long>) value -> value.getWindowEnd())
                .process(new TotalPvCount());

        dataStream.print("pv count");

        env.execute("Page view count");

    }
}
