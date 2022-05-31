package org.leave.flink.practice.page.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.leave.flink.practice.goods.bean.UserBehavior;
import org.leave.flink.practice.goods.constant.UserBehaviorConstant;
import org.leave.flink.practice.page.bean.PageViewCount;
import org.leave.flink.practice.page.function.UvCountByWindow;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 */
@Slf4j
public class UniqueVisitorTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
        DataStream<String> sourceStream = env.readTextFile("data/sample.csv");

        SingleOutputStreamOperator<PageViewCount> dataStream = sourceStream.map((MapFunction<String, UserBehavior>) value -> {
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
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new UvCountByWindow());

        dataStream.print("uv count");
        env.execute("unique visitor count");
    }
}
