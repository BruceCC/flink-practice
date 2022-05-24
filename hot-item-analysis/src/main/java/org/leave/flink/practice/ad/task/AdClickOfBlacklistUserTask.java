package org.leave.flink.practice.ad.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.leave.flink.practice.ad.bean.AdClickEvent;
import org.leave.flink.practice.ad.bean.AdCountByProvince;
import org.leave.flink.practice.ad.bean.BlackListWarning;
import org.leave.flink.practice.ad.function.AdCountAgg;
import org.leave.flink.practice.ad.function.AdCountResult;
import org.leave.flink.practice.ad.function.FilterBlackListUserProcess;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 */
@Slf4j
public class AdClickOfBlacklistUserTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
        DataStream<String> sourceStream = env.readTextFile("data/AdClickLog.csv");

        OutputTag<BlackListWarning> blackListOutputTag = new OutputTag<>("blackListOutputTag");

        //数据解析
        SingleOutputStreamOperator<AdClickEvent> adEventStream = sourceStream.map((MapFunction<String, AdClickEvent>) value -> {
            try {
                String[] splits = value.split(",");
                return new AdClickEvent(Long.parseLong(splits[0].trim()), Long.parseLong(splits[1].trim()), splits[2].trim(), splits[3].trim(), Long.parseLong(splits[4].trim()));
            } catch (Exception e) {
                log.error("Dirty data: " + value);
                return null;
            }
        })
                .filter((FilterFunction<AdClickEvent>) value -> null != value)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((SerializableTimestampAssigner<AdClickEvent>) (element, recordTimestamp) -> element.getTimestamp()));

        //自定义process function 过滤掉刷点击量的行为
        SingleOutputStreamOperator<AdClickEvent> filterBlackListStream = adEventStream.keyBy((KeySelector<AdClickEvent, Tuple2<Long, Long>>) value -> Tuple2.of(value.getUserId(), value.getAdId()))
                .process(new FilterBlackListUserProcess(15L, blackListOutputTag));

        //按照省份开窗聚合
        SingleOutputStreamOperator<AdCountByProvince> adCountStream = filterBlackListStream.keyBy((KeySelector<AdClickEvent, String>) value -> value.getProvince())
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountStream.print("Ad click count");
        filterBlackListStream.getSideOutput(blackListOutputTag).print("Black list");

        env.execute("Ad statistics");
    }

}
