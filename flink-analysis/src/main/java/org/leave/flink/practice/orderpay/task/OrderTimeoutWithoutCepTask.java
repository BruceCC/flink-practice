package org.leave.flink.practice.orderpay.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.leave.flink.practice.orderpay.bean.OrderEvent;
import org.leave.flink.practice.orderpay.bean.OrderResult;
import org.leave.flink.practice.orderpay.function.OrderPayMatch;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
@Slf4j
public class OrderTimeoutWithoutCepTask {
    public static void main(String[] args) throws Exception {
        /**
         * 创建上下文环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        /**
         * 获取数据源
         */
        DataStream<String> sourceStream = env.readTextFile("data/OrderLog.csv");
        KeyedStream<OrderEvent, Long> orderEventStream = sourceStream.map((MapFunction<String, OrderEvent>) value -> {
            try {
                String[] splits = value.split(",");
                return new OrderEvent(Long.parseLong(splits[0].trim()), splits[1].trim().toLowerCase(), splits[2].trim(), Long.parseLong(splits[3].trim()));
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Dirty data: " + value);
                return null;
            }
        })
                .filter((FilterFunction<OrderEvent>) value -> null != value)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofMillis(500)).withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy((KeySelector<OrderEvent, Long>) value -> value.getOrderId());

        // 定义process function进行超时检测
        OutputTag<OrderResult> orderTimeoutOutputTag = new OutputTag<>("orderTimeout");
        SingleOutputStreamOperator<OrderResult> orderResultStream = orderEventStream.process(new OrderPayMatch(orderTimeoutOutputTag));

        orderResultStream.print("paid");
        orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout");

        env.execute("order timeout without cep");

    }
}
