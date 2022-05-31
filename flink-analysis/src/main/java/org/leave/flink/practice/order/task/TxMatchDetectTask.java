package org.leave.flink.practice.order.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.leave.flink.practice.order.bean.OrderEvent;
import org.leave.flink.practice.order.bean.ReceiptEvent;
import org.leave.flink.practice.order.function.TxPayMatch;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
@Slf4j
public class TxMatchDetectTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
        DataStream<String> sourceStream = env.readTextFile("data/OrderLog.csv");
        //订单输入事件流
        KeyedStream<OrderEvent, String> orderEventStream = sourceStream.map((MapFunction<String, OrderEvent>) value -> {
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
                //只关心支付了的订单
                .filter((FilterFunction<OrderEvent>) value -> StringUtils.isNotEmpty(value.getTxId()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofMillis(500)).withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy((KeySelector<OrderEvent, String>) value -> value.getTxId());

        // 支付到账事件流
        DataStream<String> receiptEventSourceStream = env.readTextFile("data/ReceiptLog.csv");
        KeyedStream<ReceiptEvent, String> receiptEventStream = sourceStream.map((MapFunction<String, ReceiptEvent>) value -> {
            try {
                String[] splits = value.split(",");
                return new ReceiptEvent(splits[0].trim(), splits[1].trim(), Long.parseLong(splits[2]) * 1000);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Dirty data: " + value);
                return null;
            }
        })
                .filter((FilterFunction<ReceiptEvent>) value -> null != value)
                //只关心支付了的订单
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ReceiptEvent>forBoundedOutOfOrderness(Duration.ofMillis(500)).withTimestampAssigner((SerializableTimestampAssigner<ReceiptEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy((KeySelector<ReceiptEvent, String>) value -> value.getTxId());

        /**
         * 订单流中有 到账流中无
         */
        OutputTag<OrderEvent> unmatchedPay = new OutputTag<>("unmatchedPay");
        /**
         * 到账流中有 订单流中无
         */
        OutputTag<ReceiptEvent> unmatchedReceipt = new OutputTag<>("unmatchedReceipt");
        // 将两条流连接起来
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> processedStream = orderEventStream.connect(receiptEventStream)
        .process(new TxPayMatch(unmatchedPay, unmatchedReceipt));

        processedStream.print("matched");
        processedStream.getSideOutput(unmatchedPay).print("unmatchedPay");
        processedStream.getSideOutput(unmatchedReceipt).print("unmatchedReceipt");

        env.execute("TxMatchDetectTask");
    }
}
