package org.leave.flink.practice.order.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.leave.flink.practice.order.bean.OrderEvent;
import org.leave.flink.practice.order.bean.OrderResult;
import org.leave.flink.practice.order.function.OrderPaySelect;
import org.leave.flink.practice.order.function.OrderTimeoutSelect;

import java.time.Duration;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 * 订单超时事件处理
 */
@Slf4j
public class OrderTimeoutTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据源
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

        //定义匹配模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("begin")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equalsIgnoreCase(value.getEventType());
                    }
                })
                .followedBy("follow")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equalsIgnoreCase(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 模式运用到stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream, orderPayPattern);

        // 调用select方法，提取事件序列，超时事件要做报警提示
        OutputTag<OrderResult> orderTimeoutOutputTag = new OutputTag<>("orderTimeout");

        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print("payed");
        resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout");

        env.execute("order timeout");

    }
}
